from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING

import anyio
import certifi
import httpx
import pycurl

from .curl import AsyncCurl

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from typing import BinaryIO, Callable


# Sentinel object for signaling end of stream
_END_OF_STREAM = object()


@dataclass
class _CurlResponse:
    status_code: int
    reason_phrase: str
    headers: list[tuple[bytes, bytes]]
    http_version: bytes
    body_file: BinaryIO | None = None
    body_stream: _AsyncQueueStream | None = None


@dataclass
class _TransferContext:
    response_body: BinaryIO
    status_line: bytes | None = None
    response_headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    async_stream: _AsyncQueueStream | None = None


@dataclass
class _Transfer:
    """Represents an in-flight HTTP request through pycurl."""

    request: httpx.Request
    context: _TransferContext
    future: asyncio.Future


class _RequestBodyReader:
    def __init__(self, stream: Iterable[bytes] | AsyncIterator[bytes]):
        # Check if stream is async - prefer __iter__ for sync streams
        # httpx.ByteStream has both __iter__ and __aiter__, so we need to check
        # for __iter__ first to handle sync streams properly
        if hasattr(stream, "__iter__"):
            # Sync stream
            self._iterator = iter(stream)
            self._is_async = False
            self._async_stream = None
            self._chunks = []
        elif hasattr(stream, "__aiter__"):
            # Async stream (only if not also iterable)
            self._iterator = None
            self._is_async = True
            self._async_stream = stream
            self._chunks = []
            self._chunks_consumed = False
        else:
            # Unknown stream type, treat as empty
            self._iterator = iter([])
            self._is_async = False
            self._async_stream = None
            self._chunks = []
        self._buffer = b""

    async def _consume_async_chunks(self):
        """Consume all chunks from async stream into memory."""
        if self._chunks_consumed:
            return
        self._chunks_consumed = True
        async for chunk in self._async_stream:
            if chunk:
                self._chunks.append(chunk)
        # Create iterator from collected chunks
        self._iterator = iter(self._chunks)

    def read(self, size: int) -> bytes:
        if self._iterator is None:
            # This shouldn't happen in normal flow for async streams
            # as _consume_async_chunks should be called first
            return b""

        while len(self._buffer) < size:
            try:
                chunk = next(self._iterator)
            except StopIteration:
                break
            if chunk:
                self._buffer += chunk

        if size <= 0:
            data = self._buffer
            self._buffer = b""
            return data

        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return data

    # TODO: Implement CURL_READFUNC_PAUSE strategy for streaming async request bodies
    # Currently, async streams are pre-consumed into memory before being passed to curl's
    # synchronous READFUNCTION callback. This works but has memory implications for large
    # request bodies. A better approach would be to:
    #
    # 1. Return CURL_READFUNC_PAUSE from read() when the buffer is empty and more data
    #    needs to be fetched from the async stream
    # 2. Store reference to event loop and curl handle in _RequestBodyReader
    # 3. Register async task to fetch next chunk from stream when PAUSE is returned
    # 4. Call curl.pause(pycurl.PAUSE_RECV | pycurl.PAUSE_SEND) to pause transfer
    # 5. Resume transfer with curl.pause(0) once next chunk is available
    #
    # This would allow truly streaming async request bodies without buffering everything
    # in memory first, while maintaining pycurl's synchronous callback interface.


class _FileStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """Unified file-based stream supporting both sync and async iteration."""

    def __init__(self, body_file: BinaryIO, chunk_size: int = 65536):
        self._body_file = body_file
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator[bytes]:
        while True:
            chunk = self._body_file.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = self._body_file.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        self._body_file.close()

    async def aclose(self):
        await anyio.to_thread.run_sync(self._body_file.close)


class _AsyncQueueStream(httpx.AsyncByteStream):
    """Async byte stream that collects write_callback values from a queue.

    Pycurl callbacks are executed synchronously within the event loop,
    so we can put chunks directly into the queue without call_soon_threadsafe.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._queue: asyncio.Queue[bytes | object] = asyncio.Queue()
        self._loop = loop

    def write_callback(self, chunk: bytes) -> int:
        """Callback to be used by pycurl to add chunks to the queue.

        Called synchronously during socket event handling in the event loop,
        so put_nowait is safe without call_soon_threadsafe.
        """
        try:
            self._queue.put_nowait(chunk)
        except asyncio.QueueFull:
            # Queue is full, this shouldn't happen with asyncio.Queue
            pass
        return len(chunk)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            try:
                chunk = await asyncio.wait_for(self._queue.get(), timeout=30.0)
                if chunk is _END_OF_STREAM:
                    break
                if isinstance(chunk, bytes):
                    yield chunk
            except asyncio.TimeoutError:
                # Timeout waiting for next chunk
                break

    async def aclose(self):
        """Close the stream and signal end of stream."""
        try:
            self._queue.put_nowait(_END_OF_STREAM)
        except (RuntimeError, asyncio.QueueFull):
            pass

    def signal_end_of_stream(self):
        """Signal end of stream - called from event loop context."""
        try:
            self._queue.put_nowait(_END_OF_STREAM)
        except asyncio.QueueFull:
            pass


def _parse_status_line(status_line: bytes | None) -> tuple[str, bytes]:
    if not status_line:
        return "", b""

    parts = status_line.split(b" ", maxsplit=2)
    version = parts[0] if parts else b""
    reason = parts[2].decode("latin-1", errors="replace") if len(parts) == 3 else ""
    return reason, version


def _map_pycurl_error(
    code: int, message: str, curl: pycurl.Curl | None = None
) -> httpx.TransportError:
    """Map pycurl error codes to appropriate httpx exceptions."""
    # pycurl error code 1 is UNSUPPORTED_PROTOCOL
    if code == pycurl.E_UNSUPPORTED_PROTOCOL:
        return httpx.UnsupportedProtocol(f"Protocol not supported: {message}")

    # URL format errors
    if code == pycurl.E_URL_MALFORMAT or code == pycurl.E_BAD_FUNCTION_ARGUMENT:
        # Check if it's a port-related error
        if "port" in message.lower():
            return httpx.ConnectError(f"Connection error: {message}")
        return httpx.LocalProtocolError(f"Invalid URL: {message}")

    # Connection/timeout errors
    if code in (
        pycurl.E_COULDNT_RESOLVE_HOST,
        pycurl.E_COULDNT_CONNECT,
    ):
        return httpx.ConnectError(f"Connection error: {message}")

    if code == pycurl.E_OPERATION_TIMEDOUT:
        # Distinguish connect timeout from read timeout using connect time info.
        # If connect_time is 0, the connection was never established -> ConnectTimeout.
        if curl is not None:
            try:
                connect_time = curl.getinfo(pycurl.CONNECT_TIME)
                if connect_time > 0:
                    return httpx.ReadTimeout(f"Read timeout: {message}")
            except Exception:
                pass
        return httpx.ConnectTimeout(f"Connection timeout: {message}")

    # Remote protocol errors
    if code == pycurl.E_PARTIAL_FILE:
        return httpx.RemoteProtocolError(f"{message}")

    # Default to TransportError
    return httpx.TransportError(f"pycurl error {code}: {message}")


def _configure_curl(
    curl: pycurl.Curl,
    request: httpx.Request,
    context: _TransferContext,
    *,
    timeout: float | None,
    verify: bool,
    follow_redirects: bool,
    user_agent: str | None,
    verbose: bool,
    debug_callback: Callable[[int, bytes], None] | None,
    debug_logger: logging.Logger | None,
    cainfo: str | None,
    async_stream: _AsyncQueueStream | None = None,
    body_reader: _RequestBodyReader | None = None,
):
    # Apply per-request connect timeout from httpx extensions
    ext_timeout = request.extensions.get("timeout", {})
    connect_timeout = ext_timeout.get("connect")
    read_timeout = ext_timeout.get("read")
    _pool_timeout = ext_timeout.get("pool")  # Not available in curl

    if connect_timeout is not None:
        curl.setopt(pycurl.CONNECTTIMEOUT_MS, int(connect_timeout * 1000))
    elif timeout is not None:
        curl.setopt(pycurl.CONNECTTIMEOUT_MS, int(timeout * 1000))

    # Use transport-level timeout if specified, otherwise use read timeout from request
    # pycurl's TIMEOUT_MS sets the maximum time for the entire operation
    effective_timeout = timeout if timeout is not None else read_timeout
    if effective_timeout is not None:
        curl.setopt(pycurl.TIMEOUT_MS, int(effective_timeout * 1000))

    def header_callback(chunk: bytes) -> int:
        line = chunk.rstrip(b"\r\n")
        if not line:
            return len(chunk)
        if line.startswith(b"HTTP/"):
            context.status_line = line
            context.response_headers.clear()
            return len(chunk)
        key, sep, value = line.partition(b":")
        if sep:
            context.response_headers.append((key.strip(), value.strip()))
        return len(chunk)

    def write_callback(chunk: bytes) -> int:
        if async_stream is not None:
            # Use async stream for streaming response bodies
            return async_stream.write_callback(chunk)
        else:
            # Fall back to file-based buffering
            context.response_body.write(chunk)
            return len(chunk)

    curl.setopt(pycurl.URL, str(request.url))
    curl.setopt(pycurl.WRITEFUNCTION, write_callback)
    curl.setopt(pycurl.HEADERFUNCTION, header_callback)
    curl.setopt(pycurl.NOSIGNAL, 1)
    curl.setopt(pycurl.FOLLOWLOCATION, 1 if follow_redirects else 0)
    curl.setopt(pycurl.SSL_VERIFYPEER, 1 if verify else 0)
    curl.setopt(pycurl.SSL_VERIFYHOST, 2 if verify else 0)
    # cost incurred when TLS connection is made
    curl.setopt(pycurl.CAINFO, cainfo)
    if user_agent:
        curl.setopt(pycurl.USERAGENT, user_agent)

    debug_enabled = verbose or debug_callback is not None or debug_logger is not None
    if debug_enabled:
        curl.setopt(pycurl.VERBOSE, 1)
        if debug_callback is not None:
            context.debug_callback = debug_callback
            curl.setopt(pycurl.DEBUGFUNCTION, context.debug_callback)
        elif debug_logger is not None:

            def _log_debug(info_type: int, data: bytes):
                debug_logger.debug("curl[%s] %r", info_type, data)

            context.debug_callback = _log_debug
            curl.setopt(pycurl.DEBUGFUNCTION, context.debug_callback)

    headers: list[str] = []
    for key, value in request.headers.multi_items():
        headers.append(f"{key}: {value}")
    if headers:
        curl.setopt(pycurl.HTTPHEADER, headers)

    method = request.method.upper()
    if method == "GET":
        curl.setopt(pycurl.HTTPGET, 1)
        return
    if method == "HEAD":
        curl.setopt(pycurl.NOBODY, 1)
        curl.setopt(pycurl.CUSTOMREQUEST, "HEAD")
        return

    content_length = request.headers.get("content-length")
    has_declared_body = (
        content_length is not None or "transfer-encoding" in request.headers
    )
    body_expected = method in {"POST", "PUT", "PATCH"} or has_declared_body
    curl.setopt(pycurl.CUSTOMREQUEST, method)
    if not body_expected:
        return

    if body_reader is None:
        body_reader = _RequestBodyReader(request.stream)
    curl.setopt(pycurl.READFUNCTION, body_reader.read)
    size = int(content_length) if content_length is not None else -1

    if method == "POST":
        curl.setopt(pycurl.POST, 1)
        if size >= 0:
            curl.setopt(pycurl.POSTFIELDSIZE_LARGE, size)
    else:
        curl.setopt(pycurl.UPLOAD, 1)
        if size >= 0:
            curl.setopt(pycurl.INFILESIZE_LARGE, size)


def _finalize_curl_response(
    curl: pycurl.Curl, context: _TransferContext
) -> _CurlResponse:
    status_code = int(curl.getinfo(pycurl.RESPONSE_CODE))
    reason_phrase, http_version = _parse_status_line(context.status_line)
    body_stream = context.async_stream
    if body_stream is None:
        context.response_body.seek(0)
    return _CurlResponse(
        status_code=status_code,
        reason_phrase=reason_phrase,
        headers=context.response_headers,
        body_file=context.response_body if body_stream is None else None,
        body_stream=body_stream,
        http_version=http_version,
    )


class PyCurlTransport(httpx.BaseTransport):
    def __init__(
        self,
        timeout: float | None = None,
        verify: bool = True,
        follow_redirects: bool = False,
        user_agent: str | None = None,
        verbose: bool = False,
        debug_callback: Callable[[int, bytes], None] | None = None,
        debug_logger: logging.Logger | None = None,
        cainfo: str | None = None,
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._verbose = verbose
        self._debug_callback = debug_callback
        self._debug_logger = debug_logger
        self._cainfo = cainfo or certifi.where()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        curl_response = self._perform_request(request)
        stream = (
            _FileStream(curl_response.body_file)
            if curl_response.body_file
            else curl_response.body_stream
        )
        return httpx.Response(
            status_code=curl_response.status_code,
            headers=curl_response.headers,
            stream=stream,
            request=request,
            extensions={"http_version": curl_response.http_version},
        )

    def close(self):
        return

    def _perform_request(self, request: httpx.Request) -> _CurlResponse:
        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )

        curl = pycurl.Curl()
        try:
            # Create a body reader for the request
            body_reader = _RequestBodyReader(request.stream)

            _configure_curl(
                curl,
                request,
                context,
                timeout=self._timeout,
                verify=self._verify,
                follow_redirects=self._follow_redirects,
                user_agent=self._user_agent,
                verbose=self._verbose,
                debug_callback=self._debug_callback,
                debug_logger=self._debug_logger,
                cainfo=self._cainfo,
                body_reader=body_reader,
            )

            curl.perform()
            return _finalize_curl_response(curl, context)
        except pycurl.error as error:
            context.response_body.close()
            code, message = error.args
            raise _map_pycurl_error(code, str(message), curl) from error
        finally:
            curl.close()


class AsyncPyCurlTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        timeout: float | None = None,
        verify: bool = True,
        follow_redirects: bool = False,
        user_agent: str | None = None,
        verbose: bool = False,
        debug_callback: Callable[[int, bytes], None] | None = None,
        debug_logger: logging.Logger | None = None,
        max_connections: int = 100,
        cainfo: str | None = None,
        stream_response: bool = True,
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._verbose = verbose
        self._debug_callback = debug_callback
        self._debug_logger = debug_logger
        self._max_connections = max_connections
        self._cainfo = cainfo or certifi.where()
        self._stream_response = stream_response

        self._curl: AsyncCurl | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None

        # Track in-flight transfers
        self._transfers: dict[pycurl.Curl, _Transfer] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._closed:
            raise httpx.TransportError("transport is closed")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Not running in asyncio (e.g., trio), so we can't use this transport
            raise httpx.TransportError(
                "AsyncPyCurlTransport only supports asyncio event loops, not trio"
            )

        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise httpx.TransportError(
                "AsyncPyCurlTransport must be used from one event loop"
            )

        # Lazily initialize AsyncCurl on first request
        if self._curl is None:
            self._curl = AsyncCurl(loop, max_connections=self._max_connections)

        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )
        curl = pycurl.Curl()

        # Create async queue stream for streaming response bodies if enabled
        async_stream = _AsyncQueueStream(loop) if self._stream_response else None
        context.async_stream = async_stream

        try:
            # Pre-consume async request streams
            body_reader = _RequestBodyReader(request.stream)
            if body_reader._is_async:
                await body_reader._consume_async_chunks()

            _configure_curl(
                curl,
                request,
                context,
                timeout=self._timeout,
                verify=self._verify,
                follow_redirects=self._follow_redirects,
                user_agent=self._user_agent,
                verbose=self._verbose,
                debug_callback=self._debug_callback,
                debug_logger=self._debug_logger,
                cainfo=self._cainfo,
                async_stream=async_stream,
                body_reader=body_reader,
            )

            # Track transfer info
            future: asyncio.Future[_CurlResponse] = loop.create_future()
            self._transfers[curl] = _Transfer(request, context, future)

            try:
                # Perform the transfer via AsyncCurl
                await self._curl.perform(curl)
            except pycurl.error as error:
                code, message = error.args
                raise _map_pycurl_error(code, str(message), curl) from error
            finally:
                # Finalize response and clean up
                try:
                    curl_response = _finalize_curl_response(curl, context)
                    # Signal end of stream if using async streaming
                    if async_stream is not None:
                        async_stream.signal_end_of_stream()
                except Exception:
                    context.response_body.close()
                    raise
                finally:
                    self._transfers.pop(curl, None)
                    curl.close()

            return httpx.Response(
                status_code=curl_response.status_code,
                headers=curl_response.headers,
                stream=curl_response.body_stream
                or _FileStream(curl_response.body_file),
                request=request,
                extensions={"http_version": curl_response.http_version},
            )
        except pycurl.error as error:
            self._transfers.pop(curl, None)
            context.response_body.close()
            curl.close()
            code, message = error.args
            raise _map_pycurl_error(code, str(message), curl) from error
        except Exception:
            self._transfers.pop(curl, None)
            context.response_body.close()
            curl.close()
            raise

    async def aclose(self):
        if self._closed:
            return
        self._closed = True

        # Close AsyncCurl
        await self._curl.aclose()

        # Clean up any remaining transfers
        for curl, transfer in list(self._transfers.items()):
            transfer.context.response_body.close()
            try:
                curl.close()
            except Exception:
                pass
        self._transfers.clear()
