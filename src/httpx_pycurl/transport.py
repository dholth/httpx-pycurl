from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from tempfile import SpooledTemporaryFile
from threading import Event
from typing import TYPE_CHECKING

import anyio
import certifi
import pycurl
import pycurl as _pycurl

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from typing import BinaryIO, Callable


@dataclass
class _CurlResponse:
    status_code: int
    reason_phrase: str
    headers: list[tuple[bytes, bytes]]
    body_file: BinaryIO
    http_version: bytes


@dataclass
class _TransferContext:
    response_body: BinaryIO
    status_line: bytes | None = None
    response_headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    debug_callback: Callable[[int, bytes], None] | None = None


@dataclass
class _PendingTask:
    request: httpx.Request
    done: Event
    response: httpx.Response | None = None
    error: Exception | None = None


class _RequestBodyReader:
    def __init__(self, stream: Iterable[bytes]):
        self._iterator = iter(stream)
        self._buffer = b""

    def read(self, size: int) -> bytes:
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


class _SyncFileStream(httpx.SyncByteStream):
    def __init__(self, body_file: BinaryIO, chunk_size: int = 65536):
        self._body_file = body_file
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator[bytes]:
        while True:
            chunk = self._body_file.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        self._body_file.close()


class _AsyncFileStream(httpx.AsyncByteStream):
    def __init__(self, body_file: BinaryIO, chunk_size: int = 65536):
        self._body_file = body_file
        self._chunk_size = chunk_size

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = self._body_file.read(self._chunk_size)
            # XXX needs revision to be properly async+streaming, but
            # async+"doesn't return response until all buffered" is also a
            # useful mode.
            if not chunk:
                break
            yield chunk

    async def aclose(self):
        await anyio.to_thread.run_sync(self._body_file.close)


def _parse_status_line(status_line: bytes | None) -> tuple[str, bytes]:
    if not status_line:
        return "", b""

    parts = status_line.split(b" ", maxsplit=2)
    version = parts[0].split(b"/", maxsplit=1)[-1] if parts else b""
    # Prepend "HTTP/" to match httpx's format
    if version and not version.startswith(b"HTTP"):
        version = b"HTTP/" + version
    reason = parts[2].decode("latin-1", errors="replace") if len(parts) == 3 else ""
    return reason, version


def _map_pycurl_error(code: int, message: str) -> Exception:
    """Map pycurl error codes to appropriate httpx exceptions."""
    # pycurl error code 1 is UNSUPPORTED_PROTOCOL
    if code == _pycurl.E_UNSUPPORTED_PROTOCOL:
        return httpx.UnsupportedProtocol(f"Protocol not supported: {message}")

    # URL format errors
    if code == _pycurl.E_URL_MALFORMAT or code == _pycurl.E_BAD_FUNCTION_ARGUMENT:
        return httpx.LocalProtocolError(f"Invalid URL: {message}")

    # Connection/timeout errors
    if code in (
        _pycurl.E_COULDNT_RESOLVE_HOST,
        _pycurl.E_COULDNT_CONNECT,
        _pycurl.E_OPERATION_TIMEDOUT,
    ):
        if code == _pycurl.E_OPERATION_TIMEDOUT:
            return httpx.ConnectTimeout(f"Connection timeout: {message}")
        return httpx.ConnectError(f"Connection error: {message}")

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
):
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
        context.response_body.write(chunk)
        return len(chunk)

    curl.setopt(_pycurl.URL, str(request.url))
    curl.setopt(_pycurl.WRITEFUNCTION, write_callback)
    curl.setopt(_pycurl.HEADERFUNCTION, header_callback)
    curl.setopt(_pycurl.NOSIGNAL, 1)
    curl.setopt(_pycurl.FOLLOWLOCATION, 1 if follow_redirects else 0)
    curl.setopt(_pycurl.SSL_VERIFYPEER, 1 if verify else 0)
    curl.setopt(_pycurl.SSL_VERIFYHOST, 2 if verify else 0)
    # cost incurred when TLS connection is made
    curl.setopt(_pycurl.CAINFO, cainfo)
    if timeout is not None:
        curl.setopt(_pycurl.TIMEOUT_MS, int(timeout * 1000))
    if user_agent:
        curl.setopt(_pycurl.USERAGENT, user_agent)

    debug_enabled = verbose or debug_callback is not None or debug_logger is not None
    if debug_enabled:
        curl.setopt(_pycurl.VERBOSE, 1)
        if debug_callback is not None:
            context.debug_callback = debug_callback
            curl.setopt(_pycurl.DEBUGFUNCTION, context.debug_callback)
        elif debug_logger is not None:

            def _log_debug(info_type: int, data: bytes):
                debug_logger.debug("curl[%s] %r", info_type, data)

            context.debug_callback = _log_debug
            curl.setopt(_pycurl.DEBUGFUNCTION, context.debug_callback)

    headers: list[str] = []
    for key, value in request.headers.multi_items():
        headers.append(f"{key}: {value}")
    if headers:
        curl.setopt(_pycurl.HTTPHEADER, headers)

    method = request.method.upper()
    if method == "GET":
        curl.setopt(_pycurl.HTTPGET, 1)
        return
    if method == "HEAD":
        curl.setopt(_pycurl.NOBODY, 1)
        curl.setopt(_pycurl.CUSTOMREQUEST, "HEAD")
        return

    content_length = request.headers.get("content-length")
    has_declared_body = (
        content_length is not None or "transfer-encoding" in request.headers
    )
    body_expected = method in {"POST", "PUT", "PATCH"} or has_declared_body
    curl.setopt(_pycurl.CUSTOMREQUEST, method)
    if not body_expected:
        return

    body_reader = _RequestBodyReader(request.stream)
    curl.setopt(_pycurl.READFUNCTION, body_reader.read)
    size = int(content_length) if content_length is not None else -1

    if method == "POST":
        curl.setopt(_pycurl.POST, 1)
        if size >= 0:
            curl.setopt(_pycurl.POSTFIELDSIZE_LARGE, size)
    else:
        curl.setopt(_pycurl.UPLOAD, 1)
        if size >= 0:
            curl.setopt(_pycurl.INFILESIZE_LARGE, size)


def _finalize_curl_response(
    curl: pycurl.Curl, context: _TransferContext
) -> _CurlResponse:
    status_code = int(curl.getinfo(_pycurl.RESPONSE_CODE))
    reason_phrase, http_version = _parse_status_line(context.status_line)
    context.response_body.seek(0)
    return _CurlResponse(
        status_code=status_code,
        reason_phrase=reason_phrase,
        headers=context.response_headers,
        body_file=context.response_body,
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
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._verbose = verbose
        self._debug_callback = debug_callback
        self._debug_logger = debug_logger

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        curl_response = self._perform_request(request)
        return httpx.Response(
            status_code=curl_response.status_code,
            headers=curl_response.headers,
            stream=_SyncFileStream(curl_response.body_file),
            request=request,
            extensions={
                "http_version": curl_response.http_version
            },  # b'2' here but b'HTTP/2' in regular httpx
        )

    def close(self):
        return

    def _perform_request(self, request: httpx.Request) -> _CurlResponse:
        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )

        curl = _pycurl.Curl()
        try:
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
            )

            curl.perform()
            return _finalize_curl_response(curl, context)
        except _pycurl.error as error:
            context.response_body.close()
            code, message = error.args
            raise _map_pycurl_error(code, str(message)) from error
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
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._verbose = verbose
        self._debug_callback = debug_callback
        self._debug_logger = debug_logger
        self._max_connections = max_connections

        self._multi: pycurl.CurlMulti | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._timer_handle: asyncio.TimerHandle | None = None
        self._socket_watch: dict[int, int] = {}
        self._transfers: dict[
            pycurl.Curl,
            tuple[httpx.Request, _TransferContext, asyncio.Future[_CurlResponse]],
        ] = {}

        self._cainfo = cainfo or certifi.where()

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
                "PyCurlAsyncMultiSocketTransport must be used from one event loop"
            )

        multi = self._ensure_multi()

        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )
        curl = _pycurl.Curl()
        future: asyncio.Future[_CurlResponse] = loop.create_future()

        try:
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
            )
            self._transfers[curl] = (request, context, future)
            multi.add_handle(curl)
            self._drive_socket(_pycurl.SOCKET_TIMEOUT, 0)

            curl_response = await future
            return httpx.Response(
                status_code=curl_response.status_code,
                headers=curl_response.headers,
                stream=_AsyncFileStream(curl_response.body_file),
                request=request,
                extensions={"http_version": curl_response.http_version},
            )
        except _pycurl.error as error:
            self._remove_transfer(curl)
            context.response_body.close()
            code, message = error.args
            raise _map_pycurl_error(code, str(message)) from error
        except Exception:
            self._remove_transfer(curl)
            context.response_body.close()
            raise

    async def aclose(self):
        if self._closed:
            return
        self._closed = True

        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

        if self._loop is not None:
            for fd in list(self._socket_watch):
                self._loop.remove_reader(fd)
                self._loop.remove_writer(fd)
        self._socket_watch.clear()

        multi = self._multi
        if multi is None:
            return

        for curl, (_request, context, future) in list(self._transfers.items()):
            if not future.done():
                future.set_exception(
                    httpx.TransportError("transport closed before request completed")
                )
            context.response_body.close()
            try:
                multi.remove_handle(curl)
            finally:
                curl.close()
        self._transfers.clear()
        multi.close()
        self._multi = None

    def _ensure_multi(self) -> pycurl.CurlMulti:
        if self._multi is not None:
            return self._multi

        multi = _pycurl.CurlMulti()
        if hasattr(_pycurl, "M_MAX_TOTAL_CONNECTIONS"):
            multi.setopt(_pycurl.M_MAX_TOTAL_CONNECTIONS, self._max_connections)
        multi.setopt(_pycurl.M_SOCKETFUNCTION, self._socket_callback)
        multi.setopt(_pycurl.M_TIMERFUNCTION, self._timer_callback)
        self._multi = multi
        return multi

    def _socket_callback(self, *args) -> int:

        ints = [arg for arg in args if isinstance(arg, int)]
        if len(ints) < 2:
            return 0

        first, second = ints[0], ints[1]
        if first in {
            _pycurl.POLL_IN,
            _pycurl.POLL_OUT,
            _pycurl.POLL_INOUT,
            _pycurl.POLL_REMOVE,
        }:
            what = first
            fd = second
        else:
            fd = first
            what = second

        self._set_socket_watch(fd, what)
        return 0

    def _timer_callback(self, *args) -> int:
        if self._loop is None:
            return 0

        timeout_ms = next((arg for arg in args if isinstance(arg, int)), -1)

        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

        if timeout_ms < 0:
            return 0
        if timeout_ms == 0:
            self._loop.call_soon(self._on_timeout)
            return 0

        self._timer_handle = self._loop.call_later(
            timeout_ms / 1000.0, self._on_timeout
        )
        return 0

    def _set_socket_watch(self, fd: int, what: int):
        if self._loop is None:
            return

        self._loop.remove_reader(fd)
        self._loop.remove_writer(fd)

        if what == _pycurl.POLL_REMOVE:
            self._socket_watch.pop(fd, None)
            return

        self._socket_watch[fd] = what
        if what in {_pycurl.POLL_IN, _pycurl.POLL_INOUT}:
            try:
                self._loop.add_reader(fd, self._on_socket_readable, fd)
            except OSError:
                self._socket_watch.pop(fd, None)
                return
        if what in {_pycurl.POLL_OUT, _pycurl.POLL_INOUT}:
            try:
                self._loop.add_writer(fd, self._on_socket_writable, fd)
            except OSError:
                self._loop.remove_reader(fd)
                self._socket_watch.pop(fd, None)
                return

    def _on_socket_readable(self, fd: int):
        self._drive_socket(fd, _pycurl.CSELECT_IN)

    def _on_socket_writable(self, fd: int):
        self._drive_socket(fd, _pycurl.CSELECT_OUT)

    def _on_timeout(self):

        self._timer_handle = None
        self._drive_socket(_pycurl.SOCKET_TIMEOUT, 0)

    def _drive_socket(self, sock_fd: int, event_mask: int):
        multi = self._ensure_multi()

        while True:
            status, _running = multi.socket_action(sock_fd, event_mask)
            if status != getattr(_pycurl, "E_CALL_MULTI_PERFORM", -1):
                break

        self._drain_info_read()

    def _drain_info_read(self):
        multi = self._ensure_multi()

        while True:
            queued, successful, failed = multi.info_read()

            for curl in successful:
                self._complete_success(curl)
            for curl, code, message in failed:
                self._complete_error(curl, code, message)

            if queued == 0:
                break

    def _complete_success(self, curl: pycurl.Curl):
        transfer = self._transfers.pop(curl, None)
        if transfer is None:
            return
        _request, context, future = transfer

        try:
            response = _finalize_curl_response(curl, context)
            if not future.done():
                future.set_result(response)
        except Exception as error:
            context.response_body.close()
            if not future.done():
                future.set_exception(httpx.TransportError(str(error)))
        finally:
            self._remove_handle_only(curl)

    def _complete_error(self, curl: pycurl.Curl, code: int, message: str):
        transfer = self._transfers.pop(curl, None)
        if transfer is None:
            return
        _request, context, future = transfer
        context.response_body.close()
        if not future.done():
            future.set_exception(_map_pycurl_error(code, message))
        self._remove_handle_only(curl)

    def _remove_transfer(self, curl: pycurl.Curl):
        self._transfers.pop(curl, None)
        self._remove_handle_only(curl)

    def _remove_handle_only(self, curl: pycurl.Curl):
        if self._multi is not None:
            try:
                self._multi.remove_handle(curl)
            except Exception:
                pass
        curl.close()
