from __future__ import annotations

import logging
from dataclasses import dataclass, field
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING

import anyio
import certifi
import httpx
import pycurl
import pycurl as _pycurl

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from typing import BinaryIO, Callable


CURLSSLOPT_NATIVE_CA = 1 << 4


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
            chunk = await anyio.to_thread.run_sync(
                self._body_file.read, self._chunk_size
            )
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
    reason = parts[2].decode("latin-1", errors="replace") if len(parts) == 3 else ""
    return reason, version


def _configure_curl(
    curl: pycurl.Curl,
    request: httpx.Request,
    context: _TransferContext,
    *,
    timeout: float | None,
    verify: bool,
    follow_redirects: bool,
    user_agent: str | None,
    cainfo: str | None,
    verbose: bool,
    debug_callback: Callable[[int, bytes], None] | None,
    debug_logger: logging.Logger | None,
):
    def header_callback(chunk: bytes) -> int:
        log.debug("header %s", chunk)
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
        log.debug("body %s", chunk)
        context.response_body.write(chunk)
        return len(chunk)

    # default curl user agent is blocked by sites
    curl.setopt(_pycurl.USERAGENT, "pycurltx/0.0.1+curl")

    curl.setopt(_pycurl.URL, str(request.url))
    curl.setopt(_pycurl.WRITEFUNCTION, write_callback)
    curl.setopt(_pycurl.HEADERFUNCTION, header_callback)
    curl.setopt(_pycurl.NOSIGNAL, 1)
    curl.setopt(_pycurl.FOLLOWLOCATION, 1 if follow_redirects else 0)
    curl.setopt(_pycurl.SSL_VERIFYPEER, 1 if verify else 0)
    curl.setopt(_pycurl.SSL_VERIFYHOST, 2 if verify else 0)

    # cost incurred when TLS connection is made
    if cainfo is None:
        cainfo = certifi.where()
    curl.setopt(_pycurl.CAINFO, cainfo)

    # How to tell when available?
    # curl.setopt(pycurl.SSL_OPTIONS, CURLSSLOPT_NATIVE_CA)

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

    curl.userstuff = "hello"


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


class PyCurlTx(httpx.AsyncBaseTransport):
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
        self._closed = False
        self._cainfo = cainfo

        # anyio
        # _tg holds task driving CurlMulti()
        self._tg = None

        # store callbacks here, clean up this list after any curl function is
        # called because it might have triggered callbacks.
        self._callback_tasks = []
        self._sockets = {}

        # curl_multi (could we be threadlocal or loop-local)
        self.__curl_multi = pycurl.CurlMulti()
        self.__curl_multi.setopt(pycurl.M_SOCKETFUNCTION, self._socket_callback)
        self.__curl_multi.setopt(pycurl.M_TIMERFUNCTION, self._timer_callback)
        self.__curl_multi.setopt(pycurl.M_MAX_TOTAL_CONNECTIONS, self._max_connections)

    # Must enter transport for it to work
    async def __aenter__(self):
        tg = anyio.create_task_group()
        self._tg = await tg.__aenter__()
        assert tg is self._tg
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        # AsyncContextManagerMixin is designed to help with this pattern...
        return await self._tg.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def _curl_multi(self) -> pycurl.CurlMulti:
        return self.__curl_multi

    def _curl(self):
        """
        Return new or free pycurl.Curl()
        """
        return pycurl.Curl()

    def _socket_bitmask_decode(self, ev_bitmask: int):
        flags = []
        for sym in ("POLL_IN", "POLL_OUT", "POLL_INOUT", "POLL_REMOVE"):
            # inout is 3 0b11
            if (ev_bitmask ^ getattr(pycurl, sym)) == 0:
                flags.append(sym)
        return f"{bin(ev_bitmask)} {'|'.join(flags)}"

    def _socket_callback(
        self, ev_bitmask: int, sock_fd: int, multi_handle, data
    ) -> None:
        log.debug(
            "socket_callback: fd=%d ev=%s",
            sock_fd,
            self._socket_bitmask_decode(ev_bitmask),
        )

        async def socket_task():
            if ev_bitmask & pycurl.POLL_REMOVE:
                self._sockets.pop(sock_fd, None)
            else:
                # is this correct, we won't clear a POLL_IN or POLL_OUT...
                self._sockets[sock_fd] = ev_bitmask
                if ev_bitmask == pycurl.POLL_IN:
                    await anyio.wait_readable(sock_fd)
                elif ev_bitmask == pycurl.POLL_OUT:
                    await anyio.wait_writable(sock_fd)
                elif ev_bitmask == pycurl.POLL_INOUT:
                    raise NotImplementedError("INOUT")
                # could choose to _drain_messages() when this changes, else drain each time
                running = self._curl_multi.socket_action(sock_fd, ev_bitmask)
                log.debug("%s xfers", running)  # is (0, number_of_handles)
                self._post_curl()

        self._callback_tasks.append(socket_task)

    # Note transport's __aenter__ is also awaited when "with AsyncClient() as
    # client" is called (but it isn't when client is used without with
    # statement)

    def _timer_callback(self, timeout_ms: int) -> None:
        log.debug("timer_callback: timeout=%d", timeout_ms)
        self._callback_tasks.append(lambda: self._do_timer(timeout_ms))

    async def _do_timer(self, timeout_ms: int) -> None:
        # will we always be in just one thread?
        await anyio.sleep(timeout_ms / 1000.0)
        log.debug("Timed out! %d", timeout_ms)
        running = self._curl_multi.socket_action(pycurl.SOCKET_TIMEOUT, 0)
        log.debug("%s xfers", running)
        self._post_curl()

    def _post_curl(self):
        """
        Call after any curl call to schedule callback responses.
        """
        for callback in self._callback_tasks:
            self._tg.start_soon(callback)
        self._callback_tasks.clear()
        self._drain_messages(self._curl_multi)

    def _drain_messages(self, multi: pycurl.CurlMulti):
        while True:
            queued, successful, failed = multi.info_read()

            for curl in successful:
                self._complete_success(multi, curl)
            for curl, code, message in failed:
                self._complete_error(multi, curl, code, message)

            if queued == 0:
                break

    def _complete_success(self, multi: pycurl.CurlMulti, curl: pycurl.Curl):
        # may be able to return Response() while data is streaming out in async land
        curl.event.set()

    def _complete_error(
        self,
        multi: pycurl.CurlMulti,
        curl: pycurl.Curl,
        code: int,
        message: str,
    ):
        # will the curl object tell us its fail message or must we save it here?
        curl.event.set()

    def _complete_error(
        self,
        multi: pycurl.CurlMulti,
        curl: pycurl.Curl,
        code: int,
        message: str,
    ):
        # no context close...
        curl.error = httpx.TransportError(f"pycurl error {code}: {message}")
        curl.event.set()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._closed:
            raise httpx.TransportError("transport is closed")

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
                cainfo=self._cainfo,
            )
            curl.event = anyio.Event()
            curl.error = None
            self._curl_multi.add_handle(curl)
            self._post_curl()

            await curl.event.wait()
            if curl.error is not None:
                raise curl.error

            curl_response = _finalize_curl_response(curl, context)
            response = httpx.Response(
                status_code=curl_response.status_code,
                headers=curl_response.headers,
                stream=_AsyncFileStream(curl_response.body_file),
                request=request,
                extensions={"http_version": curl_response.http_version},
            )
            return response

        except _pycurl.error as error:  # XXX also in other curl API calls?
            context.response_body.close()
            code, message = error.args
            raise httpx.TransportError(f"pycurl error {code}: {message}") from error

        except Exception as error:
            context.response_body.close()
            log.debug("%s", error)
            raise

        finally:
            # self._curl_multi.remove_handle(curl)
            curl.close()
            self._post_curl()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True


# XXX might want to call pycurl.global_init() manually
