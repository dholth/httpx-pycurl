from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from tempfile import SpooledTemporaryFile
from threading import Condition, Event, Thread
from typing import TYPE_CHECKING

import anyio
import httpx
import pycurl

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from typing import BinaryIO


def _require_pycurl():
    if pycurl is None:
        raise httpx.TransportError(
            "pycurl is not installed; install pycurl to use pycurltx transports"
        )
    return pycurl


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
):
    _pycurl = _require_pycurl()

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
    if timeout is not None:
        curl.setopt(_pycurl.TIMEOUT_MS, int(timeout * 1000))
    if user_agent:
        curl.setopt(_pycurl.USERAGENT, user_agent)

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
    _pycurl = _require_pycurl()
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
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        curl_response = self._perform_request(request)
        return httpx.Response(
            status_code=curl_response.status_code,
            headers=curl_response.headers,
            stream=_SyncFileStream(curl_response.body_file),
            request=request,
            extensions={"http_version": curl_response.http_version},
        )

    def close(self):
        return

    def _perform_request(self, request: httpx.Request) -> _CurlResponse:
        _pycurl = _require_pycurl()
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
            )

            curl.perform()
            return _finalize_curl_response(curl, context)
        except _pycurl.error as error:
            context.response_body.close()
            code, message = error.args
            raise httpx.TransportError(f"pycurl error {code}: {message}") from error
        finally:
            curl.close()


class PyCurlMultiTransport(httpx.BaseTransport):
    def __init__(
        self,
        timeout: float | None = None,
        verify: bool = True,
        follow_redirects: bool = False,
        user_agent: str | None = None,
        max_connections: int = 100,
        select_timeout: float = 0.1,
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._max_connections = max_connections
        self._select_timeout = select_timeout

        self._condition = Condition()
        self._pending: deque[_PendingTask] = deque()
        self._inflight: dict[pycurl.Curl, tuple[_PendingTask, _TransferContext]] = {}
        self._thread: Thread | None = None
        self._closed = False

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        done = Event()
        task = _PendingTask(request=request, done=done)

        with self._condition:
            if self._closed:
                raise httpx.TransportError("transport is closed")
            if self._thread is None:
                self._thread = Thread(target=self._run_loop, daemon=True)
                self._thread.start()
            self._pending.append(task)
            self._condition.notify_all()

        task.done.wait()
        if task.error is not None:
            raise task.error
        if task.response is None:
            raise httpx.TransportError("pycurl multi request failed without response")
        return task.response

    def close(self):
        with self._condition:
            if self._closed:
                return
            self._closed = True
            while self._pending:
                task = self._pending.popleft()
                task.error = httpx.TransportError("transport is closed")
                task.done.set()
            self._condition.notify_all()

        if self._thread is not None:
            self._thread.join()

    def _run_loop(self):
        _pycurl = _require_pycurl()
        multi = _pycurl.CurlMulti()
        if hasattr(_pycurl, "M_MAX_TOTAL_CONNECTIONS"):
            multi.setopt(_pycurl.M_MAX_TOTAL_CONNECTIONS, self._max_connections)

        try:
            while True:
                with self._condition:
                    while not self._pending and not self._inflight and not self._closed:
                        self._condition.wait()
                    if self._closed and not self._inflight and not self._pending:
                        break

                    while self._pending:
                        task = self._pending.popleft()
                        self._enqueue_task(multi, task)

                while True:
                    status, _running = multi.perform()
                    if status != getattr(_pycurl, "E_CALL_MULTI_PERFORM", -1):
                        break

                self._drain_messages(multi)

                with self._condition:
                    has_work = bool(self._pending or self._inflight)
                if has_work:
                    multi.select(self._select_timeout)
        finally:
            for curl, (task, context) in list(self._inflight.items()):
                context.response_body.close()
                task.error = httpx.TransportError(
                    "transport closed before request completed"
                )
                task.done.set()
                try:
                    multi.remove_handle(curl)
                finally:
                    curl.close()
            self._inflight.clear()
            multi.close()

    def _enqueue_task(self, multi: pycurl.CurlMulti, task: _PendingTask):
        _pycurl = _require_pycurl()
        curl = _pycurl.Curl()
        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )
        try:
            _configure_curl(
                curl,
                task.request,
                context,
                timeout=self._timeout,
                verify=self._verify,
                follow_redirects=self._follow_redirects,
                user_agent=self._user_agent,
            )
            self._inflight[curl] = (task, context)
            multi.add_handle(curl)
        except Exception as error:
            context.response_body.close()
            curl.close()
            task.error = (
                error
                if isinstance(error, httpx.TransportError)
                else httpx.TransportError(str(error))
            )
            task.done.set()

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
        task, context = self._inflight.pop(curl)
        try:
            curl_response = _finalize_curl_response(curl, context)
            task.response = httpx.Response(
                status_code=curl_response.status_code,
                headers=curl_response.headers,
                stream=_SyncFileStream(curl_response.body_file),
                request=task.request,
                extensions={"http_version": curl_response.http_version},
            )
            task.done.set()
        except Exception as error:
            context.response_body.close()
            task.error = httpx.TransportError(str(error))
            task.done.set()
        finally:
            multi.remove_handle(curl)
            curl.close()

    def _complete_error(
        self,
        multi: pycurl.CurlMulti,
        curl: pycurl.Curl,
        code: int,
        message: str,
    ):
        task, context = self._inflight.pop(curl)
        context.response_body.close()
        task.error = httpx.TransportError(f"pycurl error {code}: {message}")
        task.done.set()
        multi.remove_handle(curl)
        curl.close()


class PyCurlAsyncTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        timeout: float | None = None,
        verify: bool = True,
        follow_redirects: bool = False,
        user_agent: str | None = None,
    ):
        self._sync_transport = PyCurlTransport(
            timeout=timeout,
            verify=verify,
            follow_redirects=follow_redirects,
            user_agent=user_agent,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        curl_response = await anyio.to_thread.run_sync(
            self._sync_transport._perform_request, request
        )
        return httpx.Response(
            status_code=curl_response.status_code,
            headers=curl_response.headers,
            stream=_AsyncFileStream(curl_response.body_file),
            request=request,
            extensions={"http_version": curl_response.http_version},
        )

    async def aclose(self):
        self._sync_transport.close()
