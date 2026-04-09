from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from types import SimpleNamespace

import httpx
import pytest

from pycurltx import transport


@dataclass(eq=False)
class _FakeCurl:
    module: SimpleNamespace
    options: dict[object, object] = field(default_factory=dict)
    status_code: int = 200

    def setopt(self, option: object, value: object):
        self.options[option] = value

    def perform(self):
        behavior = getattr(self.module, "behavior", None)
        if behavior is None:
            return
        behavior(self)

    def getinfo(self, option: object) -> int:
        if option == self.module.RESPONSE_CODE:
            return self.status_code
        return 0

    def close(self):
        return


def _install_fake_pycurl(monkeypatch: pytest.MonkeyPatch, behavior=None):
    class _FakeCurlError(Exception):
        pass

    class _FakeCurlMulti:
        def __init__(self):
            self._handles: list[_FakeCurl] = []
            self._completed: list[_FakeCurl] = []
            self._failed: list[tuple[_FakeCurl, int, str]] = []
            self._socket_callback = None
            self._timer_callback = None
            self._processed: set[_FakeCurl] = set()

        def setopt(self, option: object, value: object):
            if option == fake_module.M_SOCKETFUNCTION:
                self._socket_callback = value
            elif option == fake_module.M_TIMERFUNCTION:
                self._timer_callback = value

        def add_handle(self, curl: _FakeCurl):
            self._handles.append(curl)
            self._processed.discard(curl)
            if self._socket_callback is not None:
                self._socket_callback(fake_module.POLL_IN, 10, self, None)
            if self._timer_callback is not None:
                self._timer_callback(0)

        def remove_handle(self, curl: _FakeCurl):
            if curl in self._handles:
                self._handles.remove(curl)
            self._processed.discard(curl)

        def perform(self) -> tuple[int, int]:
            pending = [curl for curl in self._handles if curl not in self._processed]
            for curl in pending:
                try:
                    action = getattr(curl.module, "behavior", None)
                    if action is not None:
                        action(curl)
                except curl.module.error as error:
                    code, message = error.args
                    self._failed.append((curl, code, message))
                else:
                    self._completed.append(curl)
                self._processed.add(curl)
            return 0, len(self._handles)

        def socket_action(self, _sockfd: int, _events: int) -> tuple[int, int]:
            return self.perform()

        def info_read(self):
            completed = self._completed
            failed = self._failed
            self._completed = []
            self._failed = []
            return 0, completed, failed

        def select(self, _timeout: float) -> int:
            return 0

        def close(self):
            return

    fake_module = SimpleNamespace(
        URL="URL",
        WRITEFUNCTION="WRITEFUNCTION",
        HEADERFUNCTION="HEADERFUNCTION",
        NOSIGNAL="NOSIGNAL",
        FOLLOWLOCATION="FOLLOWLOCATION",
        SSL_VERIFYPEER="SSL_VERIFYPEER",
        SSL_VERIFYHOST="SSL_VERIFYHOST",
        TIMEOUT_MS="TIMEOUT_MS",
        USERAGENT="USERAGENT",
        VERBOSE="VERBOSE",
        DEBUGFUNCTION="DEBUGFUNCTION",
        HTTPHEADER="HTTPHEADER",
        HTTPGET="HTTPGET",
        NOBODY="NOBODY",
        CUSTOMREQUEST="CUSTOMREQUEST",
        READFUNCTION="READFUNCTION",
        POST="POST",
        POSTFIELDSIZE_LARGE="POSTFIELDSIZE_LARGE",
        UPLOAD="UPLOAD",
        INFILESIZE_LARGE="INFILESIZE_LARGE",
        RESPONSE_CODE="RESPONSE_CODE",
        E_CALL_MULTI_PERFORM=1,
        M_MAX_TOTAL_CONNECTIONS="M_MAX_TOTAL_CONNECTIONS",
        M_SOCKETFUNCTION="M_SOCKETFUNCTION",
        M_TIMERFUNCTION="M_TIMERFUNCTION",
        POLL_IN=1,
        POLL_OUT=2,
        POLL_INOUT=3,
        POLL_REMOVE=4,
        CSELECT_IN=1,
        CSELECT_OUT=2,
        SOCKET_TIMEOUT=-1,
        error=_FakeCurlError,
        behavior=behavior,
    )

    def factory() -> _FakeCurl:
        return _FakeCurl(module=fake_module)

    fake_module.CurlMulti = _FakeCurlMulti
    fake_module.Curl = factory
    monkeypatch.setattr(transport, "pycurl", fake_module)
    return fake_module


def test_sync_response_streaming(monkeypatch: pytest.MonkeyPatch):
    def behavior(curl: _FakeCurl):
        header = curl.options[curl.module.HEADERFUNCTION]
        writer = curl.options[curl.module.WRITEFUNCTION]
        header(b"HTTP/1.1 200 OK\r\n")
        header(b"Content-Type: text/plain\r\n")
        header(b"\r\n")
        writer(b"hello")
        writer(b" world")

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlTransport()
    request = httpx.Request("GET", "https://example.com")

    response = tx.handle_request(request)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain"
    assert response.read() == b"hello world"


def test_streaming_post_request_body(monkeypatch: pytest.MonkeyPatch):
    def behavior(curl: _FakeCurl):
        read_body = curl.options[curl.module.READFUNCTION]
        sent = b""
        while True:
            chunk = read_body(2)
            if not chunk:
                break
            sent += chunk

        assert sent == b"abcdef"
        assert curl.options[curl.module.POST] == 1
        assert curl.options[curl.module.POSTFIELDSIZE_LARGE] == 6

        header = curl.options[curl.module.HEADERFUNCTION]
        writer = curl.options[curl.module.WRITEFUNCTION]
        curl.status_code = 201
        header(b"HTTP/1.1 201 Created\r\n")
        header(b"\r\n")
        writer(b"ok")

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlTransport()
    request = httpx.Request(
        "POST",
        "https://example.com/upload",
        headers={"content-length": "6"},
        content=[b"abc", b"def"],
    )

    response = tx.handle_request(request)

    assert response.status_code == 201
    assert response.read() == b"ok"


@pytest.mark.anyio
async def test_async_response_streaming(monkeypatch: pytest.MonkeyPatch):
    def behavior(curl: _FakeCurl):
        header = curl.options[curl.module.HEADERFUNCTION]
        writer = curl.options[curl.module.WRITEFUNCTION]
        header(b"HTTP/1.1 200 OK\r\n")
        header(b"\r\n")
        writer(b"async")

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlAsyncTransport()
    request = httpx.Request("GET", "https://example.com")

    response = await tx.handle_async_request(request)

    assert await response.aread() == b"async"


def test_maps_pycurl_error_to_httpx(monkeypatch: pytest.MonkeyPatch):
    def behavior(curl: _FakeCurl):
        raise curl.module.error(7, "failed connect")

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlTransport()
    request = httpx.Request("GET", "https://example.com")

    with pytest.raises(httpx.TransportError, match=r"pycurl error 7: failed connect"):
        tx.handle_request(request)


def test_verbose_debug_callback(monkeypatch: pytest.MonkeyPatch):
    debug_messages: list[tuple[int, bytes]] = []

    def behavior(curl: _FakeCurl):
        assert curl.options[curl.module.VERBOSE] == 1
        debug = curl.options[curl.module.DEBUGFUNCTION]
        debug(0, b"hello debug")
        header = curl.options[curl.module.HEADERFUNCTION]
        writer = curl.options[curl.module.WRITEFUNCTION]
        header(b"HTTP/1.1 200 OK\r\n")
        header(b"\r\n")
        writer(b"ok")

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlTransport(
        verbose=True,
        debug_callback=lambda info_type, data: debug_messages.append((info_type, data)),
    )
    response = tx.handle_request(httpx.Request("GET", "https://example.com"))

    assert response.read() == b"ok"
    assert debug_messages == [(0, b"hello debug")]


def test_multi_transport_handles_concurrent_calls(monkeypatch: pytest.MonkeyPatch):
    def behavior(curl: _FakeCurl):
        header = curl.options[curl.module.HEADERFUNCTION]
        writer = curl.options[curl.module.WRITEFUNCTION]
        url = curl.options[curl.module.URL]
        header(b"HTTP/1.1 200 OK\r\n")
        header(b"\r\n")
        writer(url.encode("ascii"))

    _install_fake_pycurl(monkeypatch, behavior=behavior)
    tx = transport.PyCurlMultiTransport()

    def send(url: str) -> bytes:
        request = httpx.Request("GET", url)
        response = tx.handle_request(request)
        return response.read()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(send, "https://example.com/one")
        second = pool.submit(send, "https://example.com/two")

    assert first.result() == b"https://example.com/one"
    assert second.result() == b"https://example.com/two"
    tx.close()


@pytest.mark.anyio
async def test_async_multi_socket_transport(monkeypatch: pytest.MonkeyPatch):
    # logging.basicConfig(level=logging.DEBUG)

    async with transport.PyCurlTx() as tx:
        request = httpx.Request("GET", "http://www.example.com/")

        response = await tx.handle_async_request(request)

    print("Response", response.status_code, await response.aread())


@pytest.mark.anyio
async def test_fetch_nginx(ca_cert, server):
    # note when running editor from flatpak on Linux /tmp/ is separate
    async with transport.PyCurlTx(cainfo=ca_cert) as tx:
        request = httpx.Request("GET", server)
        response = await tx.handle_async_request(request)
        assert response.status_code == 200
        body = (await response.aread()).decode("utf-8")
        assert "Welcome" in body
        print(body, response.status_code)

    async with transport.PyCurlTx() as tx:
        # E_PEER_FAILED_VERIFICATION 60
        request = httpx.Request("GET", server)
        with pytest.raises(httpx.TransportError, match="SSL"):
            # demonstrate error when no cert
            response = await tx.handle_async_request(request)
