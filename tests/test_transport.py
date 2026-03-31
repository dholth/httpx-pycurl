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

        def setopt(self, _option: object, _value: object):
            return

        def add_handle(self, curl: _FakeCurl):
            self._handles.append(curl)

        def remove_handle(self, curl: _FakeCurl):
            if curl in self._handles:
                self._handles.remove(curl)

        def perform(self) -> tuple[int, int]:
            pending = list(self._handles)
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
            return 0, len(self._handles)

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
