from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from pycurltx import transport


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

    async with httpx.AsyncClient(
        transport=transport.PyCurlTx(cainfo=ca_cert)
    ) as client:
        response = await client.get(server)
        assert "Welcome" in (await response.aread()).decode("utf-8")


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.anyio
async def test_fetch_nginx_parallel_data(ca_cert, server, regular, ssl_context):
    begin = time.monotonic_ns()
    if regular:
        client = httpx.AsyncClient(http2=True, verify=ssl_context)
    else:
        client = httpx.AsyncClient(transport=transport.PyCurlTx(cainfo=ca_cert))
    async with client as client:
        paths = ["/data/small", "/data/medium", "/data/large"]
        urls = [
            f"{server.removesuffix('/')}{path}" for path in paths for _ in range(20)
        ]

        responses = await asyncio.gather(*(client.get(url) for url in urls))

    assert all(response.status_code == 200 for response in responses)
    end = time.monotonic_ns()

    print(f"{client._transport} took {(end - begin) / 1e9:0.03f}s")

    for response in responses:
        await response.aread()
        # print(response.url, body[:16], len(body))
