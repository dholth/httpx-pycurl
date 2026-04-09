from __future__ import annotations

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
