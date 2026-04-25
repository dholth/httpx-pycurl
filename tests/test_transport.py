from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from httpx_pycurl import transport


@pytest.mark.asyncio
async def test_async_multi_socket_transport(monkeypatch: pytest.MonkeyPatch):
    # logging.basicConfig(level=logging.DEBUG)

    async with transport.AsyncPyCurlTransport() as tx:
        request = httpx.Request("GET", "http://www.example.com/")

        response = await tx.handle_async_request(request)

    print("Response", response.status_code, await response.aread())


@pytest.mark.asyncio
async def test_fetch_nginx(ca_cert, server):
    # note when running editor from flatpak on Linux /tmp/ is separate
    async with transport.AsyncPyCurlTransport(cainfo=ca_cert) as tx:
        request = httpx.Request("GET", server)
        response = await tx.handle_async_request(request)
        assert response.status_code == 200
        body = (await response.aread()).decode("utf-8")
        assert "Welcome" in body
        print(body, response.status_code)

    async with transport.AsyncPyCurlTransport() as tx:
        # E_PEER_FAILED_VERIFICATION 60
        request = httpx.Request("GET", server)
        with pytest.raises(httpx.TransportError, match="SSL"):
            # demonstrate error when no cert
            response = await tx.handle_async_request(request)

    async with httpx.AsyncClient(
        transport=transport.AsyncPyCurlTransport(cainfo=ca_cert)
    ) as client:
        response = await client.get(server)
        assert "Welcome" in (await response.aread()).decode("utf-8")


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_fetch_nginx_parallel_data(ca_cert, server, regular, ssl_context):
    begin = time.monotonic_ns()
    if regular:
        client = httpx.AsyncClient(http2=True, verify=ssl_context)
    else:
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(cainfo=ca_cert)
        )
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
        body = await response.aread()
        expected = ord(b"0")
        assert all((b == expected) for b in body)
        assert len(body) in (1024, 16 * 1024, 1024 * 1024)


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_fetch_nginx_parallel_streaming(ca_cert, server, regular, ssl_context):
    """Test streaming response bodies with stream() instead of get()."""
    begin = time.monotonic_ns()
    if regular:
        client = httpx.AsyncClient(http2=True, verify=ssl_context)
    else:
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(cainfo=ca_cert)
        )
    async with client as client:
        paths = ["/data/small", "/data/medium", "/data/large"]
        urls = [
            f"{server.removesuffix('/')}{path}" for path in paths for _ in range(20)
        ]

        # Use stream() instead of get() to test async streaming response bodies
        async def fetch_streaming(url):
            async with client.stream("GET", url) as response:
                return response.status_code, await response.aread()

        results = await asyncio.gather(*(fetch_streaming(url) for url in urls))

    assert all(status_code == 200 for status_code, _ in results)
    end = time.monotonic_ns()

    print(f"{client._transport} (streaming) took {(end - begin) / 1e9:0.03f}s")

    for status_code, body in results:
        assert status_code == 200
        expected = ord(b"0")
        assert all((b == expected) for b in body)
        assert len(body) in (1024, 16 * 1024, 1024 * 1024)


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_fetch_nginx_parallel_streaming_chunks(
    ca_cert, server, regular, ssl_context
):
    """Test streaming response bodies with chunk iteration instead of aread()."""
    begin = time.monotonic_ns()
    if regular:
        client = httpx.AsyncClient(http2=True, verify=ssl_context)
    else:
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(cainfo=ca_cert)
        )
    async with client as client:
        paths = ["/data/small", "/data/medium", "/data/large"]
        urls = [
            f"{server.removesuffix('/')}{path}" for path in paths for _ in range(20)
        ]

        # Stream with chunk iteration for true streaming performance
        async def fetch_streaming_chunks(url):
            async with client.stream("GET", url) as response:
                body_size = 0
                chunk_count = 0
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    body_size += len(chunk)
                    chunk_count += 1
                return response.status_code, body_size, chunk_count

        results = await asyncio.gather(*(fetch_streaming_chunks(url) for url in urls))

    assert all(status_code == 200 for status_code, _, _ in results)
    end = time.monotonic_ns()

    print(f"{client._transport} (streaming chunks) took {(end - begin) / 1e9:0.03f}s")

    for status_code, body_size, chunk_count in results:
        assert status_code == 200
        assert body_size in (1024, 16 * 1024, 1024 * 1024)
        assert chunk_count > 0


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_timeout_behavior(regular, slow_server):
    """Test timeout behavior comparing httpx and pycurl transports.

    Both should timeout with a short timeout against a slow server endpoint.
    """
    url = f"{slow_server}delay"
    timeout = 0.5  # 500ms timeout

    if regular:
        # httpx AsyncHTTPTransport
        client = httpx.AsyncClient(timeout=timeout)
        transport_name = "httpx.AsyncHTTPTransport"
    else:
        # AsyncPyCurlTransport
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(timeout=timeout)
        )
        transport_name = "AsyncPyCurlTransport"

    async with client:
        start = time.monotonic()
        with pytest.raises(
            (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TransportError)
        ):
            await client.get(url)
        elapsed = time.monotonic() - start

        # Verify timeout was triggered reasonably quickly (within 2x timeout window)
        assert elapsed < timeout * 2 + 0.5, (
            f"{transport_name} took too long: {elapsed:.2f}s"
        )
        print(f"{transport_name} timeout after {elapsed:.2f}s")


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_timeout_behavior_streaming(regular, slow_server):
    """Test timeout behavior with streaming chunk iteration.

    Both should timeout when iterating over response chunks with a slow server.
    """
    url = f"{slow_server}delay"
    timeout = 0.5  # 500ms timeout

    if regular:
        # httpx AsyncHTTPTransport
        client = httpx.AsyncClient(timeout=timeout)
        transport_name = "httpx.AsyncHTTPTransport"
    else:
        # AsyncPyCurlTransport
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(timeout=timeout)
        )
        transport_name = "AsyncPyCurlTransport"

    async with client:
        start = time.monotonic()
        with pytest.raises(
            (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TransportError)
        ):
            async with client.stream("GET", url) as response:
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    pass  # Iterate through chunks
        elapsed = time.monotonic() - start

        # Verify timeout was triggered reasonably quickly
        assert elapsed < timeout * 2 + 0.5, (
            f"{transport_name} took too long: {elapsed:.2f}s"
        )
        print(f"{transport_name} streaming timeout after {elapsed:.2f}s")


@pytest.mark.parametrize("regular", [True, False])
@pytest.mark.asyncio
async def test_response_closed_early(regular, slow_server):
    """Test error behavior when server closes before content-length bytes were delivered."""
    url = f"{slow_server}short"
    timeout = 0.5  # 500ms timeout

    if regular:
        # httpx AsyncHTTPTransport
        client = httpx.AsyncClient(timeout=timeout)
        transport_name = "httpx.AsyncHTTPTransport"
    else:
        # AsyncPyCurlTransport
        client = httpx.AsyncClient(
            transport=transport.AsyncPyCurlTransport(timeout=timeout)
        )
        transport_name = "AsyncPyCurlTransport"

    async with client:
        start = time.monotonic()
        # httpx's default transport may raise ReadTimeout if timeout fires first,
        # while pycurl raises RemoteProtocolError when server closes early
        with pytest.raises(httpx.RemoteProtocolError):
            async with client.stream("GET", url) as response:
                async for chunk in response.aiter_bytes():
                    pass  # Consume all chunks
        elapsed = time.monotonic() - start

        # Verify timeout was triggered reasonably quickly
        assert elapsed < timeout * 2 + 0.5, (
            f"{transport_name} took too long: {elapsed:.2f}s"
        )
