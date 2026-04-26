"""Tests for low-level AsyncCurl implementation."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pycurl
import pytest

from httpx_pycurl.curl import AsyncCurl


@pytest.mark.asyncio
async def test_asynccurl_init_with_event_loop():
    """Test AsyncCurl initialization with explicit event loop."""
    loop = asyncio.get_event_loop()
    curl = AsyncCurl(loop=loop)
    assert curl._loop is loop
    assert curl._multi is not None
    assert not curl._closed
    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_init_without_event_loop():
    """Test AsyncCurl initialization detects running loop."""
    curl = AsyncCurl()
    assert curl._loop is asyncio.get_running_loop()
    assert curl._multi is not None
    await curl.aclose()


def test_asynccurl_init_no_running_loop():
    """Test AsyncCurl initialization fails without event loop outside async context."""
    with pytest.raises(RuntimeError, match="AsyncCurl requires asyncio event loop"):
        AsyncCurl()


@pytest.mark.asyncio
async def test_asynccurl_context_manager():
    """Test AsyncCurl as async context manager."""
    async with AsyncCurl() as curl:
        assert curl._loop is asyncio.get_running_loop()
        assert curl._multi is not None
        assert not curl._closed

    assert curl._closed


@pytest.mark.asyncio
async def test_asynccurl_aclose_idempotent():
    """Test aclose can be called multiple times safely."""
    curl = AsyncCurl()
    await curl.aclose()
    await curl.aclose()  # Should not raise
    assert curl._closed


@pytest.mark.asyncio
async def test_asynccurl_perform_when_closed():
    """Test perform raises RuntimeError when closed."""
    curl = AsyncCurl()
    await curl.aclose()

    handle = pycurl.Curl()
    with pytest.raises(RuntimeError, match="AsyncCurl is closed"):
        curl.perform(handle)


@pytest.mark.asyncio
async def test_asynccurl_socket_callback():
    """Test _socket_callback is registered."""
    curl = AsyncCurl()
    # Verify callback is set
    assert curl._socket_callback is not None

    # Mock the _register_socket to avoid real socket operations
    with patch.object(curl, "_register_socket") as mock_register:
        result = curl._socket_callback(pycurl.POLL_REMOVE, -1, curl._multi, None)
        assert result == 0
        mock_register.assert_called_once_with(-1, pycurl.POLL_REMOVE)

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_timer_callback():
    """Test _timer_callback is registered."""
    curl = AsyncCurl()
    # Verify callback is set
    assert curl._timer_callback is not None
    result = curl._timer_callback(0)
    assert result == 0

    # Cancel the scheduled timer
    curl._cancel_timer()
    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_perform_with_simple_handle():
    """Test perform with a simple curl handle."""
    curl_obj = AsyncCurl()

    # Create a simple curl handle (doesn't actually make a request)
    handle = pycurl.Curl()

    # Set up the handle to do a simple mock operation
    handle.setopt(pycurl.URL, "http://httpbin.org/get")
    handle.setopt(pycurl.TIMEOUT, 2)

    try:
        # perform() returns immediately with a PerformHandle; wait_for_completion() is awaitable
        perform_handle = curl_obj.perform(handle)
        assert perform_handle.curl is handle
        # Wait for the transfer to complete
        result = await asyncio.wait_for(curl_obj.wait_for_completion(perform_handle), timeout=10)
        assert result is handle
    finally:
        await curl_obj.aclose()
        handle.close()


@pytest.mark.asyncio
async def test_asynccurl_socket_registration_removal():
    """Test socket registration and removal."""
    curl = AsyncCurl()

    # Test registering a socket with POLL_REMOVE removes it
    curl._register_socket(999, pycurl.POLL_REMOVE)
    assert 999 not in curl._socket_watch

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_cleanup_sockets():
    """Test socket cleanup."""
    curl = AsyncCurl()

    # Add a fake socket watch (we can't register real sockets without real I/O)
    curl._socket_watch[999] = pycurl.POLL_IN

    # Clean up should not raise
    curl._cleanup_sockets()
    assert len(curl._socket_watch) == 0

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_timer_scheduling():
    """Test timer scheduling and cancellation."""
    curl = AsyncCurl()

    # Schedule a timeout
    curl._schedule_timeout(100)
    assert curl._timer_handle is not None

    # Cancel it
    curl._cancel_timer()
    assert curl._timer_handle is None

    # Schedule a zero timeout (call_soon)
    curl._schedule_timeout(0)
    assert curl._timer_handle is None  # call_soon doesn't return a handle

    # Negative timeout should not schedule
    curl._schedule_timeout(-1)
    assert curl._timer_handle is None

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_drive_socket():
    """Test _drive_socket processes socket actions."""
    curl = AsyncCurl()

    # Mock the multi handle to avoid actual I/O
    with patch.object(curl._multi, "socket_action") as mock_action:
        with patch.object(curl._multi, "info_read") as mock_info:
            mock_action.return_value = (pycurl.E_OK, 0)
            mock_info.return_value = (0, [], [])

            # Should not raise
            curl._drive_socket(pycurl.SOCKET_TIMEOUT, 0)

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_drain_with_successful_transfers():
    """Test _drain_info_read processes successful transfers."""
    curl_obj = AsyncCurl()

    # Create a mock curl handle
    handle = MagicMock()

    # Create a future and add to transfers
    future: asyncio.Future[None] = curl_obj._loop.create_future()
    curl_obj._transfers[handle] = future

    # Replace multi with mock
    mock_multi = MagicMock()
    mock_multi.info_read.side_effect = [(1, [handle], []), (0, [], [])]
    curl_obj._multi = mock_multi

    curl_obj._drain_info_read()

    # Future should be resolved
    assert future.done()
    assert future.exception() is None

    # Don't await aclose since we replaced _multi with a mock
    curl_obj._closed = True


@pytest.mark.asyncio
async def test_asynccurl_drain_with_failed_transfers():
    """Test _drain_info_read processes failed transfers."""
    curl_obj = AsyncCurl()

    # Create a mock curl handle
    handle = MagicMock()

    # Create a future and add to transfers
    future: asyncio.Future[None] = curl_obj._loop.create_future()
    curl_obj._transfers[handle] = future

    # Replace multi with mock
    mock_multi = MagicMock()
    mock_multi.info_read.side_effect = [
        (1, [], [(handle, 52, "CURLE_GOT_NOTHING")]),
        (0, [], []),
    ]
    curl_obj._multi = mock_multi

    curl_obj._drain_info_read()

    # Future should have exception
    assert future.done()
    assert isinstance(future.exception(), pycurl.error)

    # Don't await aclose since we replaced _multi with a mock
    curl_obj._closed = True


@pytest.mark.asyncio
async def test_asynccurl_complete_transfer_unknown_handle():
    """Test _complete_transfer with unknown handle doesn't crash."""
    curl_obj = AsyncCurl()
    handle = MagicMock()

    # Should not raise even if handle is not tracked
    curl_obj._complete_transfer(handle, None, None)

    await curl_obj.aclose()


@pytest.mark.asyncio
async def test_asynccurl_on_socket_readable():
    """Test _on_socket_readable calls _drive_socket."""
    curl = AsyncCurl()

    with patch.object(curl, "_drive_socket") as mock_drive:
        curl._on_socket_readable(999)
        mock_drive.assert_called_once_with(999, pycurl.CSELECT_IN)

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_on_socket_writable():
    """Test _on_socket_writable calls _drive_socket."""
    curl = AsyncCurl()

    with patch.object(curl, "_drive_socket") as mock_drive:
        curl._on_socket_writable(999)
        mock_drive.assert_called_once_with(999, pycurl.CSELECT_OUT)

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_on_timeout():
    """Test _on_timeout calls _drive_socket."""
    curl = AsyncCurl()

    with patch.object(curl, "_drive_socket") as mock_drive:
        curl._on_timeout()
        mock_drive.assert_called_once_with(pycurl.SOCKET_TIMEOUT, 0)

    assert curl._timer_handle is None
    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_register_socket_poll_in():
    """Test socket registration with POLL_IN."""
    curl = AsyncCurl()

    with patch.object(curl._loop, "add_reader") as mock_add:
        with patch.object(curl._loop, "add_writer") as mock_add_w:
            curl._register_socket(999, pycurl.POLL_IN)
            mock_add.assert_called_once()
            mock_add_w.assert_not_called()

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_register_socket_poll_out():
    """Test socket registration with POLL_OUT."""
    curl = AsyncCurl()

    with patch.object(curl._loop, "add_reader") as mock_add:
        with patch.object(curl._loop, "add_writer") as mock_add_w:
            curl._register_socket(999, pycurl.POLL_OUT)
            mock_add.assert_not_called()
            mock_add_w.assert_called_once()

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_register_socket_poll_inout():
    """Test socket registration with POLL_INOUT."""
    curl = AsyncCurl()

    with patch.object(curl._loop, "add_reader") as mock_add:
        with patch.object(curl._loop, "add_writer") as mock_add_w:
            curl._register_socket(999, pycurl.POLL_INOUT)
            mock_add.assert_called_once()
            mock_add_w.assert_called_once()

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_register_socket_os_error():
    """Test socket registration handles OSError."""
    curl = AsyncCurl()

    with patch.object(curl._loop, "add_reader", side_effect=OSError("mock error")):
        with patch.object(curl._loop, "remove_reader"):
            with patch.object(curl._loop, "remove_writer"):
                # Should not raise
                curl._register_socket(999, pycurl.POLL_IN)
                assert 999 not in curl._socket_watch

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_aclose_with_multi_error(caplog):
    """Test aclose handles errors when closing multi."""
    curl = AsyncCurl()

    # Replace with a mock that raises on close()
    mock_multi = MagicMock()
    mock_multi.close.side_effect = Exception("mock error")
    curl._multi = mock_multi

    with caplog.at_level(logging.ERROR):
        await curl.aclose()

    # Check that an exception was logged
    assert (
        len([r for r in caplog.records if "Error closing CurlMulti" in r.message]) > 0
    )


@pytest.mark.asyncio
async def test_asynccurl_complete_transfer_with_error_remove_error(caplog):
    """Test _complete_transfer handles error removing handle from multi."""
    curl_obj = AsyncCurl()

    handle = MagicMock()
    future: asyncio.Future[None] = curl_obj._loop.create_future()
    curl_obj._transfers[handle] = future

    with caplog.at_level(logging.WARNING):
        with patch.object(
            curl_obj._multi, "remove_handle", side_effect=Exception("mock error")
        ):
            curl_obj._complete_transfer(handle, None, None)
            assert "Error removing handle from multi" in caplog.text

    await curl_obj.aclose()


@pytest.mark.asyncio
async def test_asynccurl_socket_watch_updated():
    """Test socket watch dictionary is properly maintained."""
    curl = AsyncCurl()

    with patch.object(curl._loop, "add_reader"):
        with patch.object(curl._loop, "add_writer"):
            curl._register_socket(999, pycurl.POLL_IN)
            assert 999 in curl._socket_watch
            assert curl._socket_watch[999] == pycurl.POLL_IN

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_multi_socket_action_e_call_multi_perform():
    """Test _drive_socket with multiple socket_action calls."""
    curl = AsyncCurl()

    # Create a mock multi object to replace the real one
    mock_multi = MagicMock()
    mock_multi.socket_action.side_effect = [
        (pycurl.E_CALL_MULTI_PERFORM, 0),
        (pycurl.E_OK, 0),
    ]
    mock_multi.info_read.return_value = (0, [], [])

    curl._multi = mock_multi

    curl._drive_socket(pycurl.SOCKET_TIMEOUT, 0)
    assert mock_multi.socket_action.call_count == 2

    await curl.aclose()


@pytest.mark.asyncio
async def test_asynccurl_complete_transfer_already_done():
    """Test _complete_transfer when future is already done."""
    curl_obj = AsyncCurl()

    handle = MagicMock()
    future: asyncio.Future[None] = curl_obj._loop.create_future()
    future.set_result(None)  # Already done
    curl_obj._transfers[handle] = future

    mock_multi = MagicMock()
    curl_obj._multi = mock_multi

    # Should not raise, should not set_result again
    curl_obj._complete_transfer(handle, None, None)

    curl_obj._closed = True


@pytest.mark.asyncio
async def test_asynccurl_cleanup_sockets_with_error():
    """Test _cleanup_sockets handles errors removing sockets."""
    curl = AsyncCurl()

    # Add socket watches
    curl._socket_watch[999] = pycurl.POLL_IN
    curl._socket_watch[1000] = pycurl.POLL_OUT

    # Make removal raise an exception
    with patch.object(curl._loop, "remove_reader", side_effect=Exception("mock error")):
        with patch.object(
            curl._loop, "remove_writer", side_effect=Exception("mock error")
        ):
            # Should not raise
            curl._cleanup_sockets()

    # Socket watch should still be cleared
    assert len(curl._socket_watch) == 0

    await curl.aclose()
