"""Low-level async wrapper around pycurl.CurlMulti.

AsyncCurl manages a CurlMulti handle and wires socket/timer callbacks into
an asyncio event loop. It knows nothing about httpx - just handles curl
transfer lifecycle and completion signaling.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import pycurl

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AsyncCurl:
    """Manages async execution of pycurl Curl handles via CurlMulti.

    Takes pre-configured Curl objects and drives them to completion using
    an asyncio event loop. Returns the handle when successful, raises
    pycurl.error on failure.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        """Initialize AsyncCurl.

        Args:
            loop: Optional event loop. If None, detected from get_running_loop()
                  on first perform() call.
        """
        self._loop = loop
        self._multi: pycurl.CurlMulti | None = None
        self._closed = False

        # Track in-flight transfers: curl handle -> Future
        self._transfers: dict[pycurl.Curl, asyncio.Future] = {}

        # Socket management
        self._socket_watch: dict[int, int] = {}
        self._socket_callbacks_registered = False

        # Timer management
        self._timer_handle: asyncio.TimerHandle | None = None

    async def perform(self, curl: pycurl.Curl) -> pycurl.Curl:
        """Execute a curl handle to completion.

        Args:
            curl: Pre-configured pycurl.Curl handle.

        Returns:
            The same curl handle on success.

        Raises:
            pycurl.error: If the transfer fails.
        """
        if self._closed:
            raise RuntimeError("AsyncCurl is closed")

        # Lazy loop detection
        loop = self._ensure_loop()

        # Create future for this transfer
        future: asyncio.Future[None] = loop.create_future()
        self._transfers[curl] = future

        try:
            # Add to multi handle
            multi = self._ensure_multi()
            multi.add_handle(curl)

            # Trigger initial processing
            self._drive_socket(pycurl.SOCKET_TIMEOUT, 0)

            # Wait for completion
            await future

            # On success, return the handle
            return curl

        except Exception:
            # Remove transfer tracking on error
            self._transfers.pop(curl, None)
            # Don't close the handle - let caller decide
            raise

    async def aclose(self) -> None:
        """Close and clean up resources.

        Can be called multiple times safely. After close, perform() will
        raise RuntimeError on new calls, but the object can be reused if
        _multi is recreated.
        """
        self._closed = True
        self._cancel_timer()
        self._cleanup_sockets()

        if self._multi is not None:
            # Don't remove handles here - leave them to caller
            # Just clean up the multi object
            try:
                self._multi.close()
            except Exception as e:
                logger.exception("Error closing CurlMulti: %s", e)
            self._multi = None

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Get or detect the event loop."""
        if self._loop is not None:
            return self._loop

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "AsyncCurl requires asyncio event loop; call from async context or "
                "pass loop to __init__"
            )

        return self._loop

    def _ensure_multi(self) -> pycurl.CurlMulti:
        """Get or create the CurlMulti handle."""
        if self._multi is not None:
            return self._multi

        self._ensure_loop()
        multi = pycurl.CurlMulti()

        # Register callbacks
        multi.setopt(pycurl.M_SOCKETFUNCTION, self._socket_callback)
        multi.setopt(pycurl.M_TIMERFUNCTION, self._timer_callback)

        self._multi = multi
        return multi

    def _socket_callback(
        self, what: int, fd: int, multi: pycurl.CurlMulti, socketp: object
    ) -> int:
        """Called by libcurl when socket status changes."""
        self._register_socket(fd, what)
        return 0

    def _timer_callback(self, timeout_ms: int) -> int:
        """Called by libcurl to schedule next timeout."""
        self._schedule_timeout(timeout_ms)
        return 0

    def _register_socket(self, fd: int, what: int) -> None:
        """Register socket with event loop based on event mask."""
        loop = self._ensure_loop()

        # Clean up previous registration
        loop.remove_reader(fd)
        loop.remove_writer(fd)

        if what == pycurl.POLL_REMOVE:
            self._socket_watch.pop(fd, None)
            return

        self._socket_watch[fd] = what

        try:
            if what in {pycurl.POLL_IN, pycurl.POLL_INOUT}:
                loop.add_reader(fd, self._on_socket_readable, fd)
            if what in {pycurl.POLL_OUT, pycurl.POLL_INOUT}:
                loop.add_writer(fd, self._on_socket_writable, fd)
        except OSError as e:
            logger.warning("Failed to register socket %d: %s", fd, e)
            self._socket_watch.pop(fd, None)
            loop.remove_reader(fd)
            loop.remove_writer(fd)

    def _cleanup_sockets(self) -> None:
        """Unregister all sockets from event loop."""
        if self._loop is None:
            return

        for fd in list(self._socket_watch):
            try:
                self._loop.remove_reader(fd)
                self._loop.remove_writer(fd)
            except Exception:
                pass

        self._socket_watch.clear()

    def _schedule_timeout(self, timeout_ms: int) -> None:
        """Schedule or reschedule a timeout callback."""
        self._cancel_timer()

        if timeout_ms < 0:
            return

        loop = self._ensure_loop()

        if timeout_ms == 0:
            loop.call_soon(self._on_timeout)
        else:
            self._timer_handle = loop.call_later(timeout_ms / 1000.0, self._on_timeout)

    def _cancel_timer(self) -> None:
        """Cancel any pending timer."""
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

    def _on_socket_readable(self, fd: int) -> None:
        """Called when socket is readable."""
        self._drive_socket(fd, pycurl.CSELECT_IN)

    def _on_socket_writable(self, fd: int) -> None:
        """Called when socket is writable."""
        self._drive_socket(fd, pycurl.CSELECT_OUT)

    def _on_timeout(self) -> None:
        """Called when timer expires."""
        self._timer_handle = None
        self._drive_socket(pycurl.SOCKET_TIMEOUT, 0)

    def _drive_socket(self, sock_fd: int, event_mask: int) -> None:
        """Process socket activity in the multi handle."""
        multi = self._ensure_multi()

        # Call socket_action until no more immediate work
        while True:
            status, _running = multi.socket_action(sock_fd, event_mask)
            if status != pycurl.E_CALL_MULTI_PERFORM:
                break

        # Drain completed transfers
        self._drain_info_read()

    def _drain_info_read(self) -> None:
        """Process completed and failed transfers from multi handle."""
        multi = self._ensure_multi()

        while True:
            queued, successful, failed = multi.info_read()

            for curl in successful:
                self._complete_transfer(curl, None, None)

            for curl, code, message in failed:
                self._complete_transfer(curl, code, message)

            if queued == 0:
                break

    def _complete_transfer(
        self, curl: pycurl.Curl, error_code: int | None, error_message: str | None
    ) -> None:
        """Mark a transfer as complete (success or failure)."""
        future = self._transfers.pop(curl, None)
        if future is None:
            return

        if error_code is not None:
            # Transfer failed
            exc = pycurl.error(error_code, error_message or "Unknown error")
            if not future.done():
                future.set_exception(exc)
        else:
            # Transfer succeeded
            if not future.done():
                future.set_result(None)

        # Remove from multi handle
        try:
            multi = self._ensure_multi()
            multi.remove_handle(curl)
        except Exception as e:
            logger.warning("Error removing handle from multi: %s", e)
