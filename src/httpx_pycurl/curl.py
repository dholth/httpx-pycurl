"""
Low-level async wrapper around pycurl.CurlMulti.

AsyncCurl manages a CurlMulti handle and wires socket/timer callbacks
into an asyncio event loop. Caller is responsible for initializing curl
handle and translating response to a higher-level framework; this only
handles transfer lifecycle and completion signaling.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pycurl

logger = logging.getLogger(__name__)


@dataclass
class PerformHandle:
    """Handle to a transfer in progress.

    Allows non-blocking initiation of a transfer with completion
    tracked via a future that can be awaited later.
    """

    curl: pycurl.Curl
    completion_future: asyncio.Future[None]


class AsyncCurl:
    """Manages async execution of pycurl Curl handles via CurlMulti.

    Takes pre-configured Curl objects and drives them to completion using
    an asyncio event loop. Uses a non-blocking perform() API that returns
    a handle immediately, allowing the caller to decide when to wait for
    completion.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        """Initialize AsyncCurl.

        Args:
            loop: Optional event loop. If None, detected from get_running_loop().
                  AsyncCurl is one-time-use and initializes multi/loop eagerly.

        Raises:
            RuntimeError: If no event loop is available and none provided.
        """
        # Get loop eagerly
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError(
                    "AsyncCurl requires asyncio event loop; call from async context or "
                    "pass loop to __init__"
                )
        self._loop = loop
        self._closed = False

        # Create multi handle immediately
        self._multi = pycurl.CurlMulti()
        self._multi.setopt(pycurl.M_SOCKETFUNCTION, self._socket_callback)
        self._multi.setopt(pycurl.M_TIMERFUNCTION, self._timer_callback)

        # Track in-flight transfers: curl handle -> Future
        self._transfers: dict[pycurl.Curl, asyncio.Future] = {}

        # Socket management
        self._socket_watch: dict[int, int] = {}

        # Timer management
        self._timer_handle: asyncio.TimerHandle | None = None

    def setopt(self, option, value) -> None:
        """
        Forward options to our CurlMulti() instance.
        """
        return self._multi.setopt(option, value)

    def perform(self, curl: pycurl.Curl) -> PerformHandle:
        """Start a transfer without blocking.

        Initiates the transfer and returns a handle immediately. The handle's
        completion_future can be awaited later to wait for the transfer to complete.

        Args:
            curl: Pre-configured pycurl.Curl handle.

        Returns:
            PerformHandle with curl and completion_future.

        Raises:
            RuntimeError: If AsyncCurl is closed.
        """
        if self._closed:
            raise RuntimeError("AsyncCurl is closed")

        # Create future for this transfer
        future: asyncio.Future[None] = self._loop.create_future()
        self._transfers[curl] = future

        # Add to multi handle (this registers socket callbacks)
        self._multi.add_handle(curl)

        # Trigger initial processing
        self._drive_socket(pycurl.SOCKET_TIMEOUT, 0)

        # Return handle immediately (non-blocking)
        return PerformHandle(curl=curl, completion_future=future)

    async def wait_for_completion(self, handle: PerformHandle) -> pycurl.Curl:
        """Wait for a transfer to complete.

        Args:
            handle: PerformHandle returned from perform().

        Returns:
            The curl handle on success.

        Raises:
            pycurl.error: If the transfer fails.
        """
        try:
            await handle.completion_future
            return handle.curl
        except Exception:
            # Remove transfer tracking on error
            self._transfers.pop(handle.curl, None)
            # Don't close the handle - let caller decide
            raise

    async def aclose(self) -> None:
        """Close and clean up resources.

        Can be called multiple times safely.
        """
        self._closed = True
        self._cancel_timer()
        self._cleanup_sockets()

        if self._multi is not None:
            # Remove all handles before closing multi
            for curl in list(self._transfers.keys()):
                try:
                    self._multi.remove_handle(curl)
                except Exception:
                    pass
            # Just clean up the multi object
            try:
                self._multi.close()
            except Exception as e:
                logger.exception("Error closing CurlMulti: %s", e)
            self._multi = None

        self._transfers.clear()

    async def __aenter__(self) -> AsyncCurl:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.aclose()

    def _register_socket(self, fd: int, what: int) -> None:
        """Register socket with event loop based on event mask."""
        current_mode = self._socket_watch.get(fd)

        if what == pycurl.POLL_REMOVE:
            if current_mode is not None:
                try:
                    self._loop.remove_reader(fd)
                except (ValueError, RuntimeError):
                    pass
                try:
                    self._loop.remove_writer(fd)
                except (ValueError, RuntimeError):
                    pass
                self._socket_watch.pop(fd, None)
            return

        # Skip if mode hasn't changed
        if current_mode == what:
            return

        # Clean up previous registration (only necessary if mode changed)
        try:
            self._loop.remove_reader(fd)
        except (ValueError, RuntimeError):
            pass
        try:
            self._loop.remove_writer(fd)
        except (ValueError, RuntimeError):
            pass

        self._socket_watch[fd] = what

        try:
            if what in {pycurl.POLL_IN, pycurl.POLL_INOUT}:
                self._loop.add_reader(fd, self._on_socket_readable, fd)
            if what in {pycurl.POLL_OUT, pycurl.POLL_INOUT}:
                self._loop.add_writer(fd, self._on_socket_writable, fd)
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Failed to register socket %d: %s", fd, e)
            self._socket_watch.pop(fd, None)
            try:
                self._loop.remove_reader(fd)
            except (ValueError, RuntimeError):
                pass
            try:
                self._loop.remove_writer(fd)
            except (ValueError, RuntimeError):
                pass

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

    def _cleanup_sockets(self) -> None:
        """Unregister all sockets from event loop."""
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

        if timeout_ms == 0:
            self._loop.call_soon(self._on_timeout)
        else:
            self._timer_handle = self._loop.call_later(
                timeout_ms / 1000.0, self._on_timeout
            )

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
        # Call socket_action until no more immediate work
        while True:
            status, _running = self._multi.socket_action(sock_fd, event_mask)
            if status != pycurl.E_CALL_MULTI_PERFORM:
                break

        # Drain completed transfers
        self._drain_info_read()

    def _drain_info_read(self) -> None:
        """Process completed and failed transfers from multi handle."""
        while True:
            queued, successful, failed = self._multi.info_read()

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
            self._multi.remove_handle(curl)
        except Exception as e:
            logger.warning("Error removing handle from multi: %s", e)
