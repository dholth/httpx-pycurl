from __future__ import annotations

import logging
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING

import certifi
import httpx
import pycurl

from .transport import (
    _configure_curl,
    _finalize_transfer,
    _map_pycurl_error,
    _RequestBodyReader,
    _SpooledFileStream,
    _TransferContext,
)

if TYPE_CHECKING:
    from typing import Callable


class PyCurlTransport(httpx.BaseTransport):
    def __init__(
        self,
        timeout: float | None = None,
        verify: bool = True,
        follow_redirects: bool = False,
        user_agent: str | None = None,
        verbose: bool = False,
        debug_callback: Callable[[int, bytes], None] | None = None,
        debug_logger: logging.Logger | None = None,
        cainfo: str | None = None,
    ):
        self._timeout = timeout
        self._verify = verify
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._verbose = verbose
        self._debug_callback = debug_callback
        self._debug_logger = debug_logger
        self._cainfo = cainfo or certifi.where()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        context = self._perform_request(request)
        return httpx.Response(
            status_code=context.status_code,
            headers=context.response_headers,
            stream=_SpooledFileStream(context.response_body),
            request=request,
            extensions={"http_version": context.http_version},
        )

    def close(self) -> None:
        return

    def _perform_request(self, request: httpx.Request) -> _TransferContext:
        context = _TransferContext(
            response_body=SpooledTemporaryFile(max_size=1024 * 1024)
        )
        curl = pycurl.Curl()
        try:
            body_reader = _RequestBodyReader(iter(request.stream))  # type: ignore[arg-type]
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
                body_reader=body_reader,
            )
            curl.perform()
            _finalize_transfer(curl, context)
            return context
        except pycurl.error as error:
            context.response_body.close()
            code, message = error.args
            raise _map_pycurl_error(code, str(message), curl) from error
        finally:
            curl.close()
