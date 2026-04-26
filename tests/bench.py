"""
Compare httpx, httpx_pycurl and niquests.
"""

import asyncio
import random
import time

import httpx
import niquests

import httpx_pycurl

URL = "https://httpbingo.org/get"

USER_AGENT = "httpx-pycurl (bench)"
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
COUNT = 1024


async def bench():
    """
    Make COUNT requests using each of several clients. Print time taken by each.
    """

    niquests_session = niquests.AsyncSession()
    niquests_session.headers.update(DEFAULT_HEADERS)

    httpx_session = httpx.AsyncClient(http2=True)
    httpx_session.headers.update(DEFAULT_HEADERS)

    # XXX AsyncPyCurlTransport sets `curl.setopt(pycurl.USERAGENT, user_agent)`
    # but a regular header has the same effect.
    httpx_pycurl_session = httpx.AsyncClient(
        transport=httpx_pycurl.AsyncPyCurlTransport(user_agent=USER_AGENT)
    )
    httpx_pycurl_session.headers.update(DEFAULT_HEADERS)

    clients = [
        (niquests_session, "niquests"),
        (httpx_session, "httpx"),
        (httpx_pycurl_session, "httpx_pycurl"),
    ]
    random.shuffle(clients)

    # XXX run several times with COUNT / n requests each, measure stddev
    results = {}

    for client, name in clients:
        begin = time.perf_counter_ns()
        async with client as client:
            await asyncio.gather(*(client.get(URL) for _ in range(COUNT)))
        end = time.perf_counter_ns()
        results[name] = end - begin

    for name, duration in sorted(results.items(), key=lambda item: item[1]):
        print(f"{duration / 1e9:0.03f} {name}")


if __name__ == "__main__":
    asyncio.run(bench())
