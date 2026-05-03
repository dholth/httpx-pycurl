"""
Compare httpx, httpx_pycurl and niquests.
"""

import asyncio
import statistics
import sys
import time

import httpx
import niquests

import httpx_pycurl

URL = "https://httpbingo.org/get"

USER_AGENT = "httpx-pycurl (bench)"
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}


async def get_one(client, url):
    response = await client.get(url)
    assert len(response.content)
    return response.content


async def bench(PARTITIONS, COUNT):
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
        (httpx_session, "httpx"),
        (niquests_session, "niquests"),
        (httpx_pycurl_session, "httpx_pycurl"),
    ]

    print(f"{PARTITIONS} groups of {COUNT} requests each...")

    results_by_client = {name: [] for _, name in clients}
    for runs in range(PARTITIONS):
        for client, name in clients:
            begin = time.perf_counter_ns()
            async with asyncio.TaskGroup():
                # create_task() is same speed as gather()
                # for _ in range(COUNT):
                #     tg.create_task(get_one(client, URL))
                await asyncio.gather(*(get_one(client, URL) for _ in range(COUNT)))
            end = time.perf_counter_ns()
            results_by_client[name].append((end - begin) / 1e9)

    print("\nTime per group:")
    for name, times in results_by_client.items():
        print(f"{name}: {statistics.mean(times):.3f}s ± {statistics.stdev(times):.3f}s")

    # Compare httpx_pycurl vs niquests
    pycurl = results_by_client["httpx_pycurl"]
    niquests_times = results_by_client["niquests"]

    mean_diff = statistics.mean(pycurl) - statistics.mean(niquests_times)
    se_diff = (
        statistics.stdev([p - n for p, n in zip(pycurl, niquests_times)])
        / len(pycurl) ** 0.5
    )
    t_stat = mean_diff / se_diff if se_diff else 0

    print("\nPaired t-test: httpx_pycurl vs niquests")
    print(f"t-stat: {t_stat:.3f} (approx p < 0.05 if |t| > 2.365)")
    print(f"Speedup: {statistics.mean(niquests_times) / statistics.mean(pycurl):.2f}x")


if __name__ == "__main__":
    # niquests seems to shine when batch sizes are large. When batch sizes are
    # smaller, i.e. 128 requests each, vanilla httpx and niquests seem to be
    # closer.

    PARTITIONS = 4
    if len(sys.argv) == 2:
        PARTITIONS = int(sys.argv[1])

    COUNT = 1024 // PARTITIONS

    asyncio.run(bench(PARTITIONS, COUNT))
