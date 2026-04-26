# httpx-pycurl

`httpx-pycurl` provides an `httpx` transport that executes requests with
`pycurl`.  It combines the [goodness of curl](https://everything.curl.dev/) with
the familiar `httpx` API, including support for `http/2` and even non-http
protocols built into `curl`. `AsyncPyCurlTransport` performs better than
`httpx`'s default `AsyncHttpTransport` with `http2=True`, taking about 78% of
the time to issue 128 requests in parallel. Under heavier usage `httpx-pycurl`
appears to pull further ahead of alternative libraries, without replacing all of
`httpx`; just `httpx`'s transport.

## Usage

`AsyncPyCurlTransport` is the focus of this package. It uses the curl
[`multi_socket`](https://everything.curl.dev/transfers/drive/multi-socket.html)
interface to integrate `curl` with the `asyncio` event loop.

```python
import httpx
from httpx_pycurl import AsyncPyCurlTransport

transport = AsyncPyCurlTransport(timeout=10.0)

async with httpx.AsyncClient(transport=transport) as client:
    responses = await asyncio.gather(*(client.get(url) for url in urls))
```

`AsyncPyCurlTransport` delegates SSL/TLS handling to `curl` and does not use
Python's builtin `ssl` module. By default it calls
[`certifi.where()`](https://github.com/certifi/python-certifi) to set root
certificates. It is also possible to pass a custom path to the root certificates
with `AsyncPyCurlTransport(cainfo=ca_cert_path)`.

`PyCurlTransport` is not the focus of this project and is less likely to work.
In the future it may delegate to `AsyncPyCurlTransport`.

```python
import httpx
from httpx_pycurl import PyCurlTransport

transport = PyCurlTransport(timeout=10.0)

with httpx.Client(transport=transport) as client:
    response = client.get("https://example.com")
    print(response.status_code)
    print(response.text)

debug_transport = PyCurlTransport(
    verbose=True,
    debug_callback=lambda info_type, data: print(info_type, data),
)
```

## Installation

```bash
pip install httpx-pycurl
```

Or with conda,

```bash
conda install -n base conda-pypi
conda pypi install httpx-pycurl
```

## Performance

`httpx-pycurl` is in early development but it passes most `httpx` tests and has
good performance. Our `tests/bench.py` uses `asyncio.gather()` to make many
`http/2` requests to `https://httpbingo.org/get` using `httpx`, `niquests`, and
`httpx` with `httpx-pycurl`'s transport. `httpx-pycurl` is the fastest library
tested.

Running `tests/bench.py [N]` shows that the more efficient `http/2` libraries
shine when performing large numbers of parallel requests, and are closer
together when only groups of 128 parallel requests are made.

```
2 groups of 512 requests each...

Time per group:
httpx: 0.679s ± 0.119s
niquests: 0.432s ± 0.140s
httpx_pycurl: 0.299s ± 0.056s

Paired t-test: httpx_pycurl vs niquests
t-stat: -2.249 (approx p < 0.05 if |t| > 2.365)
Speedup: 1.44x
```

```
4 groups of 256 requests each...

Time per group:
httpx: 0.378s ± 0.077s
niquests: 0.259s ± 0.064s
httpx_pycurl: 0.190s ± 0.034s

Paired t-test: httpx_pycurl vs niquests
t-stat: -4.208 (approx p < 0.05 if |t| > 2.365)
Speedup: 1.36x
```

```
8 groups of 128 requests each...

Time per group:
httpx: 0.215s ± 0.073s
niquests: 0.205s ± 0.038s
httpx_pycurl: 0.162s ± 0.033s

Paired t-test: httpx_pycurl vs niquests
t-stat: -8.949 (approx p < 0.05 if |t| > 2.365)
Speedup: 1.27x
```