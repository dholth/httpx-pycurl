# httpx-pycurl

`httpx-pycurl` provides an `httpx` transport that executes requests with
`pycurl`.  It combines the [goodness of curl](https://everything.curl.dev/) with
the familiar `httpx` API, including support for `http/2` and even non-http
protocols built into `curl`. On my machine, `AsyncPyCurlTransport` performs
better than `httpx`'s default `AsyncHttpTransport`, taking approximately 75% of
the time to fetch 60 files from a local `nginx` test server.

`httpx-pycurl` is in early development, but it passes most `httpx` tests and has
good performance. A `niquests`-derived test uses `asyncio.gather()` to make 1000
http/2 requests to `https://httpbingo.org/get`. `httpx-pycurl` is about as fast.

```
# First run:
Fetch 1000x https://httpbingo.org/get
aiohttp: 1.029s
httpx: 1.369s
httpx_pycurl: 0.637s
niquests: 0.715s

# Second run:
Fetch 1000x https://httpbingo.org/get
aiohttp: 0.927s
httpx: 1.346s
httpx_pycurl: 0.677s
niquests: 0.655s
```

## Install

```bash
pip install httpx-pycurl
```

Or with conda,

```bash
conda install -n base conda-pypi
conda pypi install httpx-pycurl
```

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
