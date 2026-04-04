# Proof of concept not intended for use.

# pycurltx

`pycurltx` provides an `httpx` transport that executes requests with `pycurl`.

This is a proof of concept and is not meant to be used.

## Install

```bash
pip install pycurltx
```

## Usage

```python
import httpx
from pycurltx import PyCurlTransport

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

For higher concurrency in threaded use, use `PyCurlMultiTransport`:

```python
import concurrent.futures
import httpx
from pycurltx import PyCurlMultiTransport

transport = PyCurlMultiTransport(max_connections=100)

with httpx.Client(transport=transport) as client:
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(client.get, "https://example.com") for _ in range(20)]
        responses = [f.result() for f in futures]
```

You can also use the async client via `PyCurlAsyncTransport`:

```python
import httpx
from pycurltx import PyCurlAsyncTransport

transport = PyCurlAsyncTransport(timeout=10.0)

async with httpx.AsyncClient(transport=transport) as client:
    response = await client.get("https://example.com")
```

For event-loop integrated async concurrency, use `PyCurlAsyncMultiSocketTransport`:

```python
import asyncio
import httpx
from pycurltx import PyCurlAsyncMultiSocketTransport

transport = PyCurlAsyncMultiSocketTransport(max_connections=100)

async with httpx.AsyncClient(transport=transport) as client:
    responses = await asyncio.gather(
        client.get("https://example.com/one"),
        client.get("https://example.com/two"),
    )
```

## Notes

- This transport is implemented on top of pycurl's easy interface.
- Request bodies can be streamed from iterable byte chunks.
- Response bodies are streamed from a spooled temporary file.
- The async transport performs curl operations in a worker thread.
- `PyCurlAsyncMultiSocketTransport` integrates curl sockets with the asyncio event loop.
- Verbose curl logging is supported via `verbose=True` and optional `debug_callback`.

## TODO

- curl supports proxies
- curl supports native certificate store on some builds
- configure curl to use cacerts store
- ...