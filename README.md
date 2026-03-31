# pycurltx

`pycurltx` provides an `httpx` transport that executes requests with `pycurl`.

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

## Notes

- This transport is implemented on top of pycurl's easy interface.
- Request bodies can be streamed from iterable byte chunks.
- Response bodies are streamed from a spooled temporary file.
- The async transport performs curl operations in a worker thread.
