# 0.0.5

- Implement true streaming responses
- Simple benchmark script
- Add own benchmark script results to README
- Refactor, move sync transport into own module

# 0.0.4

- Refactor into curl-only `AsyncCurl()` to manage transfers, independent
  from httpx or any Python http library; and httpx-curl translation layer.
- Support true streaming responses in `AsyncCurl()`.
- Improve sync handling; run httpx tests against our sync transport.

# 0.0.3

- Simplify callback handling
- Simplify status line parser
- Implement ConnectionTimeout

# 0.0.2

- Run tests from `httpx` patched to use our transport
- Passing `httpx` tests
- Improve own tests
- Support streaming responses
- Support request bodies

# 0.0.1

- Early release
