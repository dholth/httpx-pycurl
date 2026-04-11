import socket
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import trustme
from xprocess import ProcessStarter

HERE = Path(__file__).parent


@pytest.fixture(scope="session")
def certs():
    # Look, you just created your certificate authority!
    ca = trustme.CA()

    # And now you issued a cert signed by this fake CA
    # https://en.wikipedia.org/wiki/Example.org
    server_cert = ca.issue_cert("localhost")

    ssl_context = ssl.create_default_context()
    ca.configure_trust(ssl_context)

    # Put the PEM-encoded data in a temporary file, for libraries that
    # insist on that:
    with (
        ca.cert_pem.tempfile() as ca_temp_path,
        server_cert.private_key_and_cert_chain_pem.tempfile() as server_cert_path,
    ):
        yield ca_temp_path, server_cert_path, ssl_context


@pytest.fixture(scope="session")
def server_cert(certs):
    return certs[1]


@pytest.fixture(scope="session")
def ca_cert(certs):
    return certs[0]


@pytest.fixture(scope="session")
def ssl_context(certs):
    return certs[2]


@pytest.fixture(scope="session")
def server(tmp_path_factory, server_cert, xprocess):
    """
    Start test http/2 web server with certs, return URL.
    """
    tmp_path = tmp_path_factory.mktemp("web")

    data_path = tmp_path / "data"
    data_path.mkdir()

    (data_path / "small").write_bytes(b"0" * 1024)
    (data_path / "medium").write_bytes(b"0" * (16 * 1024))
    (data_path / "large").write_bytes(b"0" * (1024 * 1024))

    assert Path(server_cert).exists()

    conf = (
        (HERE / "nginx" / "nginx.conf")
        .read_text()
        .replace("localhost.pem", str(server_cert))
        .replace("TEST_DATA", str(data_path))
    )
    edited = tmp_path / "nginx.conf"
    edited.write_text(conf)

    class Starter(ProcessStarter):
        args = ["nginx", "-c", str(edited.absolute())]

        def startup_check(self):
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            try:
                sock.connect(("localhost", 8443))
                return True
            except ConnectionRefusedError:
                return False

    xprocess.ensure("nginx", Starter)

    yield "https://localhost:8443/"

    xprocess.getinfo("nginx").terminate()


class SlowHandler(BaseHTTPRequestHandler):
    """HTTP handler that delays response."""

    def do_GET(self):
        try:
            if self.path == "/delay":
                # Sleep for 60 seconds before responding
                time.sleep(60)
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Delayed response after 60 seconds\n")
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"OK\n")
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected, ignore

    def log_message(self, format, *args):
        pass  # Suppress logging


@pytest.fixture(scope="session")
def slow_server():
    """Start a simple HTTP server that delays responses."""
    server = HTTPServer(("127.0.0.1", 0), SlowHandler)
    host, port = server.server_address

    def run_server():
        server.serve_forever()

    thread = threading.Thread(target=run_server, daemon=False)
    thread.start()

    yield f"http://{host}:{port}/"

    # Shutdown gracefully with a timeout to wait for in-flight requests
    server.shutdown()
    thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish
