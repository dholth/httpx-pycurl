import socket
from pathlib import Path

import pytest
import trustme
import ssl
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
                print("Accepted")
                return True
            except ConnectionRefusedError:
                print("Refused")
                return False

    xprocess.ensure("nginx", Starter)

    yield "https://localhost:8443/"

    xprocess.getinfo("nginx").terminate()
