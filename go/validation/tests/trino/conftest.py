# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import urllib.parse

import pytest


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} must be set for Trino URI validation tests")
    return value


def pytest_generate_tests(metafunc) -> None:
    metafunc.parametrize(
        "driver",
        [pytest.param("trino:", id="trino")],
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture(scope="session")
def trino_host() -> str:
    """Trino host. Example: TRINO_HOST=localhost"""
    return require_env("TRINO_HOST")


@pytest.fixture(scope="session")
def trino_port() -> str:
    """Trino port. Example: TRINO_PORT=8080"""
    return require_env("TRINO_PORT")


@pytest.fixture(scope="session")
def trino_http_port(trino_port: str, trino_ssl_mode: str) -> str:
    """Plain HTTP port exposed by the local Trino Docker setup."""
    if trino_ssl_mode == "http":
        return trino_port
    return os.environ.get("TRINO_HTTP_PORT", "8080")


@pytest.fixture(scope="session")
def trino_https_port(trino_port: str, trino_ssl_mode: str) -> str:
    """HTTPS port exposed by the local Trino Docker setup."""
    if trino_ssl_mode == "https":
        return trino_port
    return os.environ.get("TRINO_HTTPS_PORT", "8443")


@pytest.fixture(scope="session")
def trino_catalog() -> str:
    """Trino catalog name. Example: TRINO_CATALOG=memory"""
    return require_env("TRINO_CATALOG")


@pytest.fixture(scope="session")
def trino_schema() -> str:
    """Trino schema name. Example: TRINO_SCHEMA=default"""
    return require_env("TRINO_SCHEMA")


@pytest.fixture(scope="session")
def trino_ssl_mode() -> str:
    """Trino transport mode. Example: TRINO_SSL_MODE=https"""
    return require_env("TRINO_SSL_MODE").lower()


@pytest.fixture(scope="session")
def trino_ssl_cert_path() -> str:
    """CA cert path for self-signed HTTPS. Example: TRINO_SSL_CERT_PATH=/path/to/ca.crt"""
    return require_env("TRINO_SSL_CERT_PATH")


@pytest.fixture(scope="session")
def trino_client_cert_params() -> dict[str, str]:
    cert_path = os.environ.get("TRINO_SSL_CLIENT_CERT_PATH", "")
    key_path = os.environ.get("TRINO_SSL_CLIENT_KEY_PATH", "")
    if bool(cert_path) != bool(key_path):
        pytest.fail(
            "TRINO_SSL_CLIENT_CERT_PATH and TRINO_SSL_CLIENT_KEY_PATH must be set together"
        )
    if not cert_path:
        return {}
    return {"SSLClientCertPath": cert_path, "SSLClientKeyPath": key_path}


@pytest.fixture(scope="session")
def trino_username() -> str:
    """Trino username. Example: TRINO_USERNAME=test"""
    return require_env("TRINO_USERNAME")


@pytest.fixture(scope="session")
def trino_uri_query(
    trino_ssl_mode: str,
    trino_ssl_cert_path: str,
    trino_client_cert_params: dict[str, str],
) -> str:
    """Connection options appended to `trino://` URIs."""
    query_params: list[tuple[str, str]] = []
    if trino_ssl_mode == "https":
        query_params.append(("SSL", "true"))
        query_params.append(("SSLCertPath", trino_ssl_cert_path))
        query_params.extend(trino_client_cert_params.items())
    else:
        query_params.append(("SSL", "false"))

    return urllib.parse.urlencode(query_params)


@pytest.fixture(scope="session")
def trino_http_scheme(trino_ssl_mode: str) -> str:
    return "https" if trino_ssl_mode == "https" else "http"


@pytest.fixture(scope="session")
def uri(
    trino_host: str,
    trino_port: str,
    trino_catalog: str,
    trino_schema: str,
    trino_uri_query: str,
) -> str:
    """
    Constructs a clean Trino URI without credentials.
    Example: trino://localhost:8080/memory/default?SSL=false
    """
    return f"trino://{trino_host}:{trino_port}/{trino_catalog}/{trino_schema}?{trino_uri_query}"


@pytest.fixture(scope="session")
def dsn(
    trino_username: str,
    trino_host: str,
    trino_port: str,
    trino_catalog: str,
    trino_schema: str,
    trino_http_scheme: str,
    trino_ssl_cert_path: str,
    trino_client_cert_params: dict[str, str],
) -> str:
    """
    Constructs a Trino DSN in Go Trino Driver's native format.
    Example: http://test@localhost:8080?catalog=memory&schema=default
    """
    query_params: list[tuple[str, str]] = [
        ("catalog", trino_catalog),
        ("schema", trino_schema),
    ]
    if trino_http_scheme == "http":
        query_params.append(("SSL", "false"))
    else:
        query_params.append(("SSLCertPath", trino_ssl_cert_path))
        query_params.extend(trino_client_cert_params.items())

    query = urllib.parse.urlencode(query_params)
    return f"{trino_http_scheme}://{trino_username}@{trino_host}:{trino_port}?{query}"
