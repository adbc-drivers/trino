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

import pytest


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
    return os.environ.get("TRINO_HOST", "localhost")


@pytest.fixture(scope="session")
def trino_port() -> str:
    """Trino port. Example: TRINO_PORT=8080"""
    return os.environ.get("TRINO_PORT", "8080")


@pytest.fixture(scope="session")
def trino_catalog() -> str:
    """Trino catalog name. Example: TRINO_CATALOG=memory"""
    return os.environ.get("TRINO_CATALOG", "memory")


@pytest.fixture(scope="session")
def trino_schema() -> str:
    """Trino schema name. Example: TRINO_SCHEMA=default"""
    return os.environ.get("TRINO_SCHEMA", "default")


@pytest.fixture(scope="session")
def creds() -> tuple[str, str]:
    """Trino credentials. Example: TRINO_USERNAME=test TRINO_PASSWORD=password"""
    username = os.environ.get("TRINO_USERNAME", "test")
    password = os.environ.get("TRINO_PASSWORD", "password")
    return username, password


@pytest.fixture(scope="session")
def uri(trino_host: str, trino_port: str, trino_catalog: str, trino_schema: str) -> str:
    """
    Constructs a clean Trino URI without credentials. SSL=false required for local Docker testing.
    Example: trino://localhost:8080/memory/default
    """
    return f"trino://{trino_host}:{trino_port}/{trino_catalog}/{trino_schema}?SSL=false"


@pytest.fixture(scope="session")
def dsn(
    creds: tuple[str, str],
    trino_host: str,
    trino_port: str,
    trino_catalog: str,
    trino_schema: str,
) -> str:
    """
    Constructs a Trino DSN in Go Trino Driver's native format. SSL=false required for local Docker testing.
    Example: http://test:password@localhost:8080?catalog=memory&schema=default
    """
    username, password = creds
    return f"http://{username}:{password}@{trino_host}:{trino_port}?catalog={trino_catalog}&schema={trino_schema}&SSL=false"
