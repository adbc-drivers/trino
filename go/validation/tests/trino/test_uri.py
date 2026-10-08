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

import urllib.parse

import adbc_driver_manager.dbapi
import pytest
from adbc_drivers_validation import model


def test_username_uri(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,  # trino://localhost:8080/memory/default
    trino_username: str,
) -> None:
    """Test authentication with username embedded in URI."""

    parsed = urllib.parse.urlparse(uri)
    query_params = urllib.parse.parse_qs(parsed.query)
    query_params["session_properties"] = ["task_concurrency:2"]

    new_query = urllib.parse.urlencode(query_params, doseq=True)
    netloc = f"{trino_username}@{parsed.netloc}"

    auth_uri = urllib.parse.urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": auth_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_catalog, current_schema")
        catalog, schema = cursor.fetchone()
        assert catalog == "memory"
        assert schema == "default"

        cursor.execute("SHOW SESSION LIKE 'task_concurrency'")
        row = cursor.fetchone()
        value = row[1]
        assert value == "2"


def test_username_options(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,
    trino_username: str,
) -> None:
    """Test authentication with username in connection options."""
    params = {
        "uri": uri,
        "username": trino_username,
    }
    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs=params,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")


@pytest.mark.parametrize("ssl_mode", ["trusted_ca", "skip_verification", "plain_http"])
def test_ssl_modes(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_catalog: str,
    trino_schema: str,
    trino_username: str,
    trino_http_port: str,
    trino_https_port: str,
    trino_ssl_cert_path: str,
    ssl_mode: str,
) -> None:
    """Test trusted HTTPS, HTTPS with disabled verification, and plain HTTP."""
    port = trino_https_port
    query_params: list[tuple[str, str]] = []

    if ssl_mode == "trusted_ca":
        query_params.extend(
            [
                ("SSL", "true"),
                ("SSLCertPath", trino_ssl_cert_path),
            ]
        )
    elif ssl_mode == "skip_verification":
        query_params.extend(
            [
                ("SSL", "true"),
                ("SSLVerification", "NONE"),
            ]
        )
    elif ssl_mode == "plain_http":
        port = trino_http_port
        query_params.append(("SSL", "false"))
    else:
        raise AssertionError(f"unexpected ssl_mode {ssl_mode}")

    ssl_uri = urllib.parse.urlunparse(
        (
            "trino",
            f"{trino_username}@{trino_host}:{port}",
            f"/{trino_catalog}/{trino_schema}",
            "",
            urllib.parse.urlencode(query_params),
            "",
        )
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": ssl_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_uri_catalog_schema_parsing(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    trino_username: str,
    trino_uri_query: str,
) -> None:
    """Tests that catalog and schema are correctly parsed from URI path."""

    full_uri = (
        f"trino://{trino_username}@{trino_host}:{trino_port}"
        f"/memory/test_schema?{trino_uri_query}"
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": full_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_catalog, current_schema")
        result = cursor.fetchone()
        assert result[0] == "memory"
        assert result[1] == "test_schema"


def test_uri_catalog_only(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    trino_username: str,
    trino_uri_query: str,
) -> None:
    """Tests URI with catalog but no schema."""

    catalog_only_uri = (
        f"trino://{trino_username}@{trino_host}:{trino_port}/memory?{trino_uri_query}"
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": catalog_only_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_catalog")
        result = cursor.fetchone()
        assert result[0] == "memory"


def test_ipv6_host_support(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_username: str,
    trino_port: str,
    trino_catalog: str,
    trino_schema: str,
    trino_uri_query: str,
    trino_ssl_mode: str,
) -> None:
    """Tests that IPv6 addresses are correctly handled in URIs."""
    if trino_ssl_mode == "https":
        pytest.skip("local HTTPS cert covers localhost/127.0.0.1, not ::1")

    ipv6_uri = (
        f"trino://{trino_username}@[::1]:{trino_port}/{trino_catalog}/{trino_schema}"
        f"?{trino_uri_query}"
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": ipv6_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1


def test_url_encoded_catalog_schema(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    trino_username: str,
    trino_uri_query: str,
) -> None:
    """Tests that URL-encoded catalog and schema names work correctly."""

    encoded_uri = (
        f"trino://{trino_username}@{trino_host}:{trino_port}"
        f"/my%20catalog/my%20schema?{trino_uri_query}"
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": encoded_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_catalog, current_schema")
        result = cursor.fetchone()
        assert result[0] == "my catalog"
        assert result[1] == "my schema"


def test_missing_uri_raises_error(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that connecting without a 'uri' option raises an error."""
    with (
        pytest.raises(
            adbc_driver_manager.dbapi.ProgrammingError,
            match="missing required option uri",
        ),
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={},
        ),
    ):
        pass


def test_invalid_uri_format(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that a malformed URI raises a helpful error."""
    with (
        pytest.raises(
            adbc_driver_manager.dbapi.ProgrammingError,
            match="invalid Trino URI format",
        ),
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": "trino://[invalid-format"},
        ),
    ):
        pass


# --- DSN tests ---


def test_basic_dsn_connection(
    driver: model.DriverQuirks,
    driver_path: str,
    dsn: str,  # Example: http://test@localhost:8080?catalog=memory&schema=default
) -> None:
    """
    Test basic connection using DSN format, adding extra parameters
    to ensure all query args are preserved.
    """

    parsed = urllib.parse.urlparse(dsn)

    query_params = urllib.parse.parse_qs(parsed.query)
    query_params["session_properties"] = ["task_concurrency:2"]

    new_query = urllib.parse.urlencode(query_params, doseq=True)

    modified_dsn = urllib.parse.urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": modified_dsn},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SHOW SESSION LIKE 'task_concurrency'")
        row = cursor.fetchone()
        assert row is not None, "Expected session property 'task_concurrency' to be set"
        assert row[0] == "task_concurrency"
        assert row[1] == "2"

        cursor.execute("SELECT current_catalog, current_schema")
        catalog, schema = cursor.fetchone()
        assert catalog == "memory", f"Expected catalog=memory, got {catalog}"
        assert schema == "default", f"Expected schema=default, got {schema}"


def test_plain_host_with_username_options(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_username: str,
    trino_host: str,
    trino_port: str,
    trino_ssl_mode: str,
    trino_ssl_cert_path: str,
) -> None:
    """
    Tests that a plain host string
    is correctly combined with credentials from options.
    """
    query_params: list[tuple[str, str]] = []
    if trino_ssl_mode == "https":
        query_params.append(("SSL", "true"))
        query_params.append(("SSLCertPath", trino_ssl_cert_path))
    else:
        query_params.append(("SSL", "false"))

    query = urllib.parse.urlencode(query_params)

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={
                "uri": f"{trino_host}:{trino_port}?{query}",
                "username": trino_username,
            },
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
