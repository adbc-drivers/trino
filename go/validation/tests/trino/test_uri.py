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


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_userpass_uri(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,  # trino://localhost:8080/memory/default
    creds: tuple[str, str],
) -> None:
    """Test authentication with credentials embedded in URI."""
    username, password = creds

    parsed = urllib.parse.urlparse(uri)
    query_params = urllib.parse.parse_qs(parsed.query)
    query_params["session_properties"] = ["task_concurrency=2"]

    new_query = urllib.parse.urlencode(query_params, doseq=True)
    netloc = f"{username}:{password}@{parsed.netloc}"

    auth_uri = urllib.parse.urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": auth_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_catalog, current_schema")
            catalog, schema = cursor.fetchone()
            assert catalog == "memory"
            assert schema == "default"

            cursor.execute("SHOW SESSION LIKE 'task_concurrency'")
            row = cursor.fetchone()
            value = row[1]
            assert value == "2"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_userpass_options(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,
    creds: tuple[str, str],
) -> None:
    """Test authentication with credentials in connection options."""
    username, password = creds
    params = {
        "uri": uri,
        "username": username,
        "password": password,
    }
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs=params,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")


@pytest.mark.feature(group="Configuration", name="Connect with URI")
@pytest.mark.parametrize(
    "ssl_param, expect_https",
    [
        # pytest.param("SSL=true", True, id="SSL=true"), # Cannot test SSL=true with server in Docker
        pytest.param("SSL=false", False, id="SSL=false"),
    ],
)
def test_ssl_modes(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,  # trino://localhost:8080/memory/default
    creds: tuple[str, str],
    ssl_param: str,
    expect_https: bool,
) -> None:
    """Test SSL configurations with dynamic URI construction."""
    username, password = creds

    parsed = urllib.parse.urlparse(uri)
    netloc = f"{username}:{password}@{parsed.netloc}"

    query = f"{parsed.query}&{ssl_param}" if parsed.query else ssl_param
    ssl_uri = urllib.parse.urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, query, parsed.fragment)
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": ssl_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_uri_catalog_schema_parsing(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    creds: tuple[str, str],
) -> None:
    """Tests that catalog and schema are correctly parsed from URI path."""
    username, password = creds

    full_uri = f"trino://{username}:{password}@{trino_host}:{trino_port}/memory/test_schema?SSL=false"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": full_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_catalog, current_schema")
            result = cursor.fetchone()
            assert result[0] == "memory"
            assert result[1] == "test_schema"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_uri_catalog_only(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    creds: tuple[str, str],
) -> None:
    """Tests URI with catalog but no schema."""
    username, password = creds

    catalog_only_uri = (
        f"trino://{username}:{password}@{trino_host}:{trino_port}/memory?SSL=false"
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": catalog_only_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_catalog")
            result = cursor.fetchone()
            assert result[0] == "memory"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_ipv6_host_support(
    driver: model.DriverQuirks,
    driver_path: str,
    creds: tuple[str, str],
    trino_catalog: str,
    trino_schema: str,
) -> None:
    """Tests that IPv6 addresses are correctly handled in URIs."""
    username, password = creds

    ipv6_uri = f"trino://{username}:{password}@[::1]:8080/{trino_catalog}/{trino_schema}?SSL=false"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": ipv6_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_url_encoded_catalog_schema(
    driver: model.DriverQuirks,
    driver_path: str,
    trino_host: str,
    trino_port: str,
    creds: tuple[str, str],
) -> None:
    """Tests that URL-encoded catalog and schema names work correctly."""
    username, password = creds

    encoded_uri = f"trino://{username}:{password}@{trino_host}:{trino_port}/my%20catalog/my%20schema?SSL=false"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": encoded_uri},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_catalog, current_schema")
            result = cursor.fetchone()
            assert result[0] == "my catalog"
            assert result[1] == "my schema"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_missing_uri_raises_error(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that connecting without a 'uri' option raises an error."""
    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="missing required option uri",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={},
        ):
            pass


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_invalid_uri_format(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that a malformed URI raises a helpful error."""
    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="invalid Trino URI format",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": "trino://[invalid-format"},
        ):
            pass


# --- DSN tests ---


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_basic_dsn_connection(
    driver: model.DriverQuirks,
    driver_path: str,
    dsn: str,  # Example: http://test:password@localhost:8080?catalog=memory&schema=default
) -> None:
    """
    Test basic connection using DSN format, adding extra parameters
    to ensure all query args are preserved.
    """

    parsed = urllib.parse.urlparse(dsn)

    query_params = urllib.parse.parse_qs(parsed.query)
    query_params["session_properties"] = ["task_concurrency=2"]

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

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": modified_dsn},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW SESSION LIKE 'task_concurrency'")
            row = cursor.fetchone()
            assert row is not None, (
                "Expected session property 'task_concurrency' to be set"
            )
            assert row[0] == "task_concurrency"
            assert row[1] == "2"

            cursor.execute("SELECT current_catalog, current_schema")
            catalog, schema = cursor.fetchone()
            assert catalog == "memory", f"Expected catalog=memory, got {catalog}"
            assert schema == "default", f"Expected schema=default, got {schema}"


@pytest.mark.feature(group="Configuration", name="Connect with URI")
def test_plain_host_with_creds_options(
    driver: model.DriverQuirks,
    driver_path: str,
    creds: tuple[str, str],
) -> None:
    """
    Tests that a plain host string
    is correctly combined with credentials from options.
    """
    username, password = creds

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={
            "uri": "localhost:8080?SSL=false",
            "username": username,
            "password": password,
        },
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
