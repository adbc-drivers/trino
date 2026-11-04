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

import json
import os
import ssl
import urllib.request

import adbc_driver_manager
import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.statement as statement_tests
import pyarrow
import pytest

from . import trino


def pytest_generate_tests(metafunc) -> None:
    return statement_tests.generate_tests(trino.QUIRKS, metafunc)


class TestStatement(statement_tests.TestStatement):
    def test_rows_affected(self, driver, conn) -> None:
        # Trino can't update rows
        table_name = "test_rows_affected"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            cursor.adbc_statement.execute_update()

            cursor.adbc_statement.set_sql_query(f"CREATE TABLE {table_name} (id INT)")
            rows_affected = cursor.adbc_statement.execute_update()

            if driver.features.statement_rows_affected:
                assert rows_affected == 0
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {table_name} (id) VALUES (1)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            cursor.adbc_statement.execute_update()


def test_query_id_query(driver, conn) -> None:
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        cursor.adbc_statement.set_sql_query("SELECT 1")
        stream, _ = cursor.adbc_statement.execute_query()
        with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
            reader.read_all()

        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id is not None
        assert query_id != ""


def test_query_id_query_bind(driver, conn) -> None:
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        cursor.adbc_statement.set_options(**{"adbc.statement.batch_size": 1})
        cursor.adbc_statement.set_sql_query("SELECT ? + 1")
        cursor.adbc_statement.bind_stream(pyarrow.table({"col1": [1, 2, 3]}))
        stream, _ = cursor.adbc_statement.execute_query()

        # each execution gets its own query ID
        seen = set()
        with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
            for batch in reader:
                seen.add(
                    cursor.adbc_statement.get_option("trino.statement.last_query_id")
                )

        assert len(seen) == 3
        for query_id in seen:
            assert query_id is not None
            assert query_id != ""


def test_query_id_update(driver, conn) -> None:
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        cursor.adbc_statement.set_sql_query("DROP TABLE IF EXISTS foobar")
        cursor.adbc_statement.execute_update()

        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id is not None
        assert query_id != ""


def test_query_id_update_bind(driver, conn) -> None:
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        cursor.adbc_statement.set_sql_query("SELECT ? + 1")
        cursor.adbc_statement.bind_stream(pyarrow.table({"col1": [1, 2, 3]}))
        cursor.adbc_statement.execute_update()
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id is not None
        assert query_id != ""


def test_query_id_ingest(driver, conn) -> None:
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        rb = pyarrow.record_batch({"col1": range(4096)})
        table = pyarrow.Table.from_batches([rb] * 16)
        cursor.adbc_ingest("foobar", table, mode="replace")

        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id is not None
        assert query_id != ""


def test_query_id_ingest_batch_size(driver, conn) -> None:
    # driver takes a different route here
    with conn.cursor() as cursor:
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id == ""

        cursor.adbc_statement.set_options(
            **{"adbc.statement.ingest.batch_size": "2000"}
        )

        rb = pyarrow.record_batch({"col1": range(4096)})
        table = pyarrow.Table.from_batches([rb] * 16)
        cursor.adbc_ingest("foobar", table, mode="replace")

        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
        assert query_id is not None
        assert query_id != ""


def get_last_query_info(cursor) -> dict:
    """Get the last query's info from the Trino REST API, using the same
    server, transport and certificates as the validation connection."""
    host = os.environ.get("TRINO_HOST")
    port = os.environ.get("TRINO_PORT")
    scheme = os.environ.get("TRINO_SSL_MODE", "").lower()
    if not host or not port or scheme not in ("http", "https"):
        pytest.skip("TRINO_HOST, TRINO_PORT and TRINO_SSL_MODE must be set")
    context = None
    if scheme == "https":
        context = ssl.create_default_context(
            cafile=os.environ.get("TRINO_SSL_CERT_PATH") or None
        )
        client_cert = os.environ.get("TRINO_SSL_CLIENT_CERT_PATH")
        if client_cert:
            context.load_cert_chain(
                client_cert, os.environ.get("TRINO_SSL_CLIENT_KEY_PATH") or None
            )

    query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")
    request = urllib.request.Request(
        f"{scheme}://{host}:{port}/v1/query/{query_id}",
        headers={"X-Trino-User": os.environ.get("TRINO_USERNAME", "test")},
    )
    with urllib.request.urlopen(request, context=context) as response:
        return json.load(response)


def test_statement_client_tags_header(driver_path: str, db_kwargs: dict) -> None:
    uri = db_kwargs["uri"]
    uri += ("&" if "?" in uri else "?") + "clientTags=conn,shared"
    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={**db_kwargs, "uri": uri},
            autocommit=True,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        cursor.fetchall()
        info = get_last_query_info(cursor)
        assert sorted(info["session"]["clientTags"]) == ["conn", "shared"]

        # The header replaces the connection client tags
        cursor.adbc_statement.set_options(
            **{"trino.statement.http_header.X-Trino-Client-Tags": "raw"}
        )
        cursor.execute("SELECT 1")
        cursor.fetchall()
        info = get_last_query_info(cursor)
        assert info["session"]["clientTags"] == ["raw"]


def test_statement_http_header(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(
            **{"trino.statement.http_header.X-Trino-Source": "adbc-validation"}
        )
        cursor.execute("SELECT 1")
        cursor.fetchall()
        query_id = cursor.adbc_statement.get_option("trino.statement.last_query_id")

        cursor.adbc_statement.set_options(
            **{"trino.statement.http_header.X-Trino-Source": ""}
        )
        cursor.execute(
            f"SELECT source FROM system.runtime.queries WHERE query_id = '{query_id}'"
        )
        assert cursor.fetchone() == ("adbc-validation",)


def test_statement_reserved_http_header(driver, conn) -> None:
    with (
        conn.cursor() as cursor,
        pytest.raises(
            adbc_driver_manager.ProgrammingError, match="managed by the driver"
        ),
    ):
        cursor.adbc_statement.set_options(
            **{"trino.statement.http_header.X-Trino-Catalog": "other"}
        )
