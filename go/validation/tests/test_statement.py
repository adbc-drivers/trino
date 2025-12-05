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

import adbc_drivers_validation.tests.statement as statement_tests

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
