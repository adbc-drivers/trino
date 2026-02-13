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

import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class TrinoQuirks(model.DriverQuirks):
    name = "trino"
    driver = "adbc_driver_trino"
    driver_name = "ADBC Driver Foundry Driver for Trino"
    vendor_name = "Trino"
    vendor_version = re.compile(r"Trino [0-9]+")
    short_version = "4nn"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
        statement_prepare=True,
        statement_rows_affected=True,
        current_catalog="memory",
        current_schema="default",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("TRINO_DSN"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        return "?"

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        # Check if the error indicates a table not found condition
        error_str = str(error).lower()
        return (
            "table" in error_str
            and (
                "does not exist" in error_str
                or "doesn't exist" in error_str
                or "not found" in error_str
            )
            and table_name.lower() in error_str
        )

    def quote_one_identifier(self, identifier: str) -> str:
        identifier = identifier.replace('"', '""')
        return f'"{identifier}"'

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect=self.name)


QUIRKS = [TrinoQuirks()]
