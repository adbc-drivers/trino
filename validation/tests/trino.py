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

from pathlib import Path

from adbc_drivers_validation import model


class TrinoQuirks(model.DriverQuirks):
    name = "trino"
    driver = "adbc_driver_trino"
    driver_name = "ADBC Driver Foundry Driver for Trino"
    vendor_name = "Trino"
    vendor_version = "Trino 476"
    short_version = "476"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=False,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=False,
        statement_get_parameter_schema=False,
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
        # Trino doesn't support multi-statement queries, so we must split them properly
        # Split by semicolon but respect string literals (don't split on semicolons inside quotes)
        statements = []
        current_statement = ""
        in_single_quotes = False
        i = 0

        while i < len(statement):
            char = statement[i]

            if char == "'" and not in_single_quotes:
                in_single_quotes = True
                current_statement += char
            elif char == "'" and in_single_quotes:
                # Check if this is an escaped quote ('')
                if i + 1 < len(statement) and statement[i + 1] == "'":
                    # This is an escaped quote, add both characters
                    current_statement += "''"
                    i += 1  # Skip the next quote
                else:
                    # This is the end of the string literal
                    in_single_quotes = False
                    current_statement += char
            elif char == ";" and not in_single_quotes:
                # This is a statement separator - add current statement without the semicolon
                cleaned = current_statement.strip()
                if cleaned:
                    statements.append(cleaned)
                current_statement = ""
            else:
                current_statement += char

            i += 1

        # Add the last statement if any
        cleaned = current_statement.strip()
        if cleaned:
            statements.append(cleaned)

        return statements


QUIRKS = [TrinoQuirks()]
