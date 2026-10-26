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

import adbc_driver_manager.dbapi
import pytest
from adbc_drivers_validation.tests.connection import (
    TestConnection,  # noqa: F401
    generate_tests,
)

from . import trino


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(trino.QUIRKS, metafunc)


@pytest.mark.parametrize("option", ["trino.session_user", "trino.impersonation_user"])
def test_session_user(driver_path: str, db_kwargs: dict, option: str) -> None:
    session_user = os.environ.get("TRINO_SESSION_USER")
    if not session_user:
        pytest.skip("TRINO_SESSION_USER must be set")

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={**db_kwargs, option: session_user},
            autocommit=True,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_user")
        assert cursor.fetchone() == (session_user,)
