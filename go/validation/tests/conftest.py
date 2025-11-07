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

import sys
from pathlib import Path

import adbc_drivers_validation.model
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    noci,
    pytest_addoption,
    pytest_collection_modifyitems,
)

from .trino import TrinoQuirks


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("trino:")
    return TrinoQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    # Assume shared library is in the repo root
    return str(
        Path(__file__).parent.parent.parent
        / f"build/libadbc_driver_{driver.name}.{ext}"
    )
