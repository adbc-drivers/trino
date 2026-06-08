#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
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

# Initialize extra catalogs/schemas for the validation suite.

set -euo pipefail

exec trino --server "${TRINO_SERVER:-trino:8080}" --user init --execute "
CREATE SCHEMA IF NOT EXISTS secondmemory.myschema;
CREATE SCHEMA IF NOT EXISTS memory.secondschema;
"
