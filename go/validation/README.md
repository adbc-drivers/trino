<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Validation Suite Setup

1. Start the Docker container:

   ```shell
   docker compose up --detach --wait
   ```
2. Set the environment variable:

   ```shell
   export TRINO_HOST="localhost"
   export TRINO_PORT="8080"
   export TRINO_CATALOG="memory"
   export TRINO_SCHEMA="default"
   export TRINO_SSL_MODE="http"
   export TRINO_SSL_CERT_PATH="/path/to/ci/docker/certs/ca.crt"
   export TRINO_USERNAME="test"
   ```

   For HTTP:

   ```shell
   export TRINO_DSN="http://test@localhost:8080?catalog=memory&schema=default&timezone=UTC"
   ```

   For the local HTTPS setup with the self-signed test CA, switch `TRINO_PORT`
   to `8443`, `TRINO_SSL_MODE` to `https`, and use:

   ```shell
   export TRINO_DSN="https://test@localhost:8443?catalog=memory&schema=default&SSLCertPath=/path/to/ci/docker/certs/ca.crt&timezone=UTC"
   ```

   `TRINO_DSN` is used by the general validation suite. The URI-focused tests in
   `validation/tests/trino/test_uri.py` build their own connection strings from
   the `TRINO_HOST`, `TRINO_PORT`, `TRINO_CATALOG`, `TRINO_SCHEMA`,
   `TRINO_SSL_MODE`, `TRINO_SSL_CERT_PATH`, and `TRINO_USERNAME` variables.
3. Run the tests:

   ```shell
   cd validation
   pixi run test
   ```
