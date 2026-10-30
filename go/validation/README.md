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

Run these commands from the `go/` directory.

1. Start the Docker containers:

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
   export TRINO_SSL_CERT_PATH="${PWD}/ci/docker/certs/ca.crt"
   export TRINO_SSL_CLIENT_CERT_PATH="${PWD}/ci/docker/certs/client.crt"
   export TRINO_SSL_CLIENT_KEY_PATH="${PWD}/ci/docker/certs/client.key"
   export TRINO_USERNAME="test"
   ```

   For HTTP:

   ```shell
   export TRINO_DSN="http://test@localhost:8080?catalog=memory&schema=default&timezone=UTC"
   ```

   The Docker HTTPS endpoint uses certificate authentication. From the `go/`
   directory, load the server CA, client certificate/key, and connection settings:

   ```shell
   source .env.linux
   ```

   `TRINO_DSN` is used by the general validation suite. The URI-focused tests in
   `validation/tests/trino/test_uri.py` build their own connection strings from
   the `TRINO_HOST`, `TRINO_PORT`, `TRINO_CATALOG`, `TRINO_SCHEMA`,
   `TRINO_SSL_MODE`, `TRINO_SSL_CERT_PATH`, `TRINO_SSL_CLIENT_CERT_PATH`,
   `TRINO_SSL_CLIENT_KEY_PATH`, and `TRINO_USERNAME` variables.

   `SSLCertPath` verifies the server certificate. `SSLClientCertPath` and
   `SSLClientKeyPath` authenticate the client. The URI tests exercise file,
   inline PEM, and mixed credentials, and verify that HTTPS queries fail without
   a client certificate. Plain HTTP remains available for the HTTP tests.
3. Build the shared library and run validation from the `go/` directory:

   ```shell
   pixi run make
   pixi run validate
   ```
