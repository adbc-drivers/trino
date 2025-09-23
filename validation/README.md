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
   export Trino_DSN="http://test@localhost:8080?catalog=memory&schema=default"
   ```
3. Run the tests:

   ```shell
   cd validation
   pixi run test
   ```
