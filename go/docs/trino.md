---
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
{}
---

{{ cross_reference|safe }}
# Trino Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Trino][trino], a free and
open-source distributed SQL query engine.

## Installation

The Trino driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install trino
```

## Connecting

To use the driver, provide a Trino connection string as the `uri` option. The driver supports URI format and DSN-style connection strings, but URIs are recommended.

```python
from adbc_driver_manager import dbapi

dbapi.connect(
  driver="trino",
  db_kwargs={
      "uri": "http://user@localhost:8080?catalog=tcph&schema=tiny"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

```
trino://[user[:password]@]host[:port][/catalog[/schema]][?attribute1=value1&attribute2=value2...]
```

Components:
- Scheme: trino:// (required)
- `user`: Optional (for authentication)
- `password`: Optional (for authentication, requires user)
- `host`: Required (no default)
- `port`: Optional (defaults to 8080 for HTTP, 8443 for HTTPS)
- `catalog`: Optional (Trino catalog name)
- `schema`: Optional (schema within catalog)
- Query params: Trino connection attributes

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

#### HTTPS/SSL Configuration

HTTP Basic authentication is only supported on encrypted connections over HTTPS.

By default, connections use HTTPS. To connect using HTTP, add `SSL=false` as a query parameter:

- `trino://localhost:8080/catalog?SSL=false` → Uses HTTP on port 8080
- `trino://localhost/catalog?SSL=false` → Uses HTTP on default port 8080
- `trino://localhost:8443/catalog?SSL=true` → Uses HTTPS on port 8443
- `trino://localhost:8080/catalog` → Uses HTTPS on port 8080
- `trino://localhost/catalog` → Uses HTTPS on default port 8443

See [Trino JDBC Documentation](https://trino.io/docs/current/client/jdbc.html#parameter-reference) for complete parameter reference and [Trino Concepts](https://trino.io/docs/current/overview/concepts.html#catalog) for more information.

Examples:

- `trino://localhost:8080/hive/default`
- `trino://user:pass@trino.example.com:8080/postgresql/public`
- `trino://trino.example.com/hive/sales?SSL=true`
- `trino://user@localhost:8443/memory/default?SSL=true&source=myapp`
- `trino://user@localhost:8080/memory/default?session_properties=task_concurrency:2;query_priority:1`

The driver also supports the Trino DSN format (see [Go Trino Client documentation](https://github.com/trinodb/trino-go-client?tab=readme-ov-file#dsn-data-source-name)), but URIs are recommended.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.2.0](./v0.2.0.md)
- [v0.1.0](./v0.1.0.md)

{{ footnotes|safe }}

[trino]: https://trino.io/
