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

{{ version_header|safe }}

This driver provides access to [Trino][trino], a free and
open-source distributed SQL query engine.

## Installation & Quickstart

The driver can be installed with `dbc`.

To use the driver, provide a Trino connection string as the `uri` option. The driver supports URI format and DSN-style connection strings, but URIs are recommended.

## Connection String Format

```
trino://[user[:password]@]host[:port][/catalog[/schema]][?attribute1=value1&attribute2=value2...]
```

Components:
- Scheme: trino:// (required)
- User: Optional (for authentication)
- Password: Optional (for authentication, requires user)
- Host: Required (no default)
- Port: Optional (defaults to 80 for HTTP, 443 for HTTPS)
- Catalog: Optional (Trino catalog name)
- Schema: Optional (schema within catalog)
- Query params: Trino connection attributes

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

See [Trino JDBC Documentation](https://trino.io/docs/current/client/jdbc.html#parameter-reference) for complete parameter reference and [Trino Concepts](https://trino.io/docs/current/overview/concepts.html#catalog) for more information.

Examples:
- trino://localhost:8080/hive/default
- trino://user:pass@trino.example.com:8080/postgresql/public
- trino://trino.example.com/hive/sales?SSL=true
- trino://user@localhost:8443/memory/default?SSL=true&source=myapp

The driver also supports the Trino DSN format (see [Go Trino Client documentation](https://github.com/trinodb/trino-go-client?tab=readme-ov-file#dsn-data-source-name)), but URIs are recommended.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

[trino]: https://trino.io/
