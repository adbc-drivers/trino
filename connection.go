// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package trino


import (
   "context"
   "fmt"

   "github.com/apache/arrow-adbc/go/adbc"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentCatalog() (string, error) {
	var catalog string
	err := c.Db.QueryRowContext(context.Background(), "SELECT current_catalog").Scan(&catalog)
	if err != nil {
		return "", c.Base().ErrorHelper.IO("failed to get current catalog: %v", err)
	}
	if catalog == "" {
		return "", c.Base().ErrorHelper.InvalidState("no current catalog set")
	}
	return catalog, nil
}


// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentDbSchema() (string, error) {
	var schema string
	err := c.Db.QueryRowContext(context.Background(), "SELECT current_schema").Scan(&schema)
	if err != nil {
		return "", c.Base().ErrorHelper.IO("failed to get current schema: %v", err)
	}
	return schema, nil
}


// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentCatalog(catalog string) error {
	_, err := c.Db.ExecContext(context.Background(), "USE "+catalog)
	return err
}


// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema == "" {
		return nil // No-op for empty schema
	}
	_, err := c.Db.ExecContext(context.Background(), "USE "+schema)
	return err
}


func (c *trinoConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version string
		if err := c.Conn.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes LIMIT 1").Scan(&version); err != nil {
			return c.ErrorHelper.Errorf(adbc.StatusInternal, "failed to get version: %v", err)
		}
		c.version = fmt.Sprintf("Trino %s", version)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}
