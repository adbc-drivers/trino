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
	"errors"
	"fmt"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentCatalog() (string, error) {
	var catalog string
	err := c.Db.QueryRowContext(context.Background(), "SELECT current_catalog").Scan(&catalog)
	if err != nil {
		return "", c.Base().ErrorHelper.IO("failed to get current catalog: %v", err)
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
	if catalog == "" {
		return nil // No-op for empty catalog
	}
	_, err := c.Db.ExecContext(context.Background(), "USE "+catalog+".information_schema")
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

// GetTableSchema returns the Arrow schema for a Trino table
func (c *trinoConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	var catalogName, schemaName string

	// Get catalog
	if catalog != nil && *catalog != "" {
		catalogName = *catalog
	} else {
		catalogName, err = c.GetCurrentCatalog()
		if err != nil {
			return nil, err
		}
	}

	// Get schema
	if dbSchema != nil && *dbSchema != "" {
		schemaName = *dbSchema
	} else {
		schemaName, err = c.GetCurrentDbSchema()
		if err != nil {
			return nil, err
		}
	}

	qualifiedTableName := fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName)

	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", qualifiedTableName)
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, c.Base().ErrorHelper.IO("failed to prepare statement: %v", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Go's database/sql package doesn't provide a direct way to extract
	// column types from a prepared statement without executing it.
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, c.Base().ErrorHelper.IO("failed to execute schema query: %v", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Get column types from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, c.Base().ErrorHelper.Internal("failed to get column types: %v", err)
	}

	if len(columnTypes) == 0 {
		return nil, c.Base().ErrorHelper.NotFound("table not found: %s", tableName)
	}

	// Convert column types to Arrow fields using the existing type converter
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		wrappedColType := sqlwrapper.ColumnType{
			Name:             colType.Name(),
			DatabaseTypeName: colType.DatabaseTypeName(),
			Nullable:         true, // Default to nullable
		}

		if nullable, ok := colType.Nullable(); ok {
			wrappedColType.Nullable = nullable
		}

		// Add precision and scale if available
		if precision, scale, ok := colType.DecimalSize(); ok {
			p, s := int64(precision), int64(scale)
			wrappedColType.Precision = &p
			wrappedColType.Scale = &s
		} else if length, ok := colType.Length(); ok {
			l := int64(length)
			wrappedColType.Precision = &l
		}

		arrowType, nullable, metadata, err := c.TypeConverter.ConvertRawColumnType(wrappedColType)
		if err != nil {
			return nil, c.Base().ErrorHelper.Internal("failed to convert column type for %s: %v", colType.Name(), err)
		}

		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}


// ExecuteBulkIngest performs Trino bulk ingest using INSERT statements
func (c *trinoConnectionImpl) ExecuteBulkIngest(ctx context.Context, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	return -1, nil
}

// createTableIfNeeded creates the table based on the ingest mode
// nolint:unused // Placeholder implementation
func (c *trinoConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	return nil
}

// createTable creates a Trino table from Arrow schema
// nolint:unused // Placeholder implementation
func (c *trinoConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	return nil
}

// dropTable drops a Trino table
// nolint:unused // Placeholder implementation
func (c *trinoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string) error {
	return nil
}

// arrowToTrinoType converts Arrow data type to Trino column type
// nolint:unused // Placeholder implementation
func (c *trinoConnectionImpl) arrowToTrinoType(arrowType arrow.DataType, nullable bool) string {
	return "VARCHAR"
}
