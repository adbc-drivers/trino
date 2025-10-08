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

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentCatalog() (string, error) {
	return "", nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentDbSchema() (string, error) {
	return "", nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentCatalog(catalog string) error {
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentDbSchema(schema string) error {
	return nil
}

func (c *trinoConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	c.version = ""
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// GetTableSchema returns the Arrow schema for a Trino table
func (c *trinoConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	// Struct to capture Trino column information
	type tableColumn struct {
		OrdinalPosition        int32
		ColumnName             string
		DataType               string
		IsNullable             string
	}

   // Build query to get column information from Trino information schema
	query := `SELECT
		ordinal_position,
		column_name,
		data_type,
		is_nullable
	FROM information_schema.columns
	WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
	ORDER BY ordinal_position`

	var catalogName, schemaName string

	// Get catalog
	if catalog != nil && *catalog != "" {
		catalogName = *catalog
	} else {
		catalogName, err = c.GetCurrentCatalog()
		if err != nil {
			return nil, c.Base().ErrorHelper.IO("failed to get current catalog: %v", err)
		}
	}

	// Get schema
	if dbSchema != nil && *dbSchema != "" {
		schemaName = *dbSchema
	} else {
		schemaName, err = c.GetCurrentDbSchema()
		if err != nil {
			return nil, c.Base().ErrorHelper.IO("failed to get current schema: %v", err)
		}
	}

	args := []any{catalogName, schemaName, tableName}

	// Execute query to get column information
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, c.Base().ErrorHelper.IO("failed to query table schema: %v", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var columns []tableColumn
	for rows.Next() {
		var col tableColumn
		err := rows.Scan(
			&col.OrdinalPosition,
			&col.ColumnName,
			&col.DataType,
			&col.IsNullable,
		)
		if err != nil {
			return nil, c.Base().ErrorHelper.IO("failed to scan column information: %v", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, c.Base().ErrorHelper.IO("rows error: %v", err)
	}

	if len(columns) == 0 {
		return nil, c.Base().ErrorHelper.InvalidArgument("table not found: %s", tableName)
	}

	// Build Arrow schema from column information using type converter
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		// Parse the Trino data type to extract precision and scale
		typeName, precision, scale := parseTrinoDataType(col.DataType)

		// Create ColumnType struct for the type converter with parsed information
		colType := sqlwrapper.ColumnType{
			Name:             col.ColumnName,
			DatabaseTypeName: typeName,
			Nullable:         col.IsNullable == "YES",
			Precision:        precision,
			Scale:            scale,
		}

		arrowType, nullable, metadata, err := c.TypeConverter.ConvertRawColumnType(colType)
		if err != nil {
			return nil, c.Base().ErrorHelper.IO("failed to convert column type for %s: %v", col.ColumnName, err)
		}

		fields[i] = arrow.Field{
			Name:     col.ColumnName,
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// parseTrinoDataType extracts type name, precision and scale from Trino data type strings
// Examples: "decimal(10,2)" -> ("DECIMAL", 10, 2), "time(6)" -> ("TIME", 6, nil), "timestamp(6) with time zone" -> ("TIMESTAMP WITH TIME ZONE", 6, nil)
func parseTrinoDataType(dataType string) (string, *int64, *int64) {
	s := strings.ToLower(strings.TrimSpace(dataType))
	s = strings.Join(strings.Fields(s), " ") // normalize whitespace
	var precision, scale *int64

	suffix := ""
	for _, candidate := range []string{"with time zone", "without time zone"} {
		if strings.HasSuffix(s, candidate) {
			suffix = " " + candidate
			s = strings.TrimSpace(strings.TrimSuffix(s, candidate))
			break
		}
	}

	re := regexp.MustCompile(`^([a-z]+(?: [a-z]+)*)(?:\((\d+)(?:,(\d+))?\))?$`)
	m := re.FindStringSubmatch(s)
	if len(m) == 0 {
		return strings.ToUpper(dataType), nil, nil
	}

	typeName := strings.ToUpper(m[1]) + strings.ToUpper(suffix)

	if m[2] != "" {
		if p, err := strconv.ParseInt(m[2], 10, 64); err == nil {
			precision = &p
		}
	}
	if m[3] != "" {
		if s, err := strconv.ParseInt(m[3], 10, 64); err == nil {
			scale = &s
		}
	}

	return typeName, precision, scale
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
