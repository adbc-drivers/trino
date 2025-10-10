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
	return nil, nil
}

// ExecuteBulkIngest performs Trino bulk ingest using INSERT statements
func (c *trinoConnectionImpl) ExecuteBulkIngest(ctx context.Context, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	if stream == nil {
		return -1, c.Base().ErrorHelper.InvalidArgument("stream cannot be nil")
	}

	var totalRowsInserted int64

	// Get schema from stream and create table if needed
	schema := stream.Schema()
	if err := c.createTableIfNeeded(ctx, conn, options.TableName, schema, options); err != nil {
		return -1, c.Base().ErrorHelper.IO("failed to create table: %v", err)
	}

	// Build INSERT statement with appropriate casts for unsupported types
	var placeholders []string
	for i := 0; i < int(schema.NumFields()); i++ {
		field := schema.Field(i)
		placeholder := c.getParameterPlaceholder(field.Type)
		placeholders = append(placeholders, placeholder)
	}

	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" VALUES (%s)",
		options.TableName,
		strings.Join(placeholders, ", "))

	// Prepare the statement (once for all batches)
	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return -1, c.Base().ErrorHelper.IO("failed to prepare insert statement: %v", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Process each record batch in the stream
	for stream.Next() {
		record := stream.RecordBatch()

		// Insert each row
		rowsInBatch := int(record.NumRows())
		for rowIdx := range rowsInBatch {
			params := make([]any, record.NumCols())

			for colIdx := range int(record.NumCols()) {
				arr := record.Column(colIdx)
				field := schema.Field(colIdx)

				// Use type converter to get Go value
				value, err := c.TypeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return -1, c.Base().ErrorHelper.IO("failed to convert value at row %d, col %d: %v", rowIdx, colIdx, err)
				}
				params[colIdx] = value
			}

			// Execute the insert
			_, err := stmt.ExecContext(ctx, params...)
			if err != nil {
				return -1, c.Base().ErrorHelper.IO("failed to execute insert: %v", err)
			}
		}

		// Track rows inserted in this batch
		totalRowsInserted += int64(rowsInBatch)
	}

	// Check for stream errors
	if err := stream.Err(); err != nil {
		return -1, c.Base().ErrorHelper.IO("stream error: %v", err)
	}

	return totalRowsInserted, nil
}


// getParameterPlaceholder returns the appropriate SQL placeholder for Arrow types that need casting
func (c *trinoConnectionImpl) getParameterPlaceholder(arrowType arrow.DataType) string {
	switch arrowType.(type) {
	case *arrow.Float32Type:
		return "CAST(? AS REAL)"
	case *arrow.Float64Type:
		return "CAST(? AS DOUBLE)"
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		return "FROM_BASE64(?)"
	default:
		return "?"
	}
}


// createTableIfNeeded creates the table based on the ingest mode
func (c *trinoConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, tableName, schema, false)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, tableName, schema, true)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table (ignore error if table doesn't exist)
		_ = c.dropTable(ctx, conn, tableName)
		return c.createTable(ctx, conn, tableName, schema, false)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.Base().ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}


// createTable creates a Trino table from Arrow schema
func (c *trinoConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("CREATE TABLE ")
	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString("\"")
	queryBuilder.WriteString(tableName)
	queryBuilder.WriteString("\" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString("\"")
		queryBuilder.WriteString(field.Name)
		queryBuilder.WriteString("\" ")

		// Convert Arrow type to Trino type
		trinoType := c.arrowToTrinoType(field.Type, field.Nullable)
		queryBuilder.WriteString(trinoType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String())
	return err
}


// dropTable drops a Trino table
func (c *trinoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName)
	_, err := conn.ExecContext(ctx, dropSQL)
	return err
}


// arrowToTrinoType converts Arrow data type to Trino column type
func (c *trinoConnectionImpl) arrowToTrinoType(arrowType arrow.DataType, nullable bool) string {
	var trinoType string

	switch arrowType := arrowType.(type) {
	case *arrow.BooleanType:
		trinoType = "BOOLEAN"
	case *arrow.Int8Type:
		trinoType = "TINYINT"
	case *arrow.Int16Type:
		trinoType = "SMALLINT"
	case *arrow.Int32Type:
		trinoType = "INTEGER"
	case *arrow.Int64Type:
		trinoType = "BIGINT"
	case *arrow.Float32Type:
		trinoType = "REAL"
	case *arrow.Float64Type:
		trinoType = "DOUBLE"
	case *arrow.StringType:
		trinoType = "VARCHAR"
	case *arrow.BinaryType:
		trinoType = "VARBINARY"
	case *arrow.Date32Type:
		trinoType = "DATE"
	case *arrow.TimestampType:

		// Determine precision based on Arrow timestamp unit
		var precision string
		switch arrowType.Unit {
		case arrow.Second:
			precision = ""
		case arrow.Millisecond:
			precision = "(3)"
		case arrow.Microsecond:
			precision = "(6)"
		case arrow.Nanosecond:
			precision = "(6)" // MySQL max is 6 digits
		default:
			precision = ""
		}

		// Use TIMESTAMP for timezone-naive timestamps, TIMESTAMP WITH TIME ZONE for timezone-aware
		if arrowType.TimeZone != "" {
			// Timezone-aware (timestamptz) -> TIMESTAMP WITH TIME ZONE
			trinoType = "TIMESTAMP" + precision + " WITH TIME ZONE"
		} else {
			// Timezone-naive (timestamp) -> TIMESTAMP
			trinoType = "TIMESTAMP" + precision
		}

	case *arrow.Time32Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Second:
			trinoType = "TIME"
		case arrow.Millisecond:
			trinoType = "TIME(3)"
		default:
			trinoType = "TIME"
		}

	case *arrow.Time64Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Microsecond:
			trinoType = "TIME(6)"
		case arrow.Nanosecond:
			trinoType = "TIME(6)" // Trino max is 6 digits
		default:
			trinoType = "TIME"
		}
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		if decType, ok := arrowType.(arrow.DecimalType); ok {
			trinoType = fmt.Sprintf("DECIMAL(%d,%d)", decType.GetPrecision(), decType.GetScale())
		} else {
			trinoType = "DECIMAL(10,2)"
		}
	default:
		// Default to VARCHAR for unknown types
		trinoType = "VARCHAR"
	}

	// Note: In Trino, columns are nullable by default, and NOT NULL constraint is specified separately
	// For simplicity, we don't add NULL/NOT NULL here since Trino handles nullability differently
	return trinoType
}
