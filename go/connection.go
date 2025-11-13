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
	"strings"

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
	_, err := c.Db.ExecContext(context.Background(), "USE "+quoteIdentifier(catalog)+".information_schema")
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema == "" {
		return nil // No-op for empty schema
	}
	_, err := c.Db.ExecContext(context.Background(), "USE "+quoteIdentifier(schema))
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

	qualifiedTableName := fmt.Sprintf("%s.%s.%s", quoteIdentifier(catalogName), quoteIdentifier(schemaName), quoteIdentifier(tableName))

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
			Nullable:         true, // Assume every column in always nullable since trino go client does not provide clean way to get nullability.
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
	for _, field := range schema.Fields() {
		placeholder := c.getParameterPlaceholder(field.Type)
		placeholders = append(placeholders, placeholder)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)",
		quoteIdentifier(options.TableName),
		strings.Join(placeholders, ", "))

	// Prepare the statement (once for all batches)
	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return -1, c.Base().ErrorHelper.IO("failed to prepare insert statement: %v", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	params := make([]any, len(schema.Fields()))

	// Process each record batch in the stream
	for stream.Next() {
		recordBatch := stream.RecordBatch()

		// Insert each row
		rowsInBatch := int(recordBatch.NumRows())
		for rowIdx := range rowsInBatch {
			for colIdx := range int(recordBatch.NumCols()) {
				arr := recordBatch.Column(colIdx)
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
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, tableName); err != nil {
			return err
		}
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
	queryBuilder.WriteString(quoteIdentifier(tableName))
	queryBuilder.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(quoteIdentifier(field.Name))
		queryBuilder.WriteString(" ")

		// Convert Arrow type to Trino type
		trinoType := c.arrowToTrinoType(field.Type)
		queryBuilder.WriteString(trinoType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String())
	return err
}

// dropTable drops a Trino table
func (c *trinoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(tableName))
	_, err := conn.ExecContext(ctx, dropSQL)
	return err
}

// arrowToTrinoType converts Arrow data type to Trino column type
func (c *trinoConnectionImpl) arrowToTrinoType(arrowType arrow.DataType) string {
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
	case *arrow.BinaryViewType:
		trinoType = "VARBINARY"
	case *arrow.FixedSizeBinaryType:
		trinoType = "VARBINARY"
	case *arrow.LargeBinaryType:
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
			precision = "(9)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Arrow timestamp unit: %v", arrowType.Unit))
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
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time32 unit: %v", arrowType.Unit))
		}

	case *arrow.Time64Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Microsecond:
			trinoType = "TIME(6)"
		case arrow.Nanosecond:
			trinoType = "TIME(9)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time64 unit: %v", arrowType.Unit))
		}
	case arrow.DecimalType:
		trinoType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to VARCHAR for unknown types
		trinoType = "VARCHAR"
	}

	// Note: In Trino, columns are nullable by default, and assume that all columns are nullable since trino go client does not provide clean way to get nullability.
	return trinoType
}

// ListTableTypes implements driverbase.TableTypeLister interface
func (c *trinoConnectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Trino supports these standard table types
	return []string{
		"BASE TABLE", // Regular tables
		"VIEW",       // Views
	}, nil
}

// quoteIdentifier properly quotes a SQL identifier, escaping any internal quotes
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
