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
	"github.com/trinodb/trino-go-client/trino"
)

const (
	// Trino's default maximum query size limit (1 million bytes)
	TrinoMaxQuerySizeBytes = 1_000_000
)

// trinoConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type trinoConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version string
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*trinoConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*trinoConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*trinoConnectionImpl)(nil)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	var catalog string
	err := c.Db.QueryRowContext(ctx, "SELECT current_catalog").Scan(&catalog)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current catalog")
	}
	return catalog, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	var schema string
	err := c.Db.QueryRowContext(ctx, "SELECT current_schema").Scan(&schema)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current schema")
	}
	return schema, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	if catalog == "" {
		return nil // No-op for empty catalog
	}
	_, err := c.Db.ExecContext(ctx, "USE "+quoteIdentifier(catalog)+".information_schema")
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *trinoConnectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil // No-op for empty schema
	}
	_, err := c.Db.ExecContext(ctx, "USE "+quoteIdentifier(schema))
	return err
}

func (c *trinoConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version string
		if err := c.Conn.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes LIMIT 1").Scan(&version); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get version")
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
		catalogName, err = c.GetCurrentCatalog(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Get schema
	if dbSchema != nil && *dbSchema != "" {
		schemaName = *dbSchema
	} else {
		schemaName, err = c.GetCurrentDbSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	qualifiedTableName := fmt.Sprintf("%s.%s.%s", quoteIdentifier(catalogName), quoteIdentifier(schemaName), quoteIdentifier(tableName))

	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", qualifiedTableName)
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to prepare statement")
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Go's database/sql package doesn't provide a direct way to extract
	// column types from a prepared statement without executing it.
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to execute schema query")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Get column types from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, c.ErrorHelper.WrapInternal(err, "failed to get column types")
	}

	if len(columnTypes) == 0 {
		return nil, c.ErrorHelper.NotFound("table not found: %s", tableName)
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
			return nil, c.ErrorHelper.WrapInternal(err, "failed to convert column type for %s", colType.Name())
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

// QuoteIdentifier implements BulkIngester
func (c *trinoConnectionImpl) QuoteIdentifier(name string) string {
	return quoteIdentifier(name)
}

// GetPlaceholder implements BulkIngester
func (c *trinoConnectionImpl) GetPlaceholder(field *arrow.Field, index int) string {
	return c.getParameterPlaceholder(*field)
}

// Ensure trinoConnectionImpl implements BulkIngester
var _ sqlwrapper.BulkIngester = (*trinoConnectionImpl)(nil)

// ExecuteBulkIngest performs Trino bulk ingest using batched INSERT statements.
func (c *trinoConnectionImpl) ExecuteBulkIngest(ctx context.Context, stmt sqlwrapper.StatementImpl, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	// inject the query ID capture through everything
	params := stmt.(*trinoStatement).GetAdditionalExecParams()

	schema := stream.Schema()
	if err := c.createTableIfNeeded(ctx, conn, options.TableName, schema, options, params); err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to create table")
	}

	if options.IngestBatchSize > 0 {
		return sqlwrapper.ExecuteBatchedBulkIngest(
			ctx, stmt, conn, options, stream,
			c.TypeConverter, c, &c.Base().ErrorHelper,
		)
	}

	if options.MaxQuerySizeBytes == 0 {
		options.MaxQuerySizeBytes = TrinoMaxQuerySizeBytes
	}

	// Use Trino-specific batching with accurate serialized size measurement
	return c.executeDynamicBatchedIngest(ctx, conn, options, stream, params)
}

// executeDynamicBatchedIngest performs batched INSERT with incremental query building.
//
// Trino has a 1MB query limit by default. This function builds the INSERT query
// incrementally, checking the actual query length after each row. When adding a row
// would exceed the limit, the current batch is executed and a new batch starts.
//
// This implementation builds complete INSERT statements with
// serialized values embedded directly, rather than using parameterized queries.
func (c *trinoConnectionImpl) executeDynamicBatchedIngest(
	ctx context.Context,
	conn *sqlwrapper.LoggingConn,
	options *driverbase.BulkIngestOptions,
	stream array.RecordReader,
	params []any,
) (int64, error) {
	var totalRowsInserted int64
	schema := stream.Schema()
	numCols := len(schema.Fields())

	quotedTableName := quoteIdentifier(options.TableName)
	baseQuery := fmt.Sprintf("INSERT INTO %s VALUES ", quotedTableName)

	for stream.Next() {
		recordBatch := stream.RecordBatch()
		totalRows := int(recordBatch.NumRows())

		var queryBuilder strings.Builder
		queryBuilder.WriteString(baseQuery)
		currentBatchRows := 0
		startRowIdx := 0

		for rowIdx := range totalRows {
			// Serialize all values in this row
			serializedRow := make([]string, numCols)
			for colIdx := range numCols {
				arr := recordBatch.Column(colIdx)
				field := schema.Field(colIdx)

				goValue, err := c.TypeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err, "failed to convert value")
				}

				serialized, err := trino.Serial(goValue)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err, "failed to serialize value")
				}

				serializedRow[colIdx] = serialized
			}

			// Build row string: "(val1,val2,val3)" or "CAST(val AS TYPE)" where needed
			rowString := c.buildRowString(schema, serializedRow)

			// Check if adding this row would exceed the limit
			// Add 1 for comma separator (except for first row in batch)
			additionalLength := len(rowString)
			if currentBatchRows > 0 {
				additionalLength += 1 // for comma
			}

			if queryBuilder.Len()+additionalLength > options.MaxQuerySizeBytes && currentBatchRows > 0 {
				// Execute current batch
				rowsInserted, err := c.executeBatch(ctx, conn, queryBuilder.String(), params)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err,
						"failed to insert batch at rows %d-%d", startRowIdx, startRowIdx+currentBatchRows-1)
				}
				totalRowsInserted += rowsInserted

				// Start new batch with this row
				queryBuilder.Reset()
				queryBuilder.WriteString(baseQuery)
				queryBuilder.WriteString(rowString)
				currentBatchRows = 1
				startRowIdx = rowIdx
			} else {
				// Add row to current batch
				if currentBatchRows > 0 {
					queryBuilder.WriteString(",")
				}
				queryBuilder.WriteString(rowString)
				currentBatchRows++
			}
		}

		// Execute final batch for this record batch
		if currentBatchRows > 0 {
			rowsInserted, err := c.executeBatch(ctx, conn, queryBuilder.String(), params)
			if err != nil {
				return totalRowsInserted, c.ErrorHelper.WrapIO(err,
					"failed to insert final batch at rows %d-%d", startRowIdx, startRowIdx+currentBatchRows-1)
			}
			totalRowsInserted += rowsInserted
		}
	}

	if err := stream.Err(); err != nil {
		return totalRowsInserted, c.ErrorHelper.WrapIO(err, "stream error")
	}

	return totalRowsInserted, nil
}

// getParameterPlaceholder returns the appropriate SQL placeholder for Arrow types that need casting
func (c *trinoConnectionImpl) getParameterPlaceholder(field arrow.Field) string {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "CAST(? AS UUID)"
	}

	switch field.Type.(type) {
	case *arrow.Float32Type:
		return "CAST(? AS REAL)"
	case *arrow.Float64Type:
		return "CAST(? AS DOUBLE)"
	default:
		return "?"
	}
}

// buildRowString constructs a single row string like "(val1,val2,val3)" with CAST where needed.
// Returns the complete row string ready to be appended to the query.
func (c *trinoConnectionImpl) buildRowString(schema *arrow.Schema, serializedValues []string) string {
	var rowBuilder strings.Builder
	rowBuilder.WriteString("(")

	for colIdx, serializedValue := range serializedValues {
		if colIdx > 0 {
			rowBuilder.WriteString(",")
		}

		field := schema.Field(colIdx)

		// Get the placeholder (e.g., "?", "CAST(? AS REAL)", "CAST(? AS UUID)")
		// Then replace ? with the actual serialized value
		placeholder := c.getParameterPlaceholder(field)
		valueWithCast := strings.Replace(placeholder, "?", serializedValue, 1)
		rowBuilder.WriteString(valueWithCast)
	}

	rowBuilder.WriteString(")")
	return rowBuilder.String()
}

// executeBatch executes the current query batch and returns rows inserted.
func (c *trinoConnectionImpl) executeBatch(
	ctx context.Context,
	conn *sqlwrapper.LoggingConn,
	querySQL string,
	params []any,
) (int64, error) {
	if querySQL == "" {
		return 0, nil
	}

	result, err := conn.ExecContext(ctx, querySQL, params...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// createTableIfNeeded creates the table based on the ingest mode
func (c *trinoConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions, params []any) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, tableName, schema, false, params)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, tableName, schema, true, params)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, tableName, params); err != nil {
			return err
		}
		return c.createTable(ctx, conn, tableName, schema, false, params)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a Trino table from Arrow schema
func (c *trinoConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, ifNotExists bool, params []any) error {
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
		trinoType, err := c.arrowToTrinoType(field)
		if err != nil {
			return c.ErrorHelper.WrapInternal(err, "convert Arrow type for column %s", field.Name)
		}
		queryBuilder.WriteString(trinoType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String(), params...)
	return err
}

// dropTable drops a Trino table
func (c *trinoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, params []any) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(tableName))
	_, err := conn.ExecContext(ctx, dropSQL, params...)
	return err
}

// arrowToTrinoType converts Arrow data type to Trino column type
func (c *trinoConnectionImpl) arrowToTrinoType(field arrow.Field) (string, error) {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "UUID", nil
	}

	var trinoType string

	switch arrowType := field.Type.(type) {
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
			precision = "(0)"
		case arrow.Millisecond:
			precision = "(3)"
		case arrow.Microsecond:
			precision = "(6)"
		case arrow.Nanosecond:
			precision = "(9)"
		default:
			return "", fmt.Errorf("unsupported Arrow timestamp unit: %v", arrowType.Unit)
		}

		// Use TIMESTAMP for timezone-naive timestamps, TIMESTAMP WITH TIME ZONE for timezone-aware
		if arrowType.TimeZone != "" {
			trinoType = "TIMESTAMP" + precision + " WITH TIME ZONE"
		} else {
			trinoType = "TIMESTAMP" + precision
		}

	case *arrow.Time32Type:
		switch arrowType.Unit {
		case arrow.Second:
			trinoType = "TIME(0)"
		case arrow.Millisecond:
			trinoType = "TIME(3)"
		default:
			return "", fmt.Errorf("unsupported Arrow Time32 unit: %v", arrowType.Unit)
		}

	case *arrow.Time64Type:
		switch arrowType.Unit {
		case arrow.Microsecond:
			trinoType = "TIME(6)"
		case arrow.Nanosecond:
			trinoType = "TIME(9)"
		default:
			return "", fmt.Errorf("unsupported Arrow Time64 unit: %v", arrowType.Unit)
		}
	case arrow.DecimalType:
		trinoType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to VARCHAR for unknown types
		trinoType = "VARCHAR"
	}

	// Note: In Trino, columns are nullable by default, and assume that all columns are nullable since trino go client does not provide clean way to get nullability.
	return trinoType, nil
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
