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
	"database/sql"
	"errors"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
)

func (c *trinoConnectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) (catalogs []string, err error) {
	// In Trino, catalogs are data sources (like memory, hive, etc.)
	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT catalog_name FROM system.metadata.catalogs")
	args := []any{}

	if catalogFilter != nil {
		queryBuilder.WriteString(" WHERE catalog_name LIKE ?")
		args = append(args, *catalogFilter)
	}

	queryBuilder.WriteString(" ORDER BY catalog_name")

	rows, err := c.Db.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.IO("failed to query catalogs: %v", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	catalogs = make([]string, 0)
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			return nil, c.ErrorHelper.IO("failed to scan catalog: %v", err)
		}
		catalogs = append(catalogs, catalog)
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.IO("error during catalog iteration: %v", err)
	}

	return catalogs, err
}

func (c *trinoConnectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) (schemas []string, err error) {
	// In Trino, schemas are namespaces within catalogs (e.g., memory.default, hive.warehouse)
	// Build query using strings.Builder
	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?")
	args := []any{catalog}

	if schemaFilter != nil {
		queryBuilder.WriteString(" AND schema_name LIKE ?")
		args = append(args, *schemaFilter)
	}

	queryBuilder.WriteString(" ORDER BY schema_name")

	rows, err := c.Db.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.IO("failed to query schemas for catalog %s: %v", catalog, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	schemas = make([]string, 0)
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, c.ErrorHelper.IO("failed to scan schema: %v", err)
		}
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.IO("error during schema iteration: %v", err)
	}

	return schemas, nil
}

func (c *trinoConnectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) (tables []driverbase.TableInfo, err error) {
	if includeColumns {
		return c.getTablesWithColumns(ctx, catalog, schema, tableFilter, columnFilter)
	}
	return c.getTablesOnly(ctx, catalog, schema, tableFilter)
}

// getTablesOnly retrieves table information without columns
func (c *trinoConnectionImpl) getTablesOnly(ctx context.Context, catalog string, schema string, tableFilter *string) (tables []driverbase.TableInfo, err error) {
	// In Trino, both catalog and schema are used to identify tables
	// Build query using strings.Builder
	var queryBuilder strings.Builder
	queryBuilder.WriteString(`
		SELECT
			table_name,
			table_type
		FROM information_schema.tables
		WHERE table_catalog = ? AND table_schema = ?`)

	args := []any{catalog, schema}

	if tableFilter != nil {
		queryBuilder.WriteString(` AND table_name LIKE ?`)
		args = append(args, *tableFilter)
	}

	queryBuilder.WriteString(` ORDER BY table_name`)

	rows, err := c.Db.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.IO("failed to query tables for catalog %s: %v", catalog, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	tables = make([]driverbase.TableInfo, 0)
	for rows.Next() {
		var tableName, tableType string
		if err := rows.Scan(&tableName, &tableType); err != nil {
			return nil, c.ErrorHelper.IO("failed to scan table info: %v", err)
		}

		tables = append(tables, driverbase.TableInfo{
			TableName: tableName,
			TableType: tableType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.IO("error during table iteration: %v", err)
	}

	return tables, err
}

// getTablesWithColumns retrieves complete table and column information
func (c *trinoConnectionImpl) getTablesWithColumns(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string) (tables []driverbase.TableInfo, err error) {
	// In Trino, both catalog and schema are used to identify tables

	type tableColumn struct {
		TableName       string
		TableType       string
		OrdinalPosition int32
		ColumnName      string
		ColumnComment   sql.NullString
		DataType        string
		IsNullable      string
		ColumnDefault   sql.NullString
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(`
		SELECT
			t.table_name,
			t.table_type,
			c.ordinal_position,
			c.column_name,
			c.comment,
			c.data_type,
			c.is_nullable,
			c.column_default
		FROM information_schema.tables t
		INNER JOIN information_schema.columns c
			ON t.table_catalog = c.table_catalog
			AND t.table_schema = c.table_schema
			AND t.table_name = c.table_name
		WHERE t.table_catalog = ? AND t.table_schema = ?`)

	args := []any{catalog, schema}

	if tableFilter != nil {
		queryBuilder.WriteString(` AND t.table_name LIKE ?`)
		args = append(args, *tableFilter)
	}
	if columnFilter != nil {
		queryBuilder.WriteString(` AND c.column_name LIKE ?`)
		args = append(args, *columnFilter)
	}

	queryBuilder.WriteString(` ORDER BY t.table_name, c.ordinal_position`)

	rows, err := c.Db.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.IO("failed to query tables with columns for catalog %s: %v", catalog, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	tables = make([]driverbase.TableInfo, 0)
	var currentTable *driverbase.TableInfo

	for rows.Next() {
		var tc tableColumn

		if err := rows.Scan(
			&tc.TableName, &tc.TableType,
			&tc.OrdinalPosition, &tc.ColumnName, &tc.ColumnComment,
			&tc.DataType, &tc.IsNullable, &tc.ColumnDefault,
		); err != nil {
			return nil, c.ErrorHelper.IO("failed to scan table with columns: %v", err)
		}

		// Check if we need to create a new table entry
		if currentTable == nil || currentTable.TableName != tc.TableName {
			tables = append(tables, driverbase.TableInfo{
				TableName: tc.TableName,
				TableType: tc.TableType,
			})
			currentTable = &tables[len(tables)-1]
		}

		// Process column data
		var radix *int16
		var nullable *int16

		// Set numeric precision radix for Trino types
		dataType := strings.ToUpper(tc.DataType)
		switch dataType {
		// Decimal radix (base 10) - all numeric types
		case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE":
			r := int16(10)
			radix = &r

		// No radix for non-numeric types (VARCHAR, BOOLEAN, DATE, TIMESTAMP, etc.)
		default:
			radix = nil
		}

		// Set nullable information
		switch tc.IsNullable {
		case "YES":
			n := int16(driverbase.XdbcColumnNullable)
			nullable = &n
		case "NO":
			n := int16(driverbase.XdbcColumnNoNulls)
			nullable = &n
		}

		currentTable.TableColumns = append(currentTable.TableColumns, driverbase.ColumnInfo{
			ColumnName:       tc.ColumnName,
			OrdinalPosition:  &tc.OrdinalPosition,
			Remarks:          driverbase.NullStringToPtr(tc.ColumnComment),
			XdbcTypeName:     &tc.DataType,
			XdbcNumPrecRadix: radix,
			XdbcNullable:     nullable,
			XdbcIsNullable:   &tc.IsNullable,
			XdbcColumnDef:    driverbase.NullStringToPtr(tc.ColumnDefault),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.IO("error during table with columns iteration: %v", err)
	}

	// TODO: Add constraint and foreign key metadata support

	return tables, err
}
