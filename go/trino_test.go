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

package trino_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	trino "github.com/adbc-drivers/trino"
)

// TrinoQuirks implements validation.DriverQuirks for Trino ADBC driver
type TrinoQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *TrinoQuirks) SetupDriver(t *testing.T) driverbase.DriverWithContext {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return trino.NewDriver(q.mem)
}

func (q *TrinoQuirks) TearDownDriver(t *testing.T, _ driverbase.DriverWithContext) {
	q.mem.AssertSize(t, 0)
}

func (q *TrinoQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *TrinoQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("trino", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to Trino types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}

		if !field.Nullable {
			createQuery.WriteString(" NOT NULL")
		}
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]any, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []any
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					insertQuery.WriteString("?")
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *TrinoQuirks) DropTable(cnxn adbc.ConnectionWithContext, tblName string) error {
	stmt, err := cnxn.NewStatement(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close(context.Background()))
	}()

	if err = stmt.SetSqlQuery(context.Background(), "DROP TABLE IF EXISTS "+tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *TrinoQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our Trino type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "int"
		metadata["sql.precision"] = "10"
		metadata["sql.scale"] = "0"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "BIGINT"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "VARCHAR"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "float"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "double"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "tinyint"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *TrinoQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *TrinoQuirks) BindParameter(idx int) string { return "?" }

// TODO(https://github.com/adbc-drivers/driverbase-go/issues/126)
func (q *TrinoQuirks) SupportsBulkIngest(string) bool              { return false }
func (q *TrinoQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *TrinoQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *TrinoQuirks) SupportsExecuteSchema() bool                 { return false }
func (q *TrinoQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *TrinoQuirks) SupportsPartitionedData() bool               { return false }
func (q *TrinoQuirks) SupportsStatistics() bool                    { return true }
func (q *TrinoQuirks) SupportsTransactions() bool                  { return true }
func (q *TrinoQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *TrinoQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (q *TrinoQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *TrinoQuirks) SupportsGetTableSchema() bool                { return true }
func (q *TrinoQuirks) Catalog() string                             { return "memory" }
func (q *TrinoQuirks) DBSchema() string                            { return "default" }

func (q *TrinoQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for Trino"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "v18.8.0"
	case adbc.InfoVendorVersion:
		return regexp.MustCompile(`Trino [0-9]+`)
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "Trino"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*TrinoQuirks)) {
	dsn := os.Getenv("TRINO_DSN")
	if dsn == "" {
		t.Skip("Set TRINO_DSN environment variable for validation tests")
	}

	q := &TrinoQuirks{dsn: dsn}
	fn(q)
}

type TrinoStatementTests struct {
	validation.StatementTests
}

func TestGetStatisticsIncludesCreatedTable(t *testing.T) {
	withQuirks(t, func(q *TrinoQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cnxn.Close(ctx))
		}()

		tableName := "adbc_stats_test"
		rec, _, err := array.RecordFromJSON(q.Alloc(), arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil), bytes.NewReader([]byte(`[
			{"ints": 1, "strings": "a"},
			{"ints": 2, "strings": null},
			{"ints": 3, "strings": "b"}
		]`)))
		require.NoError(t, err)
		defer rec.Release()

		require.NoError(t, q.CreateSampleTable(tableName, rec))
		defer func() {
			_ = q.DropTable(cnxn, tableName)
		}()

		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		require.True(t, ok)

		cat := q.Catalog()
		sch := q.DBSchema()
		tblPattern := tableName
		rdr, err := stats.GetStatistics(ctx, &cat, &sch, &tblPattern, true)
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, adbc.GetStatisticsSchema.Equal(rdr.Schema()))

		var tableStats []testutil.Statistic
		for rdr.Next() {
			rec := rdr.RecordBatch()
			stats := testutil.ExtractStatisticsForTable(rec, cat, sch, tableName)
			if len(stats) > 0 {
				tableStats = append(tableStats, stats...)
			}
		}
		require.NoError(t, rdr.Err())
		require.NotEmpty(t, tableStats, "expected statistics output to include created table %s.%s.%s", cat, sch, tableName)

		// Convert to lookup map for easier access
		statsMap := testutil.StatisticsToLookupMap(tableStats)

		// Validate row count: should be 3 (we inserted 3 rows)
		rowCount, ok := statsMap[int16(adbc.StatisticRowCountKey)].StatisticValue.(float64)
		require.True(t, ok, "expected row count statistic as float64")
		require.InDelta(t, 3.0, rowCount, 0.1, "expected row count to be approximately 3")

		// Column-level statistics are optional (Trino may not compute them for small tables)
		// but if they exist, validate their values
		// Note: Trino GetStatistics returns one statistic per (table, column, key) tuple,
		// so we need to iterate through all stats to find column-level statistics
		for _, stat := range tableStats {
			if stat.ColumnName != nil && *stat.ColumnName == "strings" && stat.StatisticKey == int16(adbc.StatisticNullCountKey) {
				nullCount, ok := stat.StatisticValue.(float64)
				require.True(t, ok, "expected null count to be float64")
				require.InDelta(t, 1.0, nullCount, 0.1, "expected null count for 'strings' to be approximately 1")
			}
		}
	})
}

func TestGetStatisticsWithWildcardCatalog(t *testing.T) {
	withQuirks(t, func(q *TrinoQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cnxn.Close(ctx))
		}()

		tableName := "adbc_stats_wildcard_test"
		rec, _, err := array.RecordFromJSON(q.Alloc(), arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}, nil), bytes.NewReader([]byte(`[
			{"id": 1},
			{"id": 2}
		]`)))
		require.NoError(t, err)
		defer rec.Release()

		require.NoError(t, q.CreateSampleTable(tableName, rec))
		defer func() {
			_ = q.DropTable(cnxn, tableName)
		}()

		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		require.True(t, ok)

		// Test with wildcard catalog pattern (should use system.jdbc.tables)
		cat := q.Catalog()
		wildcardCat := cat[:len(cat)-1] + "%" // Strip last char and add wildcard
		sch := q.DBSchema()
		tblPattern := tableName
		rdr, err := stats.GetStatistics(ctx, &wildcardCat, &sch, &tblPattern, true)
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, adbc.GetStatisticsSchema.Equal(rdr.Schema()))

		var tableStats []testutil.Statistic
		for rdr.Next() {
			rec := rdr.RecordBatch()
			stats := testutil.ExtractStatisticsForTable(rec, cat, sch, tableName)
			if len(stats) > 0 {
				tableStats = append(tableStats, stats...)
			}
		}
		require.NoError(t, rdr.Err())
		require.NotEmpty(t, tableStats, "expected wildcard catalog query to find table %s.%s.%s", cat, sch, tableName)

		// Convert to lookup map for easier access
		statsMap := testutil.StatisticsToLookupMap(tableStats)

		// Validate at least row count exists
		rowCount, ok := statsMap[int16(adbc.StatisticRowCountKey)].StatisticValue.(float64)
		require.True(t, ok, "expected row count statistic as float64 from wildcard query")
		require.InDelta(t, 2.0, rowCount, 0.1, "expected row count to be approximately 2")
	})
}

func (s *TrinoStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *TrinoQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type TrinoTests struct {
	suite.Suite

	Quirks *TrinoQuirks

	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *TrinoTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *TrinoTests) TearDownTest() {
	s.NoError(s.stmt.Close(s.ctx))
	s.NoError(s.cnxn.Close(s.ctx))
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close(s.ctx))
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *TrinoTests) TestIngestQueryId() {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "ints",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
	}, nil)
	batch := testutil.RecordFromJSON(s.T(), s.Quirks.Alloc(), schema, `[{"ints": 1}, {"ints": 2}, {"ints": 3}]`)
	defer batch.Release()
	s.Require().NoError(s.stmt.Bind(s.ctx, batch))
	s.Require().NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, "foobar"))
	s.Require().NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace))

	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
}

func (s *TrinoTests) TestSelect() {
	// Drop table if it exists first, then create test table with basic Trino types
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `DROP TABLE IF EXISTS memory.default.test_types`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE TABLE memory.default.test_types (
			bool_col BOOLEAN,
			tinyint_col TINYINT,
			int_col INTEGER,
			bigint_col BIGINT,
			float_col REAL,
			double_col DOUBLE,
			varchar_col VARCHAR(100)
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert multiple rows including NULLs to test nullable behavior
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		INSERT INTO memory.default.test_types VALUES
			(true, 42, 12345, 9876543210, 3.25, 6.75, 'hello world'),
			(false, NULL, 54321, NULL, 1.5, NULL, NULL),
			(true, 100, 99999, 1234567890, 2.0, 8.5, 'test string')
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.FixedWidthTypes.Boolean,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "istrue",
						"sql.database_type_name": "BOOLEAN",
					}),
				},
			}, nil),
			expected: `[{"istrue": true}, {"istrue": false}, {"istrue": true}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"value": 42}, {"value": null}, {"value": 100}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "INTEGER",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 12345}, {"theanswer": 54321}, {"theanswer": 99999}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}, {"theanswer": null}, {"theanswer": 1234567890}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "REAL",
					}),
				},
			}, nil),
			expected: `[{"value": 3.25}, {"value": 1.5}, {"value": 2.0}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"value": 6.75}, {"value": null}, {"value": 8.5}]`,
		},
		{
			name:  "string",
			query: "SELECT varchar_col AS greeting FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "greeting",
						"sql.database_type_name": "VARCHAR",
						"sql.length":             "100",
					}),
				},
			}, nil),
			expected: `[{"greeting": "hello world"}, {"greeting": null}, {"greeting": "test string"}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			if rdr != nil {
				defer rdr.Release()
			}

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type TrinoTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *TrinoTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("TRINO_DSN")
	if s.dsn == "" {
		s.T().Skip("Set TRINO_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = trino.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *TrinoTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close(s.ctx))
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close(s.ctx))
	}
	if s.db != nil {
		s.NoError(s.db.Close(s.ctx))
	}
	s.mem.AssertSize(s.T(), 0)
}

func TestTrinoTypeTests(t *testing.T) {
	dsn := os.Getenv("TRINO_DSN")
	if dsn == "" {
		t.Skip("Set TRINO_DSN environment variable for type tests")
	}

	quirks := &TrinoQuirks{dsn: dsn}
	suite.Run(t, &TrinoTests{Quirks: quirks})
}

func TestTrinoIntegrationSuite(t *testing.T) {
	suite.Run(t, new(TrinoTestSuite))
}

// TestURIParsing tests the parseTrinoURIToDSN function with various URI formats
func TestURIParsing(t *testing.T) {
	factory := trino.NewTrinoDBFactory()

	tests := []struct {
		name          string
		trinoURI      string
		username      string
		password      string
		expectedDSN   string
		shouldError   bool
		errorContains string
	}{
		{
			name:        "basic trino with port and catalog/schema",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default",
			expectedDSN: "https://user:pass@localhost:8080?catalog=hive&schema=default",
		},
		{
			name:        "trino without port",
			trinoURI:    "trino://user:pass@localhost/memory/default",
			expectedDSN: "https://user:pass@localhost:8443?catalog=memory&schema=default",
		},
		{
			name:        "trino without SSL - should default to 8080",
			trinoURI:    "trino://user:pass@localhost/hive/default?SSL=false",
			expectedDSN: "http://user:pass@localhost:8080?SSL=false&catalog=hive&schema=default",
		},
		{
			name:        "trino without database/catalog",
			trinoURI:    "trino://user:pass@localhost:8080",
			expectedDSN: "https://user:pass@localhost:8080",
		},
		{
			name:        "trino with only catalog, no schema",
			trinoURI:    "trino://user:pass@localhost:8080/postgresql",
			expectedDSN: "https://user:pass@localhost:8080?catalog=postgresql",
		},
		{
			name:        "trino with custom port",
			trinoURI:    "trino://user:pass@example.com:9999/hive/sales",
			expectedDSN: "https://user:pass@example.com:9999?catalog=hive&schema=sales",
		},
		{
			name:        "trino with ip address",
			trinoURI:    "trino://user:pass@127.0.0.1:8080/memory/test",
			expectedDSN: "https://user:pass@127.0.0.1:8080?catalog=memory&schema=test",
		},
		{
			name:        "trino with ipv6 host",
			trinoURI:    "trino://user:pass@[::1]:8080/hive/default",
			expectedDSN: "https://user:pass@[::1]:8080?catalog=hive&schema=default",
		},
		{
			name:        "trino with ipv6 host, default port",
			trinoURI:    "trino://user:pass@[::1]/memory/default",
			expectedDSN: "https://user:pass@[::1]:8443?catalog=memory&schema=default",
		},
		{
			name:        "no credentials in uri",
			trinoURI:    "trino://localhost:8080/hive/default",
			expectedDSN: "https://localhost:8080?catalog=hive&schema=default",
		},
		{
			name:        "only username in uri",
			trinoURI:    "trino://user@localhost:8080/memory/default",
			expectedDSN: "https://user@localhost:8080?catalog=memory&schema=default",
		},
		{
			name:        "override credentials with options",
			trinoURI:    "trino://olduser:oldpass@localhost:8080/hive/default",
			username:    "newuser",
			password:    "newpass",
			expectedDSN: "https://newuser:newpass@localhost:8080?catalog=hive&schema=default",
		},
		{
			name:        "add credentials via options",
			trinoURI:    "trino://localhost:8080/memory/default",
			username:    "admin",
			password:    "secret",
			expectedDSN: "https://admin:secret@localhost:8080?catalog=memory&schema=default",
		},
		{
			name:        "override only username",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default",
			username:    "newuser",
			expectedDSN: "https://newuser:pass@localhost:8080?catalog=hive&schema=default",
		},
		{
			name:        "override only password",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default",
			password:    "newpass",
			expectedDSN: "https://user:newpass@localhost:8080?catalog=hive&schema=default",
		},
		{
			name:        "single query parameter",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default?source=myapp",
			expectedDSN: "https://user:pass@localhost:8080?catalog=hive&schema=default&source=myapp",
		},
		{
			name:        "multiple query parameters",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default?source=myapp&SSL=false&custom_client=test",
			expectedDSN: "http://user:pass@localhost:8080?SSL=false&catalog=hive&custom_client=test&schema=default&source=myapp",
		},
		{
			name:        "ssl parameters with custom port",
			trinoURI:    "trino://user:pass@localhost:8888/memory/default?SSL=false&source=analytics",
			expectedDSN: "http://user:pass@localhost:8888?SSL=false&catalog=memory&schema=default&source=analytics",
		},
		{
			name:        "session properties",
			trinoURI:    "trino://user:pass@localhost:8080/hive/default?session_properties=task_concurrency=8,join_distribution_type=BROADCAST",
			expectedDSN: "https://user:pass@localhost:8080?catalog=hive&schema=default&session_properties=task_concurrency%3D8%2Cjoin_distribution_type%3DBROADCAST",
		},
		{
			name:        "url encoded catalog/schema names",
			trinoURI:    "trino://user:pass@localhost:8080/my%20catalog/my%20schema?source=test",
			expectedDSN: "https://user:pass@localhost:8080?catalog=my+catalog&schema=my+schema&source=test",
		},
		{
			name:        "credentials with special characters",
			trinoURI:    "trino://my%40user:p%40ssword@localhost:8080/hive/default",
			expectedDSN: "https://my%40user:p%40ssword@localhost:8080?catalog=hive&schema=default",
		},
		{
			name:          "invalid trino uri format",
			trinoURI:      "trino://[invalid-uri",
			shouldError:   true,
			errorContains: "invalid Trino URI format",
		},
		{
			name:          "invalid url encoding",
			trinoURI:      "trino://user:pass@%ZZ%invalid:8080/hive/default",
			shouldError:   true,
			errorContains: "invalid Trino URI format",
		},
		{
			name:        "default HTTP port without SSL specified",
			trinoURI:    "trino://user:pass@localhost/hive/default",
			expectedDSN: "https://user:pass@localhost:8443?catalog=hive&schema=default",
		},
		{
			name:        "default HTTPS port with SSL=false",
			trinoURI:    "trino://user:pass@localhost/hive/default?SSL=false",
			expectedDSN: "http://user:pass@localhost:8080?SSL=false&catalog=hive&schema=default",
		},
		{
			name:        "explicit port overrides SSL default",
			trinoURI:    "trino://user:pass@localhost:9999/hive/default?SSL=false",
			expectedDSN: "http://user:pass@localhost:9999?SSL=false&catalog=hive&schema=default",
		},
		{
			name:        "trino with trailing slash",
			trinoURI:    "trino://user:pass@localhost:8080/",
			expectedDSN: "https://user:pass@localhost:8080",
		},
		{
			name:        "trino with catalog and trailing slash",
			trinoURI:    "trino://user:pass@localhost:8080/hive/",
			expectedDSN: "https://user:pass@localhost:8080?catalog=hive",
		},

		// dsn tests
		{
			name:        "session properties (DSN format)",
			trinoURI:    "http://user:pass@localhost:8080?catalog=hive&schema=default&session_properties=task_concurrency%3D8%2Cjoin_distribution_type%3DBROADCAST",
			expectedDSN: "http://user:pass@localhost:8080?catalog=hive&schema=default&session_properties=task_concurrency%3D8%2Cjoin_distribution_type%3DBROADCAST",
		},
		{
			name:        "add credentials via options (DSN format)",
			trinoURI:    "http://localhost:8080?catalog=memory&schema=default&source=myapp&SSL=false&query_max_stage_count=100",
			username:    "admin",
			password:    "secret",
			expectedDSN: "http://admin:secret@localhost:8080?catalog=memory&schema=default&source=myapp&SSL=false&query_max_stage_count=100",
		},
		{
			name:        "override credentials with options (DSN format)",
			trinoURI:    "http://olduser:oldpass@localhost:8080?catalog=hive&schema=default&session_properties=task_concurrency%3D8%2Cjoin_distribution_type%3DBROADCAST",
			username:    "newuser",
			password:    "newpass",
			expectedDSN: "http://newuser:newpass@localhost:8080?catalog=hive&schema=default&session_properties=task_concurrency%3D8%2Cjoin_distribution_type%3DBROADCAST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.trinoURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			result, err := factory.BuildTrinoDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedDSN, result)
		})
	}
}

// fakeTrinoServer answers every query with a single row, returned by a
// follow-up request, and records the headers of the request that submitted
// each query (keyed by the query text) and of its follow-up request (keyed by
// "GET " and the query text).
type fakeTrinoServer struct {
	*httptest.Server

	mu      sync.Mutex
	headers map[string]http.Header
}

func newFakeTrinoServer(t *testing.T) *fakeTrinoServer {
	srv := &fakeTrinoServer{headers: map[string]http.Header{}}
	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/info" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"starting": false}`)
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
			query, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			srv.mu.Lock()
			srv.headers[string(query)] = r.Header.Clone()
			srv.mu.Unlock()
			nextURI := srv.URL + "/v1/statement/next?query=" + url.QueryEscape(string(query))
			_, _ = fmt.Fprintf(w, `{
				"id": "20260101_000000_00000_abcde",
				"infoUri": "http://localhost/ui/query.html",
				"nextUri": %q,
				"stats": {"state": "QUEUED"}
			}`, nextURI)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statement/next":
			srv.mu.Lock()
			srv.headers["GET "+r.URL.Query().Get("query")] = r.Header.Clone()
			srv.mu.Unlock()
			_, _ = io.WriteString(w, `{
				"id": "20260101_000000_00000_abcde",
				"infoUri": "http://localhost/ui/query.html",
				"stats": {"state": "FINISHED"},
				"columns": [{"name": "_col0", "type": "integer", "typeSignature": {"rawType": "integer", "arguments": []}}],
				"data": [[1]]
			}`)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (srv *fakeTrinoServer) header(t *testing.T, query, name string) []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	headers, ok := srv.headers[query]
	require.True(t, ok, "query %q was not received", query)
	return headers.Values(name)
}

// queries returns the text of every query submitted to the server.
func (srv *fakeTrinoServer) queries() []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	var queries []string
	for query := range srv.headers {
		if !strings.HasPrefix(query, "GET ") {
			queries = append(queries, query)
		}
	}
	return queries
}

func openFakeTrino(t *testing.T, srv *fakeTrinoServer, opts map[string]string) adbc.StatementWithContext {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	dbOpts := map[string]string{
		adbc.OptionKeyURI: srv.URL + "?clientTags=conn,shared",
	}
	maps.Copy(dbOpts, opts)
	db, err := trino.NewDriver(mem).NewDatabaseWithContext(ctx, dbOpts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })
	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cnxn.Close(ctx)) })
	stmt, err := cnxn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stmt.Close(ctx)) })
	return stmt
}

func TestHTTPHeaders(t *testing.T) {
	ctx := context.Background()
	srv := newFakeTrinoServer(t)
	stmt := openFakeTrino(t, srv, map[string]string{
		"trino.http_header.X-Gateway":      "database",
		"trino.http_header.X-Other":        "database",
		"trino.http_header.x-trino-source": "my-source",
		// An empty database header is ignored.
		"trino.http_header.X-Trino-Client-Tags": "",
		"trino.session_user":                    "alice",
	})

	execute := func(query string) {
		require.NoError(t, stmt.SetSqlQuery(ctx, query))
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
	}

	execute("SELECT 1")
	assert.Equal(t, []string{"conn,shared"}, srv.header(t, "SELECT 1", "X-Trino-Client-Tags"))
	assert.Equal(t, []string{"database"}, srv.header(t, "SELECT 1", "X-Gateway"))
	assert.Equal(t, []string{"my-source"}, srv.header(t, "SELECT 1", "X-Trino-Source"))
	assert.Equal(t, []string{"alice"}, srv.header(t, "SELECT 1", "X-Trino-User"))

	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Gateway", "statement"))
	execute("SELECT 2")
	assert.Equal(t, []string{"conn,shared"}, srv.header(t, "SELECT 2", "X-Trino-Client-Tags"))
	assert.Equal(t, []string{"statement"}, srv.header(t, "SELECT 2", "X-Gateway"))
	assert.Equal(t, []string{"database"}, srv.header(t, "SELECT 2", "X-Other"))

	// Options persist across executions.
	require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 3"))
	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	for reader.Next() {
	}
	require.NoError(t, reader.Err())
	reader.Release()
	assert.Equal(t, []string{"statement"}, srv.header(t, "SELECT 3", "X-Gateway"))

	// Statement and database headers also apply to the follow-up requests of
	// the query.
	assert.Equal(t, []string{"statement"}, srv.header(t, "GET SELECT 3", "X-Gateway"))
	assert.Equal(t, []string{"database"}, srv.header(t, "GET SELECT 3", "X-Other"))
	assert.Equal(t, []string{"my-source"}, srv.header(t, "GET SELECT 3", "X-Trino-Source"))

	// The header replaces the connection client tags.
	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Trino-Client-Tags", "raw"))
	execute("SELECT 4")
	assert.Equal(t, []string{"raw"}, srv.header(t, "SELECT 4", "X-Trino-Client-Tags"))

	// An empty value clears the option.
	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Trino-Client-Tags", ""))
	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Gateway", ""))
	execute("SELECT 5")
	assert.Equal(t, []string{"conn,shared"}, srv.header(t, "SELECT 5", "X-Trino-Client-Tags"))
	assert.Equal(t, []string{"database"}, srv.header(t, "SELECT 5", "X-Gateway"))
}

func TestHTTPHeadersIngest(t *testing.T) {
	ctx := context.Background()
	srv := newFakeTrinoServer(t)
	stmt := openFakeTrino(t, srv, nil)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	batch := testutil.RecordFromJSON(t, memory.DefaultAllocator, schema, `[{"ints": 1}, {"ints": 2}]`)
	defer batch.Release()
	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Gateway", "ingest"))
	require.NoError(t, stmt.SetOption(ctx, adbc.OptionKeyIngestTargetTable, "foo"))
	require.NoError(t, stmt.SetOption(ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace))
	require.NoError(t, stmt.Bind(ctx, batch))
	_, err := stmt.ExecuteUpdate(ctx)
	require.NoError(t, err)

	// Every query run by the ingest (drop, create, insert) carries the header.
	queries := srv.queries()
	require.NotEmpty(t, queries)
	var sawInsert bool
	for _, query := range queries {
		sawInsert = sawInsert || strings.Contains(query, "INSERT")
		assert.Equal(t, []string{"ingest"}, srv.header(t, query, "X-Gateway"), query)
	}
	assert.True(t, sawInsert, "no INSERT among %q", queries)
}

func TestHTTPHeadersExecuteSchema(t *testing.T) {
	ctx := context.Background()
	srv := newFakeTrinoServer(t)
	stmt := openFakeTrino(t, srv, nil)

	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Gateway", "statement"))
	require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
	schema, err := stmt.(adbc.StatementExecuteSchema).ExecuteSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// ExecuteSchema runs a rewritten query, so look it up.
	queries := srv.queries()
	require.Len(t, queries, 1)
	assert.Equal(t, []string{"statement"}, srv.header(t, queries[0], "X-Gateway"))
}

func TestHTTPHeaderOptions(t *testing.T) {
	ctx := context.Background()
	srv := newFakeTrinoServer(t)
	stmt := openFakeTrino(t, srv, nil)
	getter := stmt.(adbc.GetSetOptionsWithContext)

	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.x-gateway", "value"))
	val, err := getter.GetOption(ctx, "trino.http_header.X-Gateway")
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	// Known options that are not set return an empty string.
	val, err = getter.GetOption(ctx, "trino.http_header.X-Unset")
	require.NoError(t, err)
	assert.Equal(t, "", val)

	require.NoError(t, stmt.SetOption(ctx, "trino.http_header.X-Gateway", ""))
	val, err = getter.GetOption(ctx, "trino.http_header.X-Gateway")
	require.NoError(t, err)
	assert.Equal(t, "", val)

	// Unknown options are not found.
	_, err = getter.GetOption(ctx, "trino.statement.unknown")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusNotFound, adbcErr.Code)

	for _, tc := range []struct {
		key, value string
	}{
		{"trino.http_header.Accept-Encoding", "gzip"},
		{"trino.http_header.Connection", "close"},
		{"trino.http_header.X-Trino-User", "someone"},
		{"trino.http_header.x-trino-catalog", "other"},
		{"trino.http_header.Authorization", "Bearer token"},
		{"trino.http_header.Bad Header", "value"},
		{"trino.http_header.", "value"},
		{"trino.http_header.X-Gateway", "bad\nvalue"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			err := stmt.SetOption(ctx, tc.key, tc.value)
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		})
	}

	for _, opts := range []map[string]string{
		{"trino.http_header.X-Trino-Session": "value"},
		{"trino.http_header.Bad Header": "value"},
		{"trino.http_header.X-Foo": "a", "trino.http_header.x-foo": "b"},
	} {
		t.Run(fmt.Sprint(opts), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			dbOpts := map[string]string{adbc.OptionKeyURI: srv.URL}
			maps.Copy(dbOpts, opts)
			_, err := trino.NewDriver(mem).NewDatabaseWithContext(ctx, dbOpts)
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		})
	}
}

// TestSetCurrentCatalogKeepsSchema checks that setting the current catalog keeps
// the current schema if the new catalog has a schema with that name.
func TestSetCurrentCatalogKeepsSchema(t *testing.T) {
	withQuirks(t, func(q *TrinoQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cnxn.Close(ctx))
		}()

		opts := cnxn.(adbc.GetSetOptionsWithContext)

		current := func() string {
			catalog, err := opts.GetOption(ctx, adbc.OptionKeyCurrentCatalog)
			require.NoError(t, err)
			schema, err := opts.GetOption(ctx, adbc.OptionKeyCurrentDbSchema)
			require.NoError(t, err)
			return catalog + "." + schema
		}

		require.Equal(t, "memory.default", current())
		require.NoError(t, opts.SetOption(ctx, adbc.OptionKeyCurrentCatalog, "memory"))
		assert.Equal(t, "memory.default", current())

		// tpch has no schema "default", so the schema has to be set as well.
		require.NoError(t, opts.SetOption(ctx, adbc.OptionKeyCurrentCatalog, "tpch"))
		assert.Equal(t, "tpch.information_schema", current())
		require.NoError(t, opts.SetOption(ctx, adbc.OptionKeyCurrentDbSchema, "tiny"))
		assert.Equal(t, "tpch.tiny", current())

		// The same works inside a transaction.
		require.NoError(t, opts.SetOption(ctx, adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
		require.NoError(t, opts.SetOption(ctx, adbc.OptionKeyCurrentCatalog, "memory"))
		assert.Equal(t, "memory.information_schema", current())
		require.NoError(t, cnxn.Rollback(ctx))
	})
}
