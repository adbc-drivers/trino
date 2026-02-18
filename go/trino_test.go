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
	"os"
	"regexp"
	"strings"
	"testing"

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

func (q *TrinoQuirks) SetupDriver(t *testing.T) adbc.Driver {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return trino.NewDriver(q.mem)
}

func (q *TrinoQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
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

			values := make([]interface{}, r.NumCols())
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
			var queryParams []interface{}
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

func (q *TrinoQuirks) DropTable(cnxn adbc.Connection, tblName string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	if err = stmt.SetSqlQuery("DROP TABLE IF EXISTS " + tblName); err != nil {
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
func (q *TrinoQuirks) SupportsStatistics() bool                    { return false }
func (q *TrinoQuirks) SupportsTransactions() bool                  { return false }
func (q *TrinoQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *TrinoQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (q *TrinoQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *TrinoQuirks) Catalog() string                             { return "memory" }
func (q *TrinoQuirks) DBSchema() string                            { return "default" }

func (q *TrinoQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for Trino"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
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
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *TrinoTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *TrinoTests) TearDownTest() {
	s.NoError(s.stmt.Close())
	s.NoError(s.cnxn.Close())
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close())
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *TrinoTests) TestSelect() {
	// Drop table if it exists first, then create test table with basic Trino types
	s.NoError(s.stmt.SetSqlQuery(`DROP TABLE IF EXISTS memory.default.test_types`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(`
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
	s.NoError(s.stmt.SetSqlQuery(`
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
			s.NoError(s.stmt.SetSqlQuery(testCase.query))

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
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
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
	s.db, err = s.driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *TrinoTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close())
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close())
	}
	if s.db != nil {
		s.NoError(s.db.Close())
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
