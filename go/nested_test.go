// Copyright (c) 2026 ADBC Drivers Contributors
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
	"fmt"
	"strconv"
	"strings"
	"testing"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// describe renders a parsed type compactly, e.g. ROW(id:BIGINT, :VARCHAR[5]).
func describe(t *trinoType) string {
	if len(t.children) == 0 {
		if len(t.params) == 0 {
			return t.base
		}
		return fmt.Sprint(t.base, t.params)
	}
	parts := make([]string, len(t.children))
	for i, c := range t.children {
		parts[i] = describe(c.typ)
		if t.base == "ROW" {
			parts[i] = c.name + ":" + parts[i]
		}
	}
	return t.base + "(" + strings.Join(parts, ", ") + ")"
}

func TestParseTrinoType(t *testing.T) {
	for text, want := range map[string]string{
		"BIGINT":                                 "BIGINT",
		"DECIMAL(10,2)":                          "DECIMAL[10 2]",
		"time(3)   with   time   zone":           "TIME WITH TIME ZONE[3]",
		"INTERVAL DAY(2) TO SECOND(3)":           "INTERVAL DAY TO SECOND[2 3]",
		"QDIGEST(BIGINT)":                        "QDIGEST", // type parameters make a leaf
		"MAP(INTEGER, ARRAY(VARCHAR))":           "MAP(INTEGER, ARRAY(VARCHAR))",
		`ROW(X INTEGER, Y ARRAY(ROW(Z BIGINT)))`: "ROW(X:INTEGER, Y:ARRAY(ROW(Z:BIGINT)))",
		// anonymous fields, including multi-word types
		`ROW(INTEGER, DECIMAL(2,1), TIMESTAMP(3) WITH TIME ZONE, INTERVAL YEAR TO MONTH)`: "ROW(:INTEGER, :DECIMAL[2 1], :TIMESTAMP WITH TIME ZONE[3], :INTERVAL YEAR TO MONTH)",
		// Trino only quotes field names that need it, so names can be keywords
		`ROW(TIMESTAMP INTEGER, TIME TIMESTAMP(3) WITH TIME ZONE, INTERVAL DAY(2) TO SECOND(6))`: "ROW(TIMESTAMP:INTEGER, TIME:TIMESTAMP WITH TIME ZONE[3], :INTERVAL DAY TO SECOND[2 6])",
		`ROW("MY FIELD" INTEGER, "A,B" VARCHAR, "F(X)" BOOLEAN, "SAY ""HI""" DOUBLE)`:            `ROW(MY FIELD:INTEGER, A,B:VARCHAR, F(X):BOOLEAN, SAY "HI":DOUBLE)`,
	} {
		typ, err := parseTrinoType(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, describe(typ), text)
	}

	for _, text := range []string{
		"", "ROW(", "ROW()", "MAP()", "ARRAY(INTEGER, INTEGER)", "MAP(INTEGER)", "ARRAY(INTEGER) EXTRA",
		`ROW("UNTERMINATED INTEGER)`, `ROW("NO TYPE")`, "ROW(A INTEGER,, B INTEGER)",
		// containers without type arguments
		"MAP", "ARRAY(MAP)", "MAP(VARCHAR, ARRAY)", "ROW(X ROW)", "ROW(AR(1)RAY)",
	} {
		_, err := parseTrinoType(text)
		assert.Error(t, err, text)
	}
}

func TestNestedColumnTypes(t *testing.T) {
	convert := func(dbType string) arrow.DataType {
		dt, nullable, meta, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: dbType})
		require.NoError(t, err)
		assert.True(t, nullable)
		name, _ := meta.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
		assert.Equal(t, dbType, name)
		return dt
	}

	// unparsable type text falls back to the default (opaque) conversion
	_, isOpaque := convert("ROW(").(*extensions.OpaqueType)
	assert.True(t, isOpaque)

	// a type without an Arrow equivalent converts like a top-level column
	st := convert("ROW(INTEGER, QDIGEST(BIGINT))").(*arrow.StructType)
	assert.Equal(t, "field0", st.Field(0).Name)
	assert.True(t, arrow.TypeEqual(extensions.NewOpaqueType(arrow.BinaryTypes.String, "QDIGEST", vendorName), st.Field(1).Type))

	// nested fields report precision, scale and length like top-level columns
	st = convert("ROW(A INTERVAL DAY(2), B INTERVAL SECOND(2, 6), C INTERVAL DAY TO SECOND, D VARCHAR, E VARCHAR(5))").(*arrow.StructType)
	for _, tc := range []struct{ field, key, want string }{
		{"a", sqlwrapper.MetaKeyFractionalSecondsPrecision, "0"},
		{"b", sqlwrapper.MetaKeyFractionalSecondsPrecision, "6"},
		{"c", sqlwrapper.MetaKeyFractionalSecondsPrecision, ""},
		{"d", sqlwrapper.MetaKeyLength, "2147483647"},
		{"e", sqlwrapper.MetaKeyLength, "5"},
	} {
		f, _ := st.FieldByName(tc.field)
		got, _ := f.Metadata.GetValue(tc.key)
		assert.Equal(t, tc.want, got, tc.field)
	}

	// map keys that cannot be recovered from the protocol's strings stay strings
	key := convert("MAP(TIMESTAMP(3), INTEGER)").(*arrow.MapType).KeyField()
	assert.Equal(t, arrow.BinaryTypes.String, key.Type)
	name, _ := key.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
	assert.Equal(t, "TIMESTAMP(3)", name)
}

// newInserter returns an inserter and builder for a column of a Trino type.
func newInserter(t *testing.T, mem memory.Allocator, dbType string) (sqlwrapper.Inserter, array.Builder) {
	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: dbType})
	require.NoError(t, err)
	bldr := array.NewBuilder(mem, dt)
	ins, err := typeConverter.CreateInserter(&arrow.Field{Name: "c", Type: dt, Nullable: true}, bldr)
	require.NoError(t, err)
	return ins, bldr
}

func TestMapInserter(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ins, bldr := newInserter(t, mem, "MAP(INTEGER, VARCHAR)")
	defer bldr.Release()

	require.NoError(t, ins.AppendValue(map[string]any{"10": "ten", "2": "two", "-1": nil}))
	require.NoError(t, ins.AppendValue(nil))
	require.NoError(t, ins.AppendValue(map[string]any{}))
	// rejected before anything is appended
	assert.Error(t, ins.AppendValue(map[string]any{"not a number": "x"}))
	assert.Error(t, ins.AppendValue("not a map"))

	arr := bldr.NewArray()
	defer arr.Release()
	// keys are converted to their type and sorted by their text
	assert.Equal(t, `[{[-1 10 2] [(null) "ten" "two"]} (null) {[] []}]`, arr.String())
}

// fakeRow stands in for trino.Row, which has no exported constructor.
type fakeRow []any

func (r fakeRow) Len() int          { return len(r) }
func (r fakeRow) Name(i int) string { return "field" + strconv.Itoa(i) }
func (r fakeRow) Value(i int) any   { return r[i] }

func TestStructInserter(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ins, bldr := newInserter(t, mem, "ROW(A BIGINT, B ARRAY(VARCHAR))")
	defer bldr.Release()

	require.NoError(t, ins.AppendValue(fakeRow{int64(1), []any{"x", nil}}))
	require.NoError(t, ins.AppendValue(nil))
	require.NoError(t, ins.AppendValue(fakeRow{nil, nil}))
	// rejected before anything is appended
	assert.Error(t, ins.AppendValue(fakeRow{int64(1)}))
	assert.Error(t, ins.AppendValue("not a row"))

	arr := bldr.NewArray()
	defer arr.Release()
	assert.Equal(t, `{[1 (null) (null)] [["x" (null)] (null) (null)]}`, arr.String())
	assert.True(t, arr.IsNull(1))
}

func TestDeeplyNested(t *testing.T) {
	// Nothing depends on the depth. Maps are kept to 10 levels, as building
	// nested map arrays in arrow-go takes time exponential in the map depth.
	for _, tc := range []struct {
		depth int
		kinds string // 'A' ARRAY, 'R' ROW, 'M' MAP, cycled from the inside out
	}{{200, "AR"}, {30, "ARM"}} {
		t.Run(fmt.Sprint(tc.depth, tc.kinds), func(t *testing.T) {
			typeText, value := "BIGINT", any(int64(42))
			for i := range tc.depth {
				switch tc.kinds[i%len(tc.kinds)] {
				case 'A':
					typeText, value = "ARRAY("+typeText+")", []any{value}
				case 'R':
					typeText, value = "ROW(F "+typeText+")", fakeRow{value}
				case 'M':
					typeText, value = "MAP(VARCHAR, "+typeText+")", map[string]any{"k": value}
				}
			}
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ins, bldr := newInserter(t, mem, typeText)
			defer bldr.Release()
			require.NoError(t, ins.AppendValue(value))
			arr := bldr.NewArray()
			defer arr.Release()

			// walk down to the BIGINT
			levels := 0
			for done := false; !done; {
				switch a := arr.(type) {
				case *array.Map: // before *array.List, which it embeds
					arr, levels = a.Items(), levels+1
				case *array.List:
					arr, levels = a.ListValues(), levels+1
				case *array.Struct:
					arr, levels = a.Field(0), levels+1
				default:
					done = true
				}
			}
			assert.Equal(t, tc.depth, levels)
			require.IsType(t, &array.Int64{}, arr)
			assert.Equal(t, int64(42), arr.(*array.Int64).Value(0))
		})
	}
}

// FuzzConvertNestedColumnType checks that any ARRAY, MAP or ROW type text
// converts without panicking or failing (unparsable types fall back to the
// default conversion). The seeds also run in plain `go test`.
func FuzzConvertNestedColumnType(f *testing.F) {
	for _, seed := range []string{
		`ROW(X INTEGER, "MY FIELD" ARRAY(VARCHAR(3)), TIMESTAMP TIMESTAMP(3) WITH TIME ZONE)`,
		`MAP(VARCHAR, ROW(N INTEGER, M MAP(DECIMAL(10, 2), ROW(FLAG BOOLEAN))))`,
		`ARRAY(ROW(INTEGER, INTERVAL DAY(2) TO SECOND(6), QDIGEST(BIGINT)))`,
		`ROW("A""B" DOUBLE, "C,D" UUID)`, `ARRAY(MAP)`, `ROW(AR(1)RAY)`, `ROW(`, `MAP()`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, typeName string) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)
		for _, text := range []string{typeName, "ARRAY(" + typeName + ")", "ROW(X " + typeName + ")"} {
			if !isNestedTypeName(text) {
				continue
			}
			ins, bldr := newInserter(t, mem, text)
			require.NoError(t, ins.AppendValue(nil))
			bldr.NewArray().Release()
			bldr.Release()
		}
	})
}
