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
	"time"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestParseTrinoType(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		typ, err := parseTrinoType("BIGINT")
		require.NoError(t, err)
		assert.Equal(t, "BIGINT", typ.base)
		assert.Empty(t, typ.params)
	})

	t.Run("parametric scalars", func(t *testing.T) {
		for text, want := range map[string]struct {
			base   string
			params []int64
		}{
			"DECIMAL(10, 2)":               {"DECIMAL", []int64{10, 2}},
			"DECIMAL(10,2)":                {"DECIMAL", []int64{10, 2}},
			"VARCHAR(5)":                   {"VARCHAR", []int64{5}},
			"TIMESTAMP(3) WITH TIME ZONE":  {"TIMESTAMP WITH TIME ZONE", []int64{3}},
			"TIME(6)":                      {"TIME", []int64{6}},
			"INTERVAL DAY TO SECOND":       {"INTERVAL DAY TO SECOND", nil},
			"INTERVAL DAY(2) TO SECOND(3)": {"INTERVAL DAY TO SECOND", []int64{2, 3}},
			"TIMESTAMP(12)":                {"TIMESTAMP", []int64{12}},
			"time(3)   with   time   zone": {"TIME WITH TIME ZONE", []int64{3}},
		} {
			typ, err := parseTrinoType(text)
			require.NoError(t, err, text)
			assert.Equal(t, want.base, typ.base, text)
			assert.Equal(t, want.params, typ.params, text)
		}
	})

	t.Run("type parameters", func(t *testing.T) {
		typ, err := parseTrinoType("QDIGEST(BIGINT)")
		require.NoError(t, err)
		assert.Equal(t, "QDIGEST", typ.base)
		assert.Equal(t, "QDIGEST(BIGINT)", typ.text)
		assert.Empty(t, typ.params)
		assert.Empty(t, typ.children)
	})

	t.Run("named row", func(t *testing.T) {
		typ, err := parseTrinoType(`ROW("ID" BIGINT, "TAGS" ARRAY(VARCHAR), "PRICE" DECIMAL(10, 2), "TS" TIMESTAMP(3) WITH TIME ZONE)`)
		require.NoError(t, err)
		require.Equal(t, "ROW", typ.base)
		require.Len(t, typ.children, 4)
		assert.Equal(t, "ID", typ.children[0].name)
		assert.Equal(t, "BIGINT", typ.children[0].typ.base)
		assert.Equal(t, "TAGS", typ.children[1].name)
		assert.Equal(t, "ARRAY", typ.children[1].typ.base)
		assert.Equal(t, "VARCHAR", typ.children[1].typ.children[0].typ.base)
		assert.Equal(t, "PRICE", typ.children[2].name)
		assert.Equal(t, []int64{10, 2}, typ.children[2].typ.params)
		assert.Equal(t, "TS", typ.children[3].name)
		assert.Equal(t, "TIMESTAMP WITH TIME ZONE", typ.children[3].typ.base)
	})

	t.Run("quoted names with spaces, commas, parentheses and quotes", func(t *testing.T) {
		typ, err := parseTrinoType(`ROW("MY FIELD" INTEGER, "A,B" VARCHAR, "F(X)" BOOLEAN, "SAY ""HI""" DOUBLE)`)
		require.NoError(t, err)
		require.Len(t, typ.children, 4)
		assert.Equal(t, "MY FIELD", typ.children[0].name)
		assert.Equal(t, "A,B", typ.children[1].name)
		assert.Equal(t, "F(X)", typ.children[2].name)
		assert.Equal(t, `SAY "HI"`, typ.children[3].name)
		assert.Equal(t, "DOUBLE", typ.children[3].typ.base)
	})

	t.Run("anonymous row", func(t *testing.T) {
		typ, err := parseTrinoType(`ROW(INTEGER, VARCHAR(1), TIMESTAMP(3) WITH TIME ZONE, DECIMAL(2,1), INTERVAL YEAR TO MONTH)`)
		require.NoError(t, err)
		require.Len(t, typ.children, 5)
		for _, c := range typ.children {
			assert.Empty(t, c.name)
		}
		assert.Equal(t, "TIMESTAMP WITH TIME ZONE", typ.children[2].typ.base)
		assert.Equal(t, "DECIMAL", typ.children[3].typ.base)
		assert.Equal(t, "INTERVAL YEAR TO MONTH", typ.children[4].typ.base)
	})

	t.Run("unquoted names", func(t *testing.T) {
		typ, err := parseTrinoType(`ROW(X INTEGER, Y ARRAY(ROW(Z BIGINT)))`)
		require.NoError(t, err)
		require.Len(t, typ.children, 2)
		assert.Equal(t, "X", typ.children[0].name)
		assert.Equal(t, "Y", typ.children[1].name)
		inner := typ.children[1].typ.children[0].typ
		assert.Equal(t, "ROW", inner.base)
		assert.Equal(t, "Z", inner.children[0].name)
	})

	t.Run("keyword field names, as the client protocol sends them", func(t *testing.T) {
		// Trino only quotes field names that need it, so a field may be
		// named after a type keyword.
		typ, err := parseTrinoType(`ROW(TIMESTAMP INTEGER, TIME TIMESTAMP(3) WITH TIME ZONE, INTERVAL DAY(2) TO SECOND(6), TIMESTAMP WITH TIME ZONE)`)
		require.NoError(t, err)
		require.Len(t, typ.children, 4)
		assert.Equal(t, "TIMESTAMP", typ.children[0].name)
		assert.Equal(t, "INTEGER", typ.children[0].typ.base)
		assert.Equal(t, "TIME", typ.children[1].name)
		assert.Equal(t, "TIMESTAMP WITH TIME ZONE", typ.children[1].typ.base)
		assert.Empty(t, typ.children[2].name)
		assert.Equal(t, "INTERVAL DAY TO SECOND", typ.children[2].typ.base)
		assert.Empty(t, typ.children[3].name)
		assert.Equal(t, "TIMESTAMP WITH TIME ZONE", typ.children[3].typ.base)
	})

	t.Run("map", func(t *testing.T) {
		typ, err := parseTrinoType(`MAP(INTEGER, ROW(DECIMAL(2,1), TIMESTAMP(3) WITH TIME ZONE))`)
		require.NoError(t, err)
		require.Equal(t, "MAP", typ.base)
		require.Len(t, typ.children, 2)
		assert.Equal(t, "INTEGER", typ.children[0].typ.base)
		assert.Equal(t, "ROW", typ.children[1].typ.base)
		assert.Len(t, typ.children[1].typ.children, 2)
	})

	t.Run("invalid", func(t *testing.T) {
		for _, text := range []string{
			"",
			"ROW(",
			"ROW()",
			"ARRAY(INTEGER, INTEGER)",
			"MAP(INTEGER)",
			`ROW("UNTERMINATED INTEGER)`,
			"ARRAY(INTEGER) EXTRA",
			// containers without type arguments
			"MAP",
			"ARRAY(MAP)",
			"MAP(VARCHAR, ARRAY)",
			"ROW(X ROW)",
			"ROW(AR(1)RAY)",
			"ROW(A INTEGER,, B INTEGER)",
		} {
			_, err := parseTrinoType(text)
			assert.Error(t, err, text)
		}
	})
}

func TestConvertNestedColumnTypes(t *testing.T) {
	tsUTC := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}

	for _, tc := range []struct {
		dbType string
		want   arrow.DataType
	}{
		{
			dbType: `ROW("X" INTEGER, "Y" VARCHAR)`,
			want: arrow.StructOf(
				arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				arrow.Field{Name: "y", Type: arrow.BinaryTypes.String, Nullable: true},
			),
		},
		{
			dbType: `ROW(INTEGER, VARCHAR(1))`,
			want: arrow.StructOf(
				arrow.Field{Name: "field0", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				arrow.Field{Name: "field1", Type: arrow.BinaryTypes.String, Nullable: true},
			),
		},
		{
			dbType: `MAP(VARCHAR, BIGINT)`,
			want: arrow.MapOfFields(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String},
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			),
		},
		{
			// date keys cannot be recovered from the JSON protocol losslessly, so they stay strings
			dbType: `MAP(DATE, DOUBLE)`,
			want: arrow.MapOfFields(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String},
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			),
		},
		{
			dbType: `ARRAY(ROW("ID" BIGINT, "TAGS" ARRAY(VARCHAR)))`,
			want: arrow.ListOfField(arrow.Field{Name: "item", Nullable: true, Type: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
			)}),
		},
		{
			dbType: `ROW("D" DECIMAL(4, 2), "TS" TIMESTAMP(3) WITH TIME ZONE, "U" UUID, "IV" INTERVAL DAY TO SECOND, "F" REAL)`,
			want: arrow.StructOf(
				arrow.Field{Name: "d", Type: &arrow.Decimal32Type{Precision: 4, Scale: 2}, Nullable: true},
				arrow.Field{Name: "ts", Type: tsUTC, Nullable: true},
				arrow.Field{Name: "u", Type: extensions.NewUUIDType(), Nullable: true},
				arrow.Field{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
				arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			),
		},
	} {
		t.Run(tc.dbType, func(t *testing.T) {
			got, nullable, meta, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
				Name:             "col",
				DatabaseTypeName: tc.dbType,
			})
			require.NoError(t, err)
			assert.True(t, nullable)
			assert.Truef(t, arrow.TypeEqual(tc.want, got), "want %s\ngot  %s", tc.want, got)
			dbTypeName, _ := meta.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
			assert.Equal(t, tc.dbType, dbTypeName)
		})
	}

	t.Run("types with type parameters become leaves named after the type", func(t *testing.T) {
		got, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
			Name:             "col",
			DatabaseTypeName: "ROW(INTEGER, QDIGEST(BIGINT))",
		})
		require.NoError(t, err)
		st, ok := got.(*arrow.StructType)
		require.Truef(t, ok, "expected a struct, got %s", got)
		require.Equal(t, 2, st.NumFields())
		assert.Equal(t, arrow.PrimitiveTypes.Int32, st.Field(0).Type)
		// QDIGEST has no Arrow equivalent; it converts like a top-level QDIGEST column
		assert.True(t, arrow.TypeEqual(extensions.NewOpaqueType(arrow.BinaryTypes.String, "QDIGEST", vendorName), st.Field(1).Type),
			"got %s", st.Field(1).Type)
	})

	t.Run("unparsable type falls back to the default conversion", func(t *testing.T) {
		got, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
			Name:             "col",
			DatabaseTypeName: "ROW(",
		})
		require.NoError(t, err)
		assert.NotNil(t, got)
		_, isStruct := got.(*arrow.StructType)
		assert.False(t, isStruct)
	})
}

func TestMapInserterKeyConversionAndOrder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name:             "m",
		DatabaseTypeName: "MAP(INTEGER, VARCHAR)",
	})
	require.NoError(t, err)
	field := arrow.Field{Name: "m", Type: dt, Nullable: true}

	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()
	ins, err := typeConverter.CreateInserter(&field, bldr)
	require.NoError(t, err)

	require.NoError(t, ins.AppendValue(map[string]any{"10": "ten", "2": "two", "-1": nil}))
	require.NoError(t, ins.AppendValue(nil))
	require.NoError(t, ins.AppendValue(map[string]any{}))
	assert.Error(t, ins.AppendValue(map[string]any{"not a number": "x"}))
	assert.Error(t, ins.AppendValue("not a map"))

	arr := bldr.NewArray().(*array.Map)
	defer arr.Release()

	// invalid keys and non-map values are rejected before anything is appended
	require.Equal(t, 3, arr.Len())
	assert.Equal(t, `[{[-1 2 10] [(null) "two" "ten"]} (null) {[] []}]`, arr.String())
}

func TestMapInserterKeyTypes(t *testing.T) {
	for _, tc := range []struct {
		dbType string
		value  map[string]any
		want   string
	}{
		{"MAP(DECIMAL(4, 1), INTEGER)", map[string]any{"10.5": int64(1), "2.0": int64(2), "-1.0": int64(3)}, `[{[-1 2 10.5] [3 2 1]}]`},
		// the formats Trino sends double keys in; NaN sorts last, as in Trino
		{"MAP(DOUBLE, INTEGER)", map[string]any{"NaN": int64(1), "-Infinity": int64(2), "1.0E10": int64(3), "1.0E-5": int64(4), "-1.5": int64(5)}, `[{[-Inf -1.5 1e-05 1e+10 NaN] [2 5 4 3 1]}]`},
		{"MAP(REAL, INTEGER)", map[string]any{"1.0E10": int64(1), "0.1": int64(2)}, `[{[0.1 1e+10] [2 1]}]`},
		{"MAP(BOOLEAN, INTEGER)", map[string]any{"true": int64(1), "false": int64(2)}, `[{[false true] [2 1]}]`},
		{"MAP(TINYINT, INTEGER)", map[string]any{"-5": int64(1), "7": int64(2)}, `[{[-5 7] [1 2]}]`},
		{"MAP(DATE, INTEGER)", map[string]any{"2024-01-02": int64(1), "2023-12-31": int64(2)}, `[{["2023-12-31" "2024-01-02"] [2 1]}]`},
	} {
		t.Run(tc.dbType, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "m", DatabaseTypeName: tc.dbType})
			require.NoError(t, err)
			field := arrow.Field{Name: "m", Type: dt, Nullable: true}
			bldr := array.NewBuilder(mem, dt)
			defer bldr.Release()
			ins, err := typeConverter.CreateInserter(&field, bldr)
			require.NoError(t, err)

			require.NoError(t, ins.AppendValue(tc.value))
			arr := bldr.NewArray()
			defer arr.Release()
			assert.Equal(t, tc.want, arr.String())
		})
	}
}

func TestMapKeyFallbackMetadata(t *testing.T) {
	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name:             "m",
		DatabaseTypeName: "MAP(TIMESTAMP(3), INTEGER)",
	})
	require.NoError(t, err)
	key := dt.(*arrow.MapType).KeyField()
	assert.Equal(t, arrow.BinaryTypes.String, key.Type)
	assert.False(t, key.Nullable)
	assert.Equal(t, arrow.MetadataFrom(map[string]string{
		sqlwrapper.MetaKeyDatabaseTypeName: "TIMESTAMP(3)",
		sqlwrapper.MetaKeyColumnName:       "key",
	}), key.Metadata)
}

func TestNestedIntervalMetadata(t *testing.T) {
	// a nested interval reports the fractional second precision like a
	// top-level column: from the qualifier if it ends in SECOND, else 0
	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name:             "r",
		DatabaseTypeName: "ROW(A INTERVAL DAY(2), B INTERVAL HOUR(2) TO MINUTE, C INTERVAL SECOND(2, 6), D INTERVAL DAY(2) TO SECOND(3), E INTERVAL YEAR(2) TO MONTH, F INTERVAL DAY TO SECOND)",
	})
	require.NoError(t, err)
	st := dt.(*arrow.StructType)
	for _, tc := range []struct {
		field     string
		dbType    string
		fractions string // empty: no fractional seconds precision key
	}{
		{"a", "INTERVAL DAY(2)", "0"},
		{"b", "INTERVAL HOUR(2) TO MINUTE", "0"},
		{"c", "INTERVAL SECOND(2, 6)", "6"},
		{"d", "INTERVAL DAY(2) TO SECOND(3)", "3"},
		{"e", "INTERVAL YEAR(2) TO MONTH", ""},
		{"f", "INTERVAL DAY TO SECOND", ""},
	} {
		f, ok := st.FieldByName(tc.field)
		require.True(t, ok, tc.field)
		assert.Equal(t, arrow.FixedWidthTypes.MonthDayNanoInterval, f.Type, tc.field)
		dbType, _ := f.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
		assert.Equal(t, tc.dbType, dbType, tc.field)
		fractions, ok := f.Metadata.GetValue(sqlwrapper.MetaKeyFractionalSecondsPrecision)
		if tc.fractions == "" {
			assert.False(t, ok, "%s: unexpected fractional seconds precision %q", tc.field, fractions)
		} else {
			assert.Equal(t, tc.fractions, fractions, tc.field)
		}
	}
}

func TestNestedVarcharLength(t *testing.T) {
	// unbounded VARCHAR reports the maximum length, like a top-level column
	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name:             "r",
		DatabaseTypeName: "ROW(A VARCHAR, B VARCHAR(5))",
	})
	require.NoError(t, err)
	st := dt.(*arrow.StructType)
	length, _ := st.Field(0).Metadata.GetValue(sqlwrapper.MetaKeyLength)
	assert.Equal(t, "2147483647", length)
	length, _ = st.Field(1).Metadata.GetValue(sqlwrapper.MetaKeyLength)
	assert.Equal(t, "5", length)
}

// FuzzConvertNestedColumnType checks that any ARRAY, MAP or ROW type text
// converts without panicking or failing: types that cannot be parsed fall
// back to the default conversion. The seeds also run in plain `go test`.
func FuzzConvertNestedColumnType(f *testing.F) {
	for _, seed := range []string{
		`ROW(X INTEGER, "MY FIELD" ARRAY(VARCHAR(3)), TIMESTAMP TIMESTAMP(3) WITH TIME ZONE)`,
		`MAP(VARCHAR, ROW(N INTEGER, INNER_MAP MAP(DECIMAL(10, 2), ROW(FLAG BOOLEAN))))`,
		`ARRAY(ROW(INTEGER, INTERVAL DAY(2) TO SECOND(6), QDIGEST(BIGINT)))`,
		`ROW("A""B" DOUBLE, "C,D" UUID)`,
		`ARRAY(MAP)`,
		`MAP(VARCHAR, ARRAY)`,
		`ROW(AR(1)RAY)`,
		`ROW(`,
		`MAP()`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, typeName string) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)
		for _, prefix := range []string{"", "ARRAY(", "MAP(VARCHAR, ", "ROW(X "} {
			text := prefix + typeName
			if prefix != "" {
				text += ")"
			}
			if !isNestedTypeName(text) {
				continue
			}
			dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: text})
			if err != nil {
				t.Fatalf("%q: %v", text, err)
			}
			field := arrow.Field{Name: "c", Type: dt, Nullable: true}
			bldr := array.NewBuilder(mem, dt)
			ins, err := typeConverter.CreateInserter(&field, bldr)
			if err != nil {
				bldr.Release()
				t.Fatalf("%q: %v", text, err)
			}
			if err := ins.AppendValue(nil); err != nil {
				bldr.Release()
				t.Fatalf("%q: %v", text, err)
			}
			bldr.NewArray().Release()
			bldr.Release()
		}
	})
}

func TestTimestampOutOfRange(t *testing.T) {
	// 9999-12-31 cannot be represented in nanoseconds (years 1677-2262 only):
	// appending it must return an error instead of panicking
	endOfTime := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	for _, dbType := range []string{
		"ROW(TS TIMESTAMP(9))",
		"ROW(TS TIMESTAMP(9) WITH TIME ZONE)",
		"MAP(VARCHAR, TIMESTAMP(12))",
		"ARRAY(ARRAY(ARRAY(ARRAY(TIMESTAMP(9) WITH TIME ZONE))))",
	} {
		t.Run(dbType, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: dbType})
			require.NoError(t, err)
			field := arrow.Field{Name: "c", Type: dt, Nullable: true}
			bldr := array.NewBuilder(mem, dt)
			defer bldr.Release()
			ins, err := typeConverter.CreateInserter(&field, bldr)
			require.NoError(t, err)

			var value any
			switch {
			case strings.HasPrefix(dbType, "ROW"):
				value = fakeRow{endOfTime}
			case strings.HasPrefix(dbType, "MAP"):
				value = map[string]any{"k": endOfTime}
			default:
				value = []any{[]any{[]any{[]any{endOfTime}}}}
			}
			err = ins.AppendValue(value)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot be represented in ns precision")
		})
	}

	t.Run("microseconds cover it", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)
		dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: "ROW(TS TIMESTAMP(6), TSTZ TIMESTAMP(6) WITH TIME ZONE)"})
		require.NoError(t, err)
		field := arrow.Field{Name: "c", Type: dt, Nullable: true}
		bldr := array.NewBuilder(mem, dt)
		defer bldr.Release()
		ins, err := typeConverter.CreateInserter(&field, bldr)
		require.NoError(t, err)
		require.NoError(t, ins.AppendValue(fakeRow{endOfTime, endOfTime}))
		arr := bldr.NewArray()
		defer arr.Release()
		assert.Contains(t, arr.String(), "253402300799999999")
	})
}

// deeplyNested returns a type nested depth levels deep around a BIGINT,
// cycling through the containers in kinds ('A' ARRAY, 'R' ROW, 'M' MAP), and
// a matching value holding 42.
func deeplyNested(depth int, kinds string) (typeText string, value any) {
	typeText, value = "BIGINT", int64(42)
	for i := range depth {
		switch kinds[i%len(kinds)] {
		case 'A':
			typeText, value = "ARRAY("+typeText+")", []any{value}
		case 'R':
			typeText, value = "ROW(F"+strconv.Itoa(i)+" "+typeText+")", fakeRow{value}
		case 'M':
			typeText, value = "MAP(VARCHAR, "+typeText+")", map[string]any{"k": value}
		}
	}
	return typeText, value
}

// leafOfNested walks down a nested array with one value per level and
// returns how many levels it passed and the innermost array.
func leafOfNested(t *testing.T, arr arrow.Array) (int, arrow.Array) {
	depth := 0
	for {
		require.Equal(t, 1, arr.Len(), "level %d", depth)
		switch a := arr.(type) {
		case *array.Map: // before *array.List, which it embeds
			arr = a.Items()
		case *array.List:
			arr = a.ListValues()
		case *array.Struct:
			arr = a.Field(0)
		default:
			return depth, arr
		}
		depth++
	}
}

func TestDeeplyNested(t *testing.T) {
	// nothing in the conversion depends on the depth, so a type nested far
	// deeper than real schemas must convert and round-trip its value. Maps
	// are kept to 10 levels: building nested map arrays in arrow-go takes
	// time exponential in the map depth (about 1 s at 20 levels).
	for _, tc := range []struct {
		depth int
		kinds string
	}{
		{200, "AR"},
		{30, "ARM"},
	} {
		t.Run(fmt.Sprintf("%d levels of %s", tc.depth, tc.kinds), func(t *testing.T) {
			testDeeplyNested(t, tc.depth, tc.kinds)
		})
	}
}

func testDeeplyNested(t *testing.T, depth int, kinds string) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typeText, value := deeplyNested(depth, kinds)
	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{Name: "c", DatabaseTypeName: typeText})
	require.NoError(t, err)
	_, isOpaque := dt.(*extensions.OpaqueType)
	require.False(t, isOpaque, "fell back to the opaque type")

	field := arrow.Field{Name: "c", Type: dt, Nullable: true}
	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()
	ins, err := typeConverter.CreateInserter(&field, bldr)
	require.NoError(t, err)
	require.NoError(t, ins.AppendValue(value))

	arr := bldr.NewArray()
	defer arr.Release()
	levels, leaf := leafOfNested(t, arr)
	assert.Equal(t, depth, levels)
	require.IsType(t, &array.Int64{}, leaf)
	assert.Equal(t, int64(42), leaf.(*array.Int64).Value(0))
}

// fakeRow stands in for trino.Row, which has no exported constructor.
type fakeRow []any

func (r fakeRow) Len() int          { return len(r) }
func (r fakeRow) Name(i int) string { return "field" + strconv.Itoa(i) }
func (r fakeRow) Value(i int) any   { return r[i] }

func TestStructInserter(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt, _, _, err := typeConverter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name:             "r",
		DatabaseTypeName: `ROW("A" BIGINT, "B" ARRAY(VARCHAR))`,
	})
	require.NoError(t, err)
	field := arrow.Field{Name: "r", Type: dt, Nullable: true}

	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()
	ins, err := typeConverter.CreateInserter(&field, bldr)
	require.NoError(t, err)

	require.NoError(t, ins.AppendValue(fakeRow{int64(1), []any{"x", nil}}))
	require.NoError(t, ins.AppendValue(nil))
	require.NoError(t, ins.AppendValue(fakeRow{nil, nil}))
	// these errors are detected before anything is appended
	assert.Error(t, ins.AppendValue(fakeRow{int64(1)}))
	assert.Error(t, ins.AppendValue([]any{int64(1), nil}))
	assert.Error(t, ins.AppendValue("not a row"))
	// the zero trino.Row is a NULL ROW
	require.NoError(t, ins.AppendValue(trino.Row{}))

	arr := bldr.NewArray()
	defer arr.Release()
	require.Equal(t, 4, arr.Len())
	assert.Equal(t, `{[1 (null) (null) (null)] [["x" (null)] (null) (null) (null)]}`, arr.String())
	assert.True(t, arr.IsNull(1))
	assert.False(t, arr.IsNull(2))
	assert.True(t, arr.IsNull(3))
}
