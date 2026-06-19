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
	"reflect"
	"testing"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	trinoClient "github.com/trinodb/trino-go-client/trino"
)

func newTestTypeConverter() *trinoTypeConverter {
	return &trinoTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "Trino"},
	}
}

func TestScanTypeToListType(t *testing.T) {
	tc := newTestTypeConverter()

	tests := []struct {
		name     string
		scanType reflect.Type
		want     arrow.DataType
	}{
		{"NullSliceString", reflect.TypeFor[trinoClient.NullSliceString](), arrow.ListOf(arrow.BinaryTypes.String)},
		{"NullSliceInt64", reflect.TypeFor[trinoClient.NullSliceInt64](), arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{"NullSliceFloat64", reflect.TypeFor[trinoClient.NullSliceFloat64](), arrow.ListOf(arrow.PrimitiveTypes.Float64)},
		{"NullSliceBool", reflect.TypeFor[trinoClient.NullSliceBool](), arrow.ListOf(arrow.FixedWidthTypes.Boolean)},
		{"NullSlice2String", reflect.TypeFor[trinoClient.NullSlice2String](), arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))},
		{"NullSlice3Int64", reflect.TypeFor[trinoClient.NullSlice3Int64](), arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)))},
		{"non-array type returns nil", reflect.TypeFor[int](), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tc.scanTypeToListType(tt.scanType)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.True(t, arrow.TypeEqual(tt.want, got), "want %s, got %s", tt.want, got)
			}
		})
	}
}

func TestConvertRawColumnType_Array(t *testing.T) {
	tc := newTestTypeConverter()

	tests := []struct {
		name     string
		colType  sqlwrapper.ColumnType
		wantType arrow.DataType
	}{
		{
			name: "ARRAY(VARCHAR) via ScanType",
			colType: sqlwrapper.ColumnType{
				Name:             "arr_col",
				DatabaseTypeName: "ARRAY(VARCHAR(1))",
				ScanType:         reflect.TypeFor[trinoClient.NullSliceString](),
				Nullable:         true,
			},
			wantType: arrow.ListOf(arrow.BinaryTypes.String),
		},
		{
			name: "ARRAY(INTEGER) via ScanType",
			colType: sqlwrapper.ColumnType{
				Name:             "int_arr",
				DatabaseTypeName: "ARRAY(INTEGER)",
				ScanType:         reflect.TypeFor[trinoClient.NullSliceInt64](),
				Nullable:         true,
			},
			wantType: arrow.ListOf(arrow.PrimitiveTypes.Int64),
		},
		{
			name: "ARRAY(ARRAY(INTEGER)) via ScanType",
			colType: sqlwrapper.ColumnType{
				Name:             "nested",
				DatabaseTypeName: "ARRAY(ARRAY(INTEGER))",
				ScanType:         reflect.TypeFor[trinoClient.NullSlice2Int64](),
				Nullable:         true,
			},
			wantType: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
		},
		{
			name: "VARCHAR unchanged",
			colType: sqlwrapper.ColumnType{
				Name:             "str_col",
				DatabaseTypeName: "VARCHAR",
				Nullable:         true,
			},
			wantType: arrow.BinaryTypes.String,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType, _, _, err := tc.ConvertRawColumnType(tt.colType)
			require.NoError(t, err)
			assert.True(t, arrow.TypeEqual(tt.wantType, arrowType), "want %s, got %s", tt.wantType, arrowType)
		})
	}
}

func TestListInserter_StringArray(t *testing.T) {
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer lb.Release()

	tc := newTestTypeConverter()
	elemField := arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true}
	childIns, err := tc.CreateInserter(&elemField, lb.ValueBuilder())
	require.NoError(t, err)

	ins := &listInserter{builder: lb, childInserter: childIns}

	require.NoError(t, ins.AppendValue([]any{"a", "b", "c"}))
	require.NoError(t, ins.AppendValue(nil))
	require.NoError(t, ins.AppendValue([]any{"x", nil, "z"}))

	arr := lb.NewListArray()
	defer arr.Release()

	assert.Equal(t, 3, arr.Len())
	assert.False(t, arr.IsNull(0))
	assert.True(t, arr.IsNull(1))
	assert.False(t, arr.IsNull(2))

	offsets := arr.Offsets()
	assert.Equal(t, 3, int(offsets[1]-offsets[0]))
	assert.Equal(t, 3, int(offsets[3]-offsets[2]))
}

func TestListInserter_IntArray(t *testing.T) {
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer lb.Release()

	tc := newTestTypeConverter()
	elemField := arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	childIns, err := tc.CreateInserter(&elemField, lb.ValueBuilder())
	require.NoError(t, err)

	ins := &listInserter{builder: lb, childInserter: childIns}

	require.NoError(t, ins.AppendValue([]any{float64(1), float64(2), float64(3)}))
	require.NoError(t, ins.AppendValue(nil))

	arr := lb.NewListArray()
	defer arr.Release()

	assert.Equal(t, 2, arr.Len())
	assert.False(t, arr.IsNull(0))
	assert.True(t, arr.IsNull(1))
}

func TestCreateInserter_ListType(t *testing.T) {
	mem := memory.NewGoAllocator()
	listType := arrow.ListOf(arrow.BinaryTypes.String)
	field := arrow.Field{Name: "arr", Type: listType, Nullable: true}
	lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer lb.Release()

	tc := newTestTypeConverter()
	ins, err := tc.CreateInserter(&field, lb)
	require.NoError(t, err)

	_, ok := ins.(*listInserter)
	assert.True(t, ok, "expected *listInserter, got %T", ins)
}
