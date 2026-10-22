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
	"database/sql"
	"reflect"
	"strconv"
	"testing"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestIntervalConversion(t *testing.T) {
	tests := []struct {
		typeName  string
		precision int64
		scale     int64
		value     string
		expected  arrow.MonthDayNanoInterval
	}{
		{"INTERVAL YEAR TO MONTH", -1, -1, "2-6", arrow.MonthDayNanoInterval{Months: 30}},
		{"INTERVAL DAY TO SECOND", -1, -1, "1 00:00:00.123", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 123000000}},
		{"INTERVAL YEAR(3) TO MONTH", 3, 0, "123-4", arrow.MonthDayNanoInterval{Months: 1480}},
		{"INTERVAL YEAR(3)", 3, 0, "123-0", arrow.MonthDayNanoInterval{Months: 1476}},
		{"INTERVAL MONTH(3)", 3, 0, "10-3", arrow.MonthDayNanoInterval{Months: 123}},
		{"INTERVAL DAY(3)", 3, 0, "123 00:00:00", arrow.MonthDayNanoInterval{Days: 123}},
		{"INTERVAL DAY(3) TO HOUR", 3, 0, "123 01:00:00", arrow.MonthDayNanoInterval{Days: 123, Nanoseconds: 3600000000000}},
		{"INTERVAL DAY(3) TO MINUTE", 3, 0, "123 01:02:00", arrow.MonthDayNanoInterval{Days: 123, Nanoseconds: 3720000000000}},
		{"INTERVAL DAY(2) TO SECOND(0)", 2, 0, "1 00:00:01", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 1000000000}},
		{"INTERVAL DAY(2) TO SECOND(6)", 2, 6, "1 00:00:00.123456", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 123456000}},
		{"INTERVAL DAY(2) TO SECOND(9)", 2, 9, "1 00:00:00.123456789", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 123456789}},
		{"INTERVAL DAY(2) TO SECOND(12)", 2, 12, "1 00:00:00.123456789999", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 123456789}},
		{"INTERVAL HOUR(3)", 3, 0, "5 03:00:00", arrow.MonthDayNanoInterval{Days: 5, Nanoseconds: 10800000000000}},
		{"INTERVAL HOUR(3) TO MINUTE", 3, 0, "5 03:04:00", arrow.MonthDayNanoInterval{Days: 5, Nanoseconds: 11040000000000}},
		{"INTERVAL HOUR(3) TO SECOND(6)", 3, 6, "5 03:04:05.123456", arrow.MonthDayNanoInterval{Days: 5, Nanoseconds: 11045123456000}},
		{"INTERVAL MINUTE(3)", 3, 0, "0 02:03:00", arrow.MonthDayNanoInterval{Nanoseconds: 7380000000000}},
		{"INTERVAL MINUTE(3) TO SECOND(6)", 3, 6, "0 02:03:04.123456", arrow.MonthDayNanoInterval{Nanoseconds: 7384123456000}},
		{"INTERVAL SECOND(3, 0)", 3, 0, "0 00:02:03", arrow.MonthDayNanoInterval{Nanoseconds: 123000000000}},
		{"INTERVAL SECOND(3, 12)", 3, 12, "0 00:02:03.123456789999", arrow.MonthDayNanoInterval{Nanoseconds: 123123456789}},
		{"interval day(2) to second(6)", 2, 6, "1 00:00:00.123456", arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: 123456000}},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			col := sqlwrapper.ColumnType{
				Name:             "res",
				DatabaseTypeName: tt.typeName,
				ScanType:         reflect.TypeFor[sql.NullString](),
			}
			if tt.precision >= 0 {
				col.Precision = &tt.precision
				col.Scale = &tt.scale
			}
			dtype, nullable, metadata, err := typeConverter.ConvertRawColumnType(col)
			require.NoError(t, err)
			require.Equal(t, arrow.FixedWidthTypes.MonthDayNanoInterval, dtype)
			require.True(t, nullable)
			name, ok := metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
			require.True(t, ok)
			require.Equal(t, tt.typeName, name)
			fractional, ok := metadata.GetValue(sqlwrapper.MetaKeyFractionalSecondsPrecision)
			if tt.scale >= 0 && tt.expected.Months == 0 {
				require.True(t, ok)
				require.Equal(t, strconv.FormatInt(tt.scale, 10), fractional)
			} else {
				require.False(t, ok)
			}

			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			builder := array.NewMonthDayNanoIntervalBuilder(mem)
			defer builder.Release()
			field := arrow.Field{Name: "res", Type: dtype, Nullable: nullable, Metadata: metadata}
			inserter, err := typeConverter.CreateInserter(&field, builder)
			require.NoError(t, err)
			require.NoError(t, inserter.AppendValue(sql.NullString{String: tt.value, Valid: true}))
			require.NoError(t, inserter.AppendValue(sql.NullString{}))
			values := builder.NewMonthDayNanoIntervalArray()
			defer values.Release()
			require.Equal(t, 2, values.Len())
			require.Equal(t, tt.expected, values.Value(0))
			require.True(t, values.IsNull(1))
		})
	}
}
