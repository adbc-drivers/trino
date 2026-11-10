// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestTimestampSerializationPreservesInstant(t *testing.T) {
	for _, tc := range []struct {
		unit   arrow.TimeUnit
		values []arrow.Timestamp
	}{
		{arrow.Second, []arrow.Timestamp{-62135596800, 0, 1684158330, 253402300799}},
		{arrow.Millisecond, []arrow.Timestamp{-62135596800000, 0, 1684158330123, 253402300799999}},
		{arrow.Microsecond, []arrow.Timestamp{-62135596800000000, 0, 1684158330123456, 253402300799999999}},
		{arrow.Nanosecond, []arrow.Timestamp{math.MinInt64, 0, 1684158330123456789, math.MaxInt64}},
	} {
		for _, zone := range []string{"UTC", "Asia/Tokyo", "America/New_York", ""} {
			t.Run(tc.unit.String()+"/"+zone, func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				timestampType := &arrow.TimestampType{Unit: tc.unit, TimeZone: zone}
				builder := array.NewTimestampBuilder(alloc, timestampType)
				defer builder.Release()
				builder.AppendValues(tc.values, nil)
				builder.AppendNull()
				values := builder.NewTimestampArray()
				defer values.Release()
				field := arrow.Field{Name: "value", Type: timestampType, Nullable: true}

				for i, value := range tc.values {
					converted, err := typeConverter.ConvertArrowToGo(values, i, &field)
					require.NoError(t, err)
					literal, err := trino.Serial(converted)
					require.NoError(t, err)
					// Parse the emitted literal to check the instant and precision
					// that Trino receives, including dates with historical offsets.
					parsed, err := time.Parse("TIMESTAMP '2006-01-02 15:04:05.999999999 Z07:00'", literal)
					require.NoError(t, err)
					assert.True(t, parsed.Equal(value.ToTime(tc.unit)), "timestamp %d serialized as %s", value, literal)
				}

				converted, err := typeConverter.ConvertArrowToGo(values, len(tc.values), &field)
				require.NoError(t, err)
				assert.Nil(t, converted)
			})
		}
	}
}
