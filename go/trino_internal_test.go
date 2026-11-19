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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseYearToMonth(t *testing.T) {
	// the text forms Trino sends, e.g. INTERVAL '-3' MONTH is "-0-3"
	for text, months := range map[string]int32{
		"2-6":  30,
		"0-0":  0,
		"0-3":  3,
		"-2-6": -30,
		"-1-0": -12,
		"-0-3": -3,
		// the limits of Trino's INTERVAL YEAR TO MONTH
		"178956970-7":  2147483647,
		"-178956970-8": -2147483648,
	} {
		got, err := parseYearToMonth(text)
		require.NoError(t, err, text)
		assert.Equal(t, arrow.MonthDayNanoInterval{Months: months}, got, text)
	}

	for _, text := range []string{"", "-", "2", "2-6-1", "--2-6", "a-6", "2-b"} {
		_, err := parseYearToMonth(text)
		assert.Error(t, err, text)
	}
}

func TestParseDayToSecond(t *testing.T) {
	const hour = int64(3_600_000_000_000)
	// the text forms Trino sends, e.g. INTERVAL '-2' HOUR is "-0 02:00:00"
	for text, want := range map[string]arrow.MonthDayNanoInterval{
		"0 00:00:00":          {},
		"2 00:00:00":          {Days: 2},
		"1 23:59:59.999":      {Days: 1, Nanoseconds: 23*hour + 59*60_000_000_000 + 59_999_000_000},
		"-0 02:00:00":         {Nanoseconds: -2 * hour},
		"-1 02:00:00":         {Days: -1, Nanoseconds: -2 * hour},
		"-1 02:03:04.567000":  {Days: -1, Nanoseconds: -(2*hour + 3*60_000_000_000 + 4_567_000_000)},
		"-0 00:00:00.5":       {Nanoseconds: -500_000_000},
		"0 00:00:00.12345678": {Nanoseconds: 123_456_780},
	} {
		got, err := parseDayToSecond(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, got, text)
	}

	for _, text := range []string{"", "-", "1", "1 02:00", "x 02:00:00", "1 02:xx:00", "--1 02:00:00", "1 -02:00:00", "-1 02:-03:00"} {
		_, err := parseDayToSecond(text)
		assert.Error(t, err, text)
	}
}
