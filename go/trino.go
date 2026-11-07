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
	"database/sql/driver"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	// register the "trino" driver with database/sql
	"github.com/trinodb/trino-go-client/trino"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// TrinoTypeConverter provides Trino-specific type conversion enhancements
type trinoTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

const vendorName = "Trino"

var typeConverter = &trinoTypeConverter{
	VendorName: vendorName,
}

var intervalPrecisionPattern = regexp.MustCompile(`\([0-9]+(?:,\s*[0-9]+)?\)`)

// intervalTypeFamily recognizes both legacy and parametric interval qualifiers.
// Keep the original database type name in field metadata, including precision.
func intervalTypeFamily(typeName string) string {
	typeName = strings.ToUpper(typeName)
	if !strings.HasPrefix(typeName, "INTERVAL ") {
		return typeName
	}
	qualifier := intervalPrecisionPattern.ReplaceAllString(typeName, "")
	switch qualifier {
	case "INTERVAL YEAR", "INTERVAL MONTH", "INTERVAL YEAR TO MONTH":
		return "INTERVAL YEAR TO MONTH"
	case "INTERVAL DAY", "INTERVAL HOUR", "INTERVAL MINUTE", "INTERVAL SECOND",
		"INTERVAL DAY TO HOUR", "INTERVAL DAY TO MINUTE", "INTERVAL DAY TO SECOND",
		"INTERVAL HOUR TO MINUTE", "INTERVAL HOUR TO SECOND", "INTERVAL MINUTE TO SECOND":
		return "INTERVAL DAY TO SECOND"
	default:
		return typeName
	}
}

// ConvertRawColumnType implements TypeConverter with Trino-specific enhancements
func (m *trinoTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	// Handle Trino-specific type mappings first
	typeName := intervalTypeFamily(colType.DatabaseTypeName)

	// trino go client does not provide clean way to get nullability so assume every column is always nullable
	colType.Nullable = true

	switch typeName {
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE":
		// Try to get precision from Precision field (which represents fractional seconds precision)
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			// precision represents fractional seconds digits (0-9)
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			timeUnit := convertPrecisionToTimeUnit(precision)
			timestampType = &arrow.TimestampType{Unit: timeUnit}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond}
		}

		if typeName == "TIMESTAMP WITH TIME ZONE" {
			timestampType.(*arrow.TimestampType).TimeZone = "UTC"
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil
	case "REAL":
		// Trino uses REAL for float32/single precision
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		})
		return arrow.PrimitiveTypes.Float32, colType.Nullable, metadata, nil
	case "INTERVAL YEAR TO MONTH":
		// Trino's INTERVAL YEAR TO MONTH is returned as a string (e.g., "2-6" for 2 years 6 months)
		// Map to Arrow's MonthDayNanoInterval (with days=0, nanoseconds=0) since PyArrow doesn't have year-month interval support
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.FixedWidthTypes.MonthDayNanoInterval, colType.Nullable, metadata, nil
	case "INTERVAL DAY TO SECOND":
		// Trino's INTERVAL DAY TO SECOND is returned as a string (e.g., "1 23:59:59.999" for 1 day 23:59:59.999)
		// Map to Arrow's MonthDayNanoInterval (with months=0)
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Scale != nil {
			// The client reports leading-field precision in Precision and
			// fractional-second precision in Scale for parametric intervals.
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", *colType.Scale)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.FixedWidthTypes.MonthDayNanoInterval, colType.Nullable, metadata, nil
	case "IPADDRESS":
		// Trino's IPADDRESS type stores IPv4 and IPv6 addresses
		// Returned as string representation (e.g., "192.168.1.1", "::1", "2001:db8::1")
		// Arrow doesn't have a native IP address type, so map to string
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil
	case "UUID":
		// Trino's UUID type represents universally unique identifiers (RFC 4122)
		// Returned as string representation in standard format (e.g., "12151fd2-7586-11e9-8f9e-2a86e4085a59")
		// Arrow doesn't have a native UUID type, so map to string
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return extensions.NewUUIDType(), colType.Nullable, metadata, nil
	}

	if listType := scanTypeToListMap[colType.ScanType]; listType != nil {
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		})
		return listType, colType.Nullable, metadata, nil
	}

	// ARRAY, MAP and ROW types the scan type does not cover, e.g. MAP(VARCHAR,
	// BIGINT), ROW(...) or ARRAY(ROW(...)), are converted from their type
	// signature. If it cannot be parsed, fall back to the default conversion.
	if isNestedTypeName(colType.DatabaseTypeName) {
		if parsed, err := parseTrinoType(colType.DatabaseTypeName); err == nil {
			if field, err := m.nestedArrowField(colType.Name, parsed); err == nil {
				return field.Type, colType.Nullable, field.Metadata, nil
			}
		}
	}

	// For all other types, fall back to default conversion
	return m.DefaultTypeConverter.ConvertRawColumnType(colType)
}

// Integer arrays keep their int64 Arrow representation even when the client
// reports sql.NullInt32 for TINYINT, SMALLINT and INTEGER elements.
var scanTypeToListMap = map[reflect.Type]arrow.DataType{
	reflect.TypeFor[trino.NullSlice[sql.NullString]]():                                    arrow.ListOf(arrow.BinaryTypes.String),
	reflect.TypeFor[trino.NullSlice[sql.NullInt32]]():                                     arrow.ListOf(arrow.PrimitiveTypes.Int64),
	reflect.TypeFor[trino.NullSlice[sql.NullInt64]]():                                     arrow.ListOf(arrow.PrimitiveTypes.Int64),
	reflect.TypeFor[trino.NullSlice[sql.NullFloat64]]():                                   arrow.ListOf(arrow.PrimitiveTypes.Float64),
	reflect.TypeFor[trino.NullSlice[sql.NullBool]]():                                      arrow.ListOf(arrow.FixedWidthTypes.Boolean),
	reflect.TypeFor[trino.NullSlice[sql.NullTime]]():                                      arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}),
	reflect.TypeFor[trino.NullSlice[[]byte]]():                                            arrow.ListOf(arrow.BinaryTypes.Binary),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullString]]]():                   arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullInt32]]]():                    arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullInt64]]]():                    arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullFloat64]]]():                  arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Float64)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullBool]]]():                     arrow.ListOf(arrow.ListOf(arrow.FixedWidthTypes.Boolean)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[sql.NullTime]]]():                     arrow.ListOf(arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[[]byte]]]():                           arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.Binary)),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullString]]]]():  arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullInt32]]]]():   arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullInt64]]]]():   arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullFloat64]]]](): arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Float64))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullBool]]]]():    arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.FixedWidthTypes.Boolean))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[sql.NullTime]]]]():    arrow.ListOf(arrow.ListOf(arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}))),
	reflect.TypeFor[trino.NullSlice[trino.NullSlice[trino.NullSlice[[]byte]]]]():          arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.Binary))),
}

func convertPrecisionToTimeUnit(precision int64) arrow.TimeUnit {
	switch {
	case precision == 0:
		return arrow.Second
	case precision >= 1 && precision <= 3:
		return arrow.Millisecond
	case precision >= 4 && precision <= 6:
		return arrow.Microsecond
	case precision >= 7:
		return arrow.Nanosecond
	default:
		return arrow.Microsecond
	}
}

// CreateInserter creates Trino-specific inserters bound to builders for enhanced performance
func (m *trinoTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for Trino-specific types first
	switch fieldType := field.Type.(type) {
	case *arrow.TimestampType:
		return &trinoTimestampInserter{
			builder: builder.(*array.TimestampBuilder),
			unit:    fieldType.Unit,
			zoned:   fieldType.TimeZone != "",
		}, nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		// Use custom binary inserter that handles base64 decoding from ConvertArrowToGo
		return &trinoBinaryInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
	case *arrow.Date32Type:
		return &date32Inserter{builder: builder.(*array.Date32Builder)}, nil
	case *arrow.MonthDayNanoIntervalType:
		// Interval types require custom inserter to parse Trino interval strings
		dbTypeName, exists := field.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
		if !exists {
			return nil, fmt.Errorf("no database type name in field metadata for interval type")
		}
		switch intervalTypeFamily(dbTypeName) {
		case "INTERVAL YEAR TO MONTH":
			return &yearToMonthIntervalInserter{
				builder: builder.(*array.MonthDayNanoIntervalBuilder),
			}, nil
		case "INTERVAL DAY TO SECOND":
			return &dayToSecondIntervalInserter{
				builder: builder.(*array.MonthDayNanoIntervalBuilder),
			}, nil
		default:
			return nil, fmt.Errorf("unsupported interval type: %s", dbTypeName)
		}
	case *arrow.ListType:
		lb := builder.(*array.ListBuilder)
		// The element field keeps its metadata, which inserters for nested
		// types like intervals read the database type name from.
		elemField := fieldType.ElemField()
		childIns, err := m.CreateInserter(&elemField, lb.ValueBuilder())
		if err != nil {
			return nil, fmt.Errorf("failed to create child inserter for list: %w", err)
		}
		return &listInserter{builder: lb, childInserter: childIns}, nil
	case *arrow.StructType:
		sb := builder.(*array.StructBuilder)
		children := make([]sqlwrapper.Inserter, fieldType.NumFields())
		for i := range children {
			childField := fieldType.Field(i)
			childIns, err := m.CreateInserter(&childField, sb.FieldBuilder(i))
			if err != nil {
				return nil, fmt.Errorf("failed to create inserter for struct field %q: %w", childField.Name, err)
			}
			children[i] = childIns
		}
		return &structInserter{builder: sb, children: children}, nil
	case *arrow.MapType:
		mb := builder.(*array.MapBuilder)
		keyField, itemField := fieldType.KeyField(), fieldType.ItemField()
		keyIns, err := m.CreateInserter(&keyField, mb.KeyBuilder())
		if err != nil {
			return nil, fmt.Errorf("failed to create inserter for map keys: %w", err)
		}
		valueIns, err := m.CreateInserter(&itemField, mb.ItemBuilder())
		if err != nil {
			return nil, fmt.Errorf("failed to create inserter for map values: %w", err)
		}
		return &mapInserter{builder: mb, keyType: keyField.Type, keyIns: keyIns, valueIns: valueIns}, nil
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

type listInserter struct {
	builder       *array.ListBuilder
	childInserter sqlwrapper.Inserter
}

func (ins *listInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	slice, ok := sqlValue.([]any)
	if !ok {
		return fmt.Errorf("expected []interface{} for list type, got %T", sqlValue)
	}
	ins.builder.Append(true)
	for _, elem := range slice {
		if err := ins.childInserter.AppendValue(elem); err != nil {
			return err
		}
	}
	return nil
}

func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// trinoTimestampInserter handles TIMESTAMP, keeping naive values as
// wall-clock time, and TIMESTAMP WITH TIME ZONE (zoned)
type trinoTimestampInserter struct {
	builder *array.TimestampBuilder
	unit    arrow.TimeUnit
	zoned   bool
}

func (ins *trinoTimestampInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := unwrapped.(time.Time)
	if !ok {
		return fmt.Errorf("expected time.Time for timestamp, got %T", sqlValue)
	}

	layout := time.RFC3339Nano
	if !ins.zoned {
		// Keep a naive timestamp's wall-clock time: create a time in UTC with
		// the same wall-clock values, to prevent time zone conversion.
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
		layout = "2006-01-02 15:04:05.999999999"
	}
	// TimestampBuilder.AppendTime would panic for times the unit cannot
	// represent, like 9999-12-31 in nanoseconds (which cover 1677 to 2262).
	ts, err := arrow.TimestampFromTime(t, ins.unit)
	if err != nil {
		cast := "TIMESTAMP(6)"
		if ins.zoned {
			cast += " WITH TIME ZONE"
		}
		return fmt.Errorf("timestamp %s cannot be represented in %s precision; cast it to %s", t.Format(layout), ins.unit, cast)
	}
	ins.builder.Append(ts)
	return nil
}

// trinoBinaryInserter handles Trino's base64-encoded binary data
type trinoBinaryInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *trinoBinaryInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := unwrapped.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for trino binary inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

// Date inserters
type date32Inserter struct {
	builder *array.Date32Builder
}

func (ins *date32Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := unwrapped.(time.Time)
	if !ok {
		return fmt.Errorf("expected time.Time for date32 inserter, got %T", sqlValue)
	}

	// Convert to date without timezone conversion
	// Extract just the date components and calculate days since epoch manually
	year, month, day := t.Date()
	utcDate := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	val := arrow.Date32FromTime(utcDate)

	ins.builder.Append(val)
	return nil
}

// yearToMonthIntervalInserter handles INTERVAL YEAR TO MONTH values
type yearToMonthIntervalInserter struct {
	builder *array.MonthDayNanoIntervalBuilder
}

func (ins *yearToMonthIntervalInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	// Interval comes from Trino as a string
	intervalStr, ok := unwrapped.(string)
	if !ok {
		return fmt.Errorf("expected string for interval, got %T", sqlValue)
	}

	interval, err := parseYearToMonth(intervalStr)
	if err != nil {
		return fmt.Errorf("failed to parse YEAR TO MONTH interval '%s': %w", intervalStr, err)
	}

	ins.builder.Append(interval)
	return nil
}

// parseYearToMonth parses "2-6" format (2 years 6 months) to MonthDayNanoInterval.
// Trino writes negative intervals with one leading sign, e.g. "-2-6".
func parseYearToMonth(intervalStr string) (arrow.MonthDayNanoInterval, error) {
	negative, intervalStr := cutSign(intervalStr)
	parts := strings.Split(intervalStr, "-")
	if len(parts) != 2 {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid YEAR TO MONTH format, expected 'Y-M'")
	}

	years, err := strconv.Atoi(parts[0])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid years: %w", err)
	}

	months, err := strconv.Atoi(parts[1])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid months: %w", err)
	}

	totalMonths := int64(years)*12 + int64(months)
	if negative {
		totalMonths = -totalMonths
	}
	if totalMonths < math.MinInt32 || totalMonths > math.MaxInt32 {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("interval of %d months is out of range", totalMonths)
	}
	return arrow.MonthDayNanoInterval{
		Months:      int32(totalMonths),
		Days:        0,
		Nanoseconds: 0,
	}, nil
}

// dayToSecondIntervalInserter handles INTERVAL DAY TO SECOND values
type dayToSecondIntervalInserter struct {
	builder *array.MonthDayNanoIntervalBuilder
}

func (ins *dayToSecondIntervalInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	// Interval comes from Trino as a string
	intervalStr, ok := unwrapped.(string)
	if !ok {
		return fmt.Errorf("expected string for interval, got %T", sqlValue)
	}

	interval, err := parseDayToSecond(intervalStr)
	if err != nil {
		return fmt.Errorf("failed to parse DAY TO SECOND interval '%s': %w", intervalStr, err)
	}

	ins.builder.Append(interval)
	return nil
}

// parseDayToSecond parses "1 23:59:59.999" format to MonthDayNanoInterval.
// Trino writes negative intervals with one leading sign that applies to the
// whole value, e.g. "-1 02:00:00" is minus 1 day and 2 hours.
func parseDayToSecond(intervalStr string) (arrow.MonthDayNanoInterval, error) {
	negative, intervalStr := cutSign(intervalStr)
	if strings.Contains(intervalStr, "-") {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid DAY TO SECOND format, expected a single leading sign")
	}
	// Split "D HH:MM:SS.sss" format
	parts := strings.Fields(intervalStr)
	if len(parts) != 2 {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid DAY TO SECOND format, expected 'D HH:MM:SS.sss'")
	}

	days, err := strconv.Atoi(parts[0])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid days: %w", err)
	}

	// Parse time part HH:MM:SS.sss
	timeParts := strings.Split(parts[1], ":")
	if len(timeParts) != 3 {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid time format, expected 'HH:MM:SS.sss'")
	}

	hours, err := strconv.Atoi(timeParts[0])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid hours: %w", err)
	}

	minutes, err := strconv.Atoi(timeParts[1])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid minutes: %w", err)
	}

	// Parse seconds with fractional part
	secondsParts := strings.Split(timeParts[2], ".")
	seconds, err := strconv.Atoi(secondsParts[0])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid seconds: %w", err)
	}

	var nanos int64 = 0
	if len(secondsParts) > 1 {
		// Convert fractional seconds to nanoseconds
		fracStr := secondsParts[1]
		// Pad or truncate to 9 digits (nanoseconds). Trino 484+ supports
		// picoseconds, but Arrow's interval type only stores nanoseconds.
		for len(fracStr) < 9 {
			fracStr += "0"
		}
		if len(fracStr) > 9 {
			fracStr = fracStr[:9]
		}
		fracNanos, err := strconv.ParseInt(fracStr, 10, 64)
		if err != nil {
			return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid fractional seconds: %w", err)
		}
		nanos = fracNanos
	}

	// Convert time to total nanoseconds
	timeNanos := int64(hours*3600+minutes*60+seconds)*1_000_000_000 + nanos
	if negative {
		days, timeNanos = -days, -timeNanos
	}

	return arrow.MonthDayNanoInterval{
		Months:      0,
		Days:        int32(days),
		Nanoseconds: timeNanos,
	}, nil
}

// cutSign removes the leading minus Trino writes before a negative interval
// and reports whether there was one.
func cutSign(intervalStr string) (negative bool, rest string) {
	rest = strings.TrimSpace(intervalStr)
	if after, ok := strings.CutPrefix(rest, "-"); ok {
		return true, after
	}
	return false, rest
}

// ConvertArrowToGo implements Trino-specific Arrow value to Go value conversion
func (m *trinoTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle Trino-specific Arrow to Go conversions
	// Trino Go client has limited parameter type support, so convert unsupported types to strings
	switch a := arrowArray.(type) {
	case *array.Time32:
		// For Trino driver, convert Time32 to Trino's Time type (time without timezone)
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return trino.Time(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()), nil

	case *array.Time64:
		// For Trino driver, convert Time64 to Trino's Time type (time without timezone)
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return trino.Time(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()), nil

	case *array.Decimal32:
		// For Trino driver, convert Decimal32 to trino.Numeric
		decimalType := a.DataType().(*arrow.Decimal32Type)
		val := a.Value(index)
		return convertDecimalToTrinoNumericFromInt(big.NewInt(int64(val)), decimalType.Scale), nil

	case *array.Decimal64:
		// For Trino driver, convert Decimal64 to trino.Numeric
		decimalType := a.DataType().(*arrow.Decimal64Type)
		val := a.Value(index)
		return convertDecimalToTrinoNumericFromInt(big.NewInt(int64(val)), decimalType.Scale), nil

	case *array.Decimal128:
		// For Trino driver, convert Decimal128 to trino.Numeric
		decimalType := a.DataType().(*arrow.Decimal128Type)
		val := a.Value(index)
		return convertDecimalToTrinoNumericFromInt(val.BigInt(), decimalType.Scale), nil

	case *array.Decimal256:
		// For Trino driver, convert Decimal256 to trino.Numeric
		decimalType := a.DataType().(*arrow.Decimal256Type)
		val := a.Value(index)
		return convertDecimalToTrinoNumericFromInt(val.BigInt(), decimalType.Scale), nil

	case *array.Float16:
		// Trino Go client doesn't support float types - convert to string
		val := a.Value(index)
		return fmt.Sprintf("%g", val), nil

	case *array.Float32:
		// Trino Go client doesn't support float types - convert to string
		val := a.Value(index)
		return fmt.Sprintf("%g", val), nil

	case *array.Float64:
		// Trino Go client doesn't support float types - convert to string
		val := a.Value(index)
		return fmt.Sprintf("%g", val), nil

	case *array.FixedSizeBinary:
		// Check metadata for UUID extension type indication
		if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
			binaryValue := a.Value(index)

			uuidVal, err := uuid.FromBytes(binaryValue)
			if err != nil {
				return nil, err
			}
			return uuidVal.String(), nil
		}
		// For non-UUID fixed-size binary, let default converter handle it for proper varbinary conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// convertDecimalToTrinoNumericFromInt converts a big.Int with scale to trino.Numeric
func convertDecimalToTrinoNumericFromInt(value *big.Int, scale int32) trino.Numeric {
	// scale means divide by 10^scale
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat := new(big.Rat).SetFrac(value, scaleFactor)

	return trino.Numeric(rat.FloatString(int(scale)))
}

// trinoConnectionFactory creates Trino connections
type trinoConnectionFactory struct{}

// CreateConnection implements sqlwrapper.ConnectionFactory
func (f *trinoConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with Trino-specific functionality
	return &trinoConnectionImpl{
		ConnectionImplBase: conn,
	}, nil
}

func (f *trinoConnectionFactory) CreateStatement(stmt *sqlwrapper.StatementImplBase) (sqlwrapper.StatementImpl, error) {
	return &trinoStatement{
		StatementImplBase: stmt,
	}, nil
}

// NewDriver constructs the ADBC Driver for "trino".
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	factory := &trinoConnectionFactory{}
	driver := sqlwrapper.NewDriver(alloc, "trino", vendorName, NewTrinoDBFactory()).
		WithConnectionFactory(factory).
		WithStatementFactory(factory).
		WithErrorInspector(TrinoErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for Trino",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
