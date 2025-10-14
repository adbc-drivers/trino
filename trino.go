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
	"encoding/base64"
	"fmt"
	"strings"
	"time"
	"math/big"
	"database/sql/driver"

	// register the "trino" driver with database/sql
	_ "github.com/trinodb/trino-go-client/trino"
	"github.com/trinodb/trino-go-client/trino"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TrinoTypeConverter provides Trino-specific type conversion enhancements
type trinoTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

// ConvertRawColumnType implements TypeConverter with Trino-specific enhancements
func (m *trinoTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	// Handle Trino-specific type mappings first
	typeName := strings.ToUpper(colType.DatabaseTypeName)

	// trino go client does not provide clean way to get nullability so assume every column is always nullable
	colType.Nullable = true

	switch typeName {
	case "TIMESTAMP WITH TIME ZONE":
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
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
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
	case "TIME WITH TIME ZONE":
		// Trino's TIME WITH TIME ZONE stores time of day with timezone offset
		// Map to Arrow Time32/Time64 based on precision, similar to TIMESTAMP WITH TIME ZONE approach
		var timeType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			// precision represents fractional seconds digits (0-9)
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			timeUnit := convertPrecisionToTimeUnit(precision)

			if timeUnit == arrow.Second || timeUnit == arrow.Millisecond {
				timeType = &arrow.Time32Type{Unit: timeUnit}
			} else {
				timeType = &arrow.Time64Type{Unit: timeUnit}
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timeType = arrow.FixedWidthTypes.Time64us
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timeType, colType.Nullable, metadata, nil
	case "INTERVAL YEAR TO MONTH":
		// Trino's INTERVAL YEAR TO MONTH is returned as a string (e.g., "2-6" for 2 years 6 months)
		// Arrow doesn't have a native INTERVAL YEAR TO MONTH type, so map to string
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil
	case "INTERVAL DAY TO SECOND":
		// Trino's INTERVAL DAY TO SECOND is returned as a string (e.g., "1 23:59:59.999" for 1 day 23:59:59.999)
		// Arrow doesn't have a native INTERVAL DAY TO SECOND type, so map to string
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			// precision represents fractional seconds digits for the seconds component
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil
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
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil
	}

	// For all other types, fall back to default conversion
	return m.DefaultTypeConverter.ConvertRawColumnType(colType)
}

// Clamps precision to maximum supported value (9 fractional digits = nanoseconds)
func convertPrecisionToTimeUnit(precision int64) arrow.TimeUnit {
	if precision > 9 {
		// Clamp to max supported precision
		precision = 9
	}
	return arrow.TimeUnit(precision / 3)
}

// CreateInserter creates Trino-specific inserters bound to builders for enhanced performance
func (m *trinoTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for Trino-specific types first
	switch fieldType := field.Type.(type) {
	case *arrow.TimestampType:
		// Handle Trino's timezone-naive TIMESTAMP specially
		if fieldType.TimeZone == "" {
			return &trinoTimestampInserter{
				builder: builder.(*array.TimestampBuilder),
				unit:    fieldType.Unit,
			}, nil
		}
		// For timezone-aware timestamps, fall back to default
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		// Use custom binary inserter that handles base64 decoding from ConvertArrowToGo
		return &trinoBinaryInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
	case *arrow.Date32Type:
		return &date32Inserter{builder: builder.(*array.Date32Builder)}, nil
	case *arrow.Time32Type:
		// If this is a TIME WITH TIME ZONE column, use a custom inserter to normalize values to UTC.
		if dbTypeName, exists := field.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName); exists && dbTypeName == "TIME WITH TIME ZONE" {
			return &timeWithTimeZoneInserter32{
				builder: builder.(*array.Time32Builder),
				unit:    fieldType.Unit,
			}, nil
		}
		// For regular TIME, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.Time64Type:
		// If this is a TIME WITH TIME ZONE column, use a custom inserter to normalize values to UTC.
		if dbTypeName, exists := field.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName); exists && dbTypeName == "TIME WITH TIME ZONE" {
			return &timeWithTimeZoneInserter64{
				builder: builder.(*array.Time64Builder),
				unit:    fieldType.Unit,
			}, nil
		}
		// For regular TIME, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// trinoTimestampInserter handles Trino's timezone-naive TIMESTAMP specially
type trinoTimestampInserter struct {
	builder *array.TimestampBuilder
	unit    arrow.TimeUnit
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

	// For Trino's naive timestamps, treat the time.Time as wall-clock time
	// Create a new time.Time in UTC with the same wall-clock values
	// This preserves the naive semantics while working with Arrow's expectations
	naiveTime := time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		time.UTC, // Force UTC to prevent timezone conversion
	)

	ins.builder.AppendTime(naiveTime)
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

	t, ok := unwrapped.(string)
	if !ok {
		return fmt.Errorf("expected string for trino binary inserter, got %T", sqlValue)
	}

	decoded, err := base64.StdEncoding.DecodeString(t)
	if err != nil {
		return fmt.Errorf("failed to decode base64 binary data: %w", err)
	}
	ins.builder.Append(decoded)
	return nil
}

// TIME WITH TIME ZONE inserters
type timeWithTimeZoneInserter32 struct {
	builder *array.Time32Builder
	unit    arrow.TimeUnit
}

func (ins *timeWithTimeZoneInserter32) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	// When Trino Go client returns TIME WITH TIME ZONE we need to turn it into UTC
	timeValue, err := convertTimeWithTimeZoneToUTC32(unwrapped, ins.unit)
	if err != nil {
		return fmt.Errorf("failed to convert TIME WITH TIME ZONE: %w", err)
	}

	ins.builder.Append(timeValue)
	return nil
}

// convertTimeWithTimeZoneToUTC32 converts TIME WITH TIME ZONE to UTC equivalent Time32 value
func convertTimeWithTimeZoneToUTC32(sqlValue any, unit arrow.TimeUnit) (arrow.Time32, error) {
	// Trino Go client returns time.Time with timezone information
	t, ok := sqlValue.(time.Time)
	if !ok {
		return 0, fmt.Errorf("expected time.Time for TIME WITH TIME ZONE, got %T", sqlValue)
	}

	// Convert to UTC
	utcTime := t.UTC()

	// Extract time-of-day in UTC and convert to Time32 units
	seconds := utcTime.Hour()*3600 + utcTime.Minute()*60 + utcTime.Second()
	nanos := utcTime.Nanosecond()

	switch unit {
	case arrow.Second:
		return arrow.Time32(seconds), nil
	case arrow.Millisecond:
		return arrow.Time32(seconds*1000 + nanos/1000000), nil
	default:
		return 0, fmt.Errorf("unsupported Time32 unit: %v", unit)
	}
}

type timeWithTimeZoneInserter64 struct {
	builder *array.Time64Builder
	unit    arrow.TimeUnit
}

func (ins *timeWithTimeZoneInserter64) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	// When Trino Go client returns TIME WITH TIME ZONE we need to turn it into UTC
	timeValue, err := convertTimeWithTimeZoneToUTC64(unwrapped, ins.unit)
	if err != nil {
		return fmt.Errorf("failed to convert TIME WITH TIME ZONE: %w", err)
	}

	ins.builder.Append(timeValue)
	return nil
}

// convertTimeWithTimeZoneToUTC64 converts TIME WITH TIME ZONE to UTC equivalent Time64 value
func convertTimeWithTimeZoneToUTC64(sqlValue any, unit arrow.TimeUnit) (arrow.Time64, error) {
	// Trino Go client returns time.Time with timezone information
	t, ok := sqlValue.(time.Time)
	if !ok {
		return 0, fmt.Errorf("expected time.Time for TIME WITH TIME ZONE, got %T", sqlValue)
	}

	// Convert to UTC
	utcTime := t.UTC()

	// Extract time-of-day in UTC and convert to Time64 units
	seconds := utcTime.Hour()*3600 + utcTime.Minute()*60 + utcTime.Second()
	nanos := utcTime.Nanosecond()

	switch unit {
	case arrow.Microsecond:
		return arrow.Time64(int64(seconds)*1000000 + int64(nanos)/1000), nil
	case arrow.Nanosecond:
		return arrow.Time64(int64(seconds)*1000000000 + int64(nanos)), nil
	default:
		return 0, fmt.Errorf("unsupported Time64 unit: %v", unit)
	}
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

	case *array.Float32:
		// Trino Go client doesn't support float32/float64 - convert to string
		val := a.Value(index)
		return fmt.Sprintf("%g", val), nil

	case *array.Float64:
		// Trino Go client doesn't support float32/float64 - convert to string
		val := a.Value(index)
		return fmt.Sprintf("%g", val), nil

	case *array.Binary:
		// Trino Go client doesn't support []byte - convert to base64 string
		val := a.Value(index)
		return base64.StdEncoding.EncodeToString(val), nil

	case *array.LargeBinary:
		// Trino Go client doesn't support []byte - convert to base64 string
		val := a.Value(index)
		return base64.StdEncoding.EncodeToString(val), nil

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

// trinoConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type trinoConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version string
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*trinoConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*trinoConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*trinoConnectionImpl)(nil)

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

// NewDriver constructs the ADBC Driver for "trino".
func NewDriver(alloc memory.Allocator) adbc.Driver {
	typeConverter := &trinoTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{},
	}

	driver := sqlwrapper.NewDriver(alloc, "trino", "Trino", NewTrinoDBFactory(), typeConverter).
		WithConnectionFactory(&trinoConnectionFactory{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for Trino",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
