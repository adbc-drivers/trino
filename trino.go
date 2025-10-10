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
	nullable := colType.Nullable

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
			// Convert precision to time unit
			if precision > 9 {
				precision = 9 // Clamp to max supported precision
			}
			timeUnit := arrow.TimeUnit(precision / 3)
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, nullable, metadata, nil
	case "REAL":
		// Trino uses REAL for float32/single precision
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		})
		return arrow.PrimitiveTypes.Float32, nullable, metadata, nil
	}

	// For all other types, fall back to default conversion
	return m.DefaultTypeConverter.ConvertRawColumnType(colType)
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
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// trinoTimestampInserter handles Trino's timezone-naive TIMESTAMP specially
type trinoTimestampInserter struct {
	builder *array.TimestampBuilder
	unit    arrow.TimeUnit
}

func (ins *trinoTimestampInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.(time.Time)
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
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.(string)
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
