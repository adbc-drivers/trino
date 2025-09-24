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

	// register the "trino" driver with database/sql
	_ "github.com/trinodb/trino-go-client/trino"

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
	// For now, just fall back to default conversion for all types
	// TODO: Add Trino-specific type conversions when needed
	return m.DefaultTypeConverter.ConvertRawColumnType(colType)
}

// CreateInserter creates Trino-specific inserters bound to builders for enhanced performance
func (m *trinoTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for Trino-specific types first
	switch field.Type.(type) {
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// ConvertArrowToGo implements Trino-specific Arrow value to Go value conversion
func (m *trinoTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle Trino-specific Arrow to Go conversions
	switch arrowArray.(type) {
	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
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

	driver := sqlwrapper.NewDriver(alloc, "trino", "Trino", typeConverter).
		WithConnectionFactory(&trinoConnectionFactory{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for Trino",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
