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
	"errors"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/trinodb/trino-go-client/trino"
)

type TrinoErrorInspector struct{}

// InspectError examines a Trino error and formats it as an ADBC error
// Trino error mapping for reference: https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/StandardErrorCode.java
func (t TrinoErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	status := defaultStatus
	var vendorCode int32
	var sqlState [5]byte

	if trinoErr, ok := errors.AsType[*trino.ErrTrino](err); ok {
		vendorCode = int32(trinoErr.ErrorCode)
		if len(trinoErr.SqlState) >= 5 {
			copy(sqlState[:], trinoErr.SqlState[0:5])
		}

		switch trinoErr.ErrorType {
		case "USER_ERROR":
			// User errors include syntax errors, invalid arguments, etc.
			// Check error name for more specific mapping
			switch trinoErr.ErrorName {
			case "SYNTAX_ERROR", "INVALID_COLUMN_REFERENCE", "MISSING_COLUMN_NAME", "DUPLICATE_COLUMN_NAME":
				status = adbc.StatusInvalidArgument
			case "NOT_FOUND", "CATALOG_NOT_FOUND", "COLUMN_NOT_FOUND", "TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND", "FUNCTION_NOT_FOUND":
				status = adbc.StatusNotFound
			case "ALREADY_EXISTS":
				status = adbc.StatusAlreadyExists
			case "PERMISSION_DENIED":
				status = adbc.StatusUnauthorized
			case "NOT_SUPPORTED":
				status = adbc.StatusNotImplemented
			case "INVALID_CAST_ARGUMENT", "INVALID_FUNCTION_ARGUMENT":
				status = adbc.StatusInvalidArgument
			case "NUMERIC_VALUE_OUT_OF_RANGE", "DIVISION_BY_ZERO":
				status = adbc.StatusInvalidData
			case "CONSTRAINT_VIOLATION":
				status = adbc.StatusIntegrity
			case "USER_CANCELED":
				status = adbc.StatusCancelled
			case "ABANDONED_QUERY":
				status = adbc.StatusTimeout
			default:
				status = adbc.StatusInvalidArgument
			}

		case "INTERNAL_ERROR":
			status = adbc.StatusInternal

		case "EXTERNAL":
			status = adbc.StatusUnknown

		case "INSUFFICIENT_RESOURCES":
			status = adbc.StatusInternal
		}

		// If status still not determined, use official Trino error code ranges as fallback.
		if status == defaultStatus {
			switch {
			case trinoErr.ErrorCode >= 0 && trinoErr.ErrorCode <= 138:
				// USER_ERROR range
				status = adbc.StatusInvalidArgument
			case trinoErr.ErrorCode >= 65536 && trinoErr.ErrorCode <= 65566:
				// INTERNAL_ERROR range
				status = adbc.StatusInternal
			case trinoErr.ErrorCode >= 131072 && trinoErr.ErrorCode <= 131083:
				// INSUFFICIENT_RESOURCES range
				status = adbc.StatusInternal
			case trinoErr.ErrorCode >= 133001:
				// EXTERNAL range (starts at 133001, connector-specific codes start at 0x0100_0000)
				status = adbc.StatusUnknown
			}
		}

		//  If status still not determined, use SQLSTATE prefix as fallback.
		// SQLState values for reference: https://www.ibm.com/docs/en/i/7.6.0?topic=codes-listing-sqlstate-values
		if sqlState[0] != 0 && status == defaultStatus {
			switch string(sqlState[:2]) {
			case "02": // No data
				status = adbc.StatusNotFound
			case "07": // Dynamic SQL/Connection errors
				status = adbc.StatusIO
			case "08": // Connection exception
				status = adbc.StatusIO
			case "21", "22": // Cardinality/Data exception
				status = adbc.StatusInvalidData
			case "23": // Integrity constraint violation
				status = adbc.StatusIntegrity
			case "28": // Invalid authorization
				status = adbc.StatusUnauthenticated
			case "34": // Invalid cursor name
				status = adbc.StatusInvalidArgument
			case "42": // Syntax error or access rule violation
				status = adbc.StatusInvalidArgument
			case "44": // WITH CHECK OPTION violation
				status = adbc.StatusIntegrity
			case "55", "57": // Object not in prerequisite state / Operator intervention
				status = adbc.StatusInvalidState
			case "58": // System error
				status = adbc.StatusInternal
			}
		}
	}

	return adbc.Error{
		Code:       status,
		Msg:        err.Error(),
		VendorCode: vendorCode,
		SqlState:   sqlState,
	}
}
