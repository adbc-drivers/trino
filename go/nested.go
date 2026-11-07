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
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/trinodb/trino-go-client/trino"
)

// trinoType is a parsed Trino type, as the client reports it in
// ColumnTypeDatabaseTypeName for ARRAY, MAP and ROW columns, e.g.
// "ROW(ID BIGINT, TAGS ARRAY(VARCHAR), PRICE DECIMAL(10, 2))".
type trinoType struct {
	base     string       // name without parameters, e.g. "TIMESTAMP WITH TIME ZONE"
	text     string       // full text, e.g. "TIMESTAMP(3) WITH TIME ZONE"
	params   []int64      // numeric parameters, e.g. [10 2] for DECIMAL(10, 2)
	children []trinoField // ARRAY element, MAP key and value, or ROW fields
}

type trinoField struct {
	name string // ROW field name; empty for anonymous fields and ARRAY/MAP children
	typ  *trinoType
}

func parseTrinoType(text string) (*trinoType, error) {
	text = strings.TrimSpace(text)
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return newScalarType(normalizeSpaces(strings.ToUpper(text)), text, nil)
	}
	head := normalizeSpaces(strings.ToUpper(text[:open]))

	if head == "ARRAY" || head == "MAP" || head == "ROW" {
		args, end, err := splitArgs(text, open)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(text[end:]) != "" ||
			(head == "ARRAY" && len(args) != 1) || (head == "MAP" && len(args) != 2) {
			return nil, fmt.Errorf("invalid %s type %q", head, text)
		}
		t := &trinoType{base: head, text: text}
		for _, arg := range args {
			var field trinoField
			if head == "ROW" {
				if field.name, arg, err = splitRowField(arg); err != nil {
					return nil, err
				}
			}
			if field.typ, err = parseTrinoType(arg); err != nil {
				return nil, err
			}
			t.children = append(t.children, field)
		}
		return t, nil
	}

	// A parametric scalar, e.g. DECIMAL(10, 2) or INTERVAL DAY(2) TO
	// SECOND(3): collect the parameters and drop them from the name.
	var base strings.Builder
	var params []int64
	for rest := text; ; {
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			base.WriteString(rest)
			break
		}
		base.WriteString(rest[:open])
		args, end, err := splitArgs(rest, open)
		if err != nil {
			return nil, err
		}
		for _, arg := range args {
			p, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				// A type parameter, as in QDIGEST(BIGINT): convert it like a
				// top-level column of that type.
				return &trinoType{base: head, text: text}, nil
			}
			params = append(params, p)
		}
		rest = rest[end:]
	}
	return newScalarType(normalizeSpaces(strings.ToUpper(base.String())), text, params)
}

func newScalarType(base, text string, params []int64) (*trinoType, error) {
	if base == "" || base == "ARRAY" || base == "MAP" || base == "ROW" {
		return nil, fmt.Errorf("invalid type %q", text)
	}
	return &trinoType{base: base, text: text, params: params}, nil
}

// splitArgs splits the parenthesized list starting at text[open] on its
// top-level commas, skipping over quoted identifiers, and returns the index
// after its closing parenthesis.
func splitArgs(text string, open int) (args []string, end int, err error) {
	depth, start, inQuote := 0, open+1, false
	for i := open; i < len(text); i++ {
		c := text[i]
		if c == '"' {
			inQuote = !inQuote // an escaped "" toggles twice
		}
		if inQuote || (c != '(' && c != ')' && c != ',') {
			continue
		}
		if c == '(' {
			depth++
			continue
		}
		if c == ')' {
			depth--
		}
		if depth == 0 || (c == ',' && depth == 1) {
			arg := strings.TrimSpace(text[start:i])
			if arg == "" {
				return nil, 0, fmt.Errorf("empty type argument in %q", text)
			}
			args, start = append(args, arg), i+1
			if depth == 0 {
				return args, i + 1, nil
			}
		}
	}
	return nil, 0, fmt.Errorf("unbalanced parentheses or quotes in %q", text)
}

// anonymousMultiWordType matches the types whose names are several words, so
// a ROW field like "TIMESTAMP(3) WITH TIME ZONE" is read as an anonymous field
// rather than a field named TIMESTAMP(3).
var anonymousMultiWordType = regexp.MustCompile(
	`^(TIME|TIMESTAMP)(\s*\(\s*\d+\s*\))?\s+WITH(OUT)?\s+TIME\s+ZONE$|` +
		`^INTERVAL\s+(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)(\s*\([\d\s,]+\))?(\s+TO\s+(MONTH|HOUR|MINUTE|SECOND)(\s*\([\d\s,]+\))?)?$`)

// splitRowField splits a ROW field into its name, which may be a quoted
// identifier, and its type. Anonymous fields are just a type.
func splitRowField(field string) (name, typeText string, err error) {
	if strings.HasPrefix(field, `"`) {
		end := 1 // index of the closing quote
		for end < len(field) && (field[end] != '"' || strings.HasPrefix(field[end:], `""`)) {
			if field[end] == '"' {
				end++ // escaped quote
			}
			end++
		}
		if end >= len(field) || strings.TrimSpace(field[end+1:]) == "" {
			return "", "", fmt.Errorf("invalid ROW field %q", field)
		}
		return strings.ReplaceAll(field[1:end], `""`, `"`), strings.TrimSpace(field[end+1:]), nil
	}
	// The name is the first word, unless it is directly followed by a
	// parenthesis, as in the anonymous field DECIMAL(10, 2).
	idx := strings.IndexAny(field, " \t\n(")
	if idx < 0 || field[idx] == '(' || anonymousMultiWordType.MatchString(strings.ToUpper(field)) {
		return "", field, nil
	}
	return field[:idx], strings.TrimSpace(field[idx+1:]), nil
}

func normalizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// nestedArrowField converts a parsed type into an Arrow field. Scalars go
// through ConvertRawColumnType, so they get the same Arrow type as top-level
// columns.
func (m *trinoTypeConverter) nestedArrowField(name string, t *trinoType) (arrow.Field, error) {
	field := arrow.Field{Name: name, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{
		sqlwrapper.MetaKeyDatabaseTypeName: t.text,
		sqlwrapper.MetaKeyColumnName:       name,
	})}
	children := make([]arrow.Field, len(t.children))
	for i, child := range t.children {
		childName := "item"
		switch t.base {
		case "MAP":
			childName = []string{"key", "value"}[i]
		case "ROW":
			// The client reports type text upper-cased, so names are
			// lower-cased: right for unquoted identifiers and Hive/Iceberg
			// names, but quoted names like "MixedCase" lose their case.
			// Anonymous fields are named like the client names them.
			childName = strings.ToLower(child.name)
			if child.name == "" {
				childName = "field" + strconv.Itoa(i)
			}
		}
		var err error
		if children[i], err = m.nestedArrowField(childName, child.typ); err != nil {
			return arrow.Field{}, err
		}
	}

	switch t.base {
	case "ARRAY":
		field.Type = arrow.ListOfField(children[0])
		return field, nil
	case "ROW":
		field.Type = arrow.StructOf(children...)
		return field, nil
	case "MAP":
		key := children[0]
		if !isSupportedMapKeyType(key.Type) {
			// The protocol sends map keys as strings. Keys of other types
			// (dates, timestamps, ...) stay strings rather than reparsing
			// them here.
			key.Type = arrow.BinaryTypes.String
			key.Metadata = arrow.MetadataFrom(map[string]string{
				sqlwrapper.MetaKeyDatabaseTypeName: t.children[0].typ.text,
				sqlwrapper.MetaKeyColumnName:       "key",
			})
		}
		key.Nullable = false
		field.Type = arrow.MapOfFields(key, children[1])
		return field, nil
	}

	// Report precision, scale and length like the client does for a
	// top-level column of this type.
	colType := sqlwrapper.ColumnType{Name: name, DatabaseTypeName: t.base, Nullable: true}
	switch {
	case t.base == "DECIMAL" && len(t.params) == 2:
		colType.Precision, colType.Scale = &t.params[0], &t.params[1]
	case (t.base == "VARCHAR" || t.base == "CHAR") && len(t.params) == 1:
		colType.Length = &t.params[0]
	case t.base == "VARCHAR":
		unbounded := int64(math.MaxInt32)
		colType.Length = &unbounded
	case strings.HasPrefix(t.base, "TIME") && len(t.params) == 1:
		colType.Precision = &t.params[0]
	case strings.HasPrefix(t.base, "INTERVAL "):
		// keep the qualifier with its precision, like top-level columns
		colType.DatabaseTypeName = t.text
		if len(t.params) > 0 {
			scale := int64(0) // fractional seconds, only after SECOND
			if strings.HasSuffix(t.base, "SECOND") && len(t.params) == 2 {
				scale = t.params[1]
			}
			colType.Precision, colType.Scale = &t.params[0], &scale
		}
	}
	var err error
	field.Type, field.Nullable, field.Metadata, err = m.ConvertRawColumnType(colType)
	return field, err
}

// isSupportedMapKeyType reports whether a map key of this type can be
// recovered from the string the protocol sends it as.
func isSupportedMapKeyType(dt arrow.DataType) bool {
	switch dt.(type) {
	case *arrow.StringType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.BooleanType,
		*arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type,
		*extensions.UUIDType:
		return true
	}
	return false
}

// rowValue is the part of trino.Row the struct inserter uses, so tests can
// provide ROW values (trino.Row has no exported constructor).
type rowValue interface {
	Len() int
	Name(i int) string
	Value(i int) any
}

var _ rowValue = trino.Row{}

type structInserter struct {
	builder  *array.StructBuilder
	children []sqlwrapper.Inserter
}

func (ins *structInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	row, ok := sqlValue.(rowValue)
	if !ok {
		return fmt.Errorf("expected trino.Row for ROW type, got %T", sqlValue)
	}
	if row.Len() != len(ins.children) {
		return fmt.Errorf("expected ROW with %d fields, got %d", len(ins.children), row.Len())
	}
	ins.builder.Append(true)
	for i, child := range ins.children {
		if err := child.AppendValue(row.Value(i)); err != nil {
			return fmt.Errorf("ROW field %q: %w", row.Name(i), err)
		}
	}
	return nil
}

// mapInserter appends MAP values. The client returns a Go map, so entries
// are sorted by key text to make the output deterministic.
type mapInserter struct {
	builder  *array.MapBuilder
	keyIns   sqlwrapper.Inserter
	valueIns sqlwrapper.Inserter
}

func (ins *mapInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	m, ok := sqlValue.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map[string]any for MAP type, got %T", sqlValue)
	}
	ins.builder.Append(true)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		// the default numeric and boolean inserters parse the key text
		if err := ins.keyIns.AppendValue(k); err != nil {
			return fmt.Errorf("MAP key %q: %w", k, err)
		}
		if err := ins.valueIns.AppendValue(m[k]); err != nil {
			return fmt.Errorf("MAP value for key %q: %w", k, err)
		}
	}
	return nil
}
