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
	"cmp"
	"fmt"
	"math"
	"math/big"
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
	// base is the type name without parameters, e.g. "DECIMAL" or
	// "TIMESTAMP WITH TIME ZONE".
	base string
	// text is the full text of the type, e.g. "TIMESTAMP(3) WITH TIME ZONE".
	text string
	// params holds the numeric parameters, e.g. [10 2] for DECIMAL(10, 2).
	params []int64
	// children holds the element of an ARRAY, the key and value of a MAP, or
	// the fields of a ROW.
	children []trinoField
}

type trinoField struct {
	// name is the ROW field name, or empty for an anonymous ROW field and
	// for ARRAY and MAP children.
	name string
	typ  *trinoType
}

// isNestedTypeName reports whether a database type name is an ARRAY, MAP or
// ROW type the client describes by its full signature.
func isNestedTypeName(typeName string) bool {
	upper := strings.ToUpper(strings.TrimSpace(typeName))
	return strings.HasPrefix(upper, "ARRAY(") ||
		strings.HasPrefix(upper, "MAP(") ||
		strings.HasPrefix(upper, "ROW(")
}

// parseTrinoType parses a Trino type signature.
func parseTrinoType(text string) (*trinoType, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("empty type")
	}

	open := strings.IndexByte(text, '(')
	if open < 0 {
		return newScalarType(normalizeSpaces(strings.ToUpper(text)), text, nil)
	}

	head := normalizeSpaces(strings.ToUpper(strings.TrimSpace(text[:open])))
	closeIdx, err := matchingParen(text, open)
	if err != nil {
		return nil, err
	}
	inner := text[open+1 : closeIdx]
	tail := strings.TrimSpace(text[closeIdx+1:])

	switch head {
	case "ARRAY", "MAP", "ROW":
		if tail != "" {
			return nil, fmt.Errorf("unexpected text %q after %s type", tail, head)
		}
		args, err := splitTopLevel(inner)
		if err != nil {
			return nil, err
		}
		t := &trinoType{base: head, text: text}
		switch head {
		case "ARRAY":
			if len(args) != 1 {
				return nil, fmt.Errorf("ARRAY takes 1 type argument, got %d", len(args))
			}
		case "MAP":
			if len(args) != 2 {
				return nil, fmt.Errorf("MAP takes 2 type arguments, got %d", len(args))
			}
		case "ROW":
			if len(args) == 0 {
				return nil, fmt.Errorf("ROW needs at least 1 field")
			}
		}
		for _, arg := range args {
			field := trinoField{}
			typeText := arg
			if head == "ROW" {
				field.name, typeText, err = splitRowField(arg)
				if err != nil {
					return nil, err
				}
			}
			field.typ, err = parseTrinoType(typeText)
			if err != nil {
				return nil, err
			}
			t.children = append(t.children, field)
		}
		return t, nil
	}

	// A parametric scalar type, e.g. DECIMAL(10, 2), VARCHAR(5),
	// TIMESTAMP(3) WITH TIME ZONE or INTERVAL DAY(2) TO SECOND(3). Parameters
	// may appear more than once (intervals), so collect all of them and
	// drop them from the base name. Types with type parameters, like
	// QDIGEST(BIGINT), are kept as an unparameterized leaf named after the
	// type, so they convert like a top-level column of that type would
	// instead of failing the whole column.
	if !numericParams(text) {
		return &trinoType{base: head, text: text}, nil
	}
	t := &trinoType{text: text}
	var base strings.Builder
	rest := text
	for {
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			base.WriteString(rest)
			break
		}
		base.WriteString(rest[:open])
		closeIdx, err := matchingParen(rest, open)
		if err != nil {
			return nil, err
		}
		for p := range strings.SplitSeq(rest[open+1:closeIdx], ",") {
			v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid parameter %q in type %q", p, text)
			}
			t.params = append(t.params, v)
		}
		rest = rest[closeIdx+1:]
	}
	return newScalarType(normalizeSpaces(strings.ToUpper(base.String())), text, t.params)
}

// newScalarType returns a type without children, rejecting ARRAY, MAP and
// ROW, which always need type arguments.
func newScalarType(base, text string, params []int64) (*trinoType, error) {
	switch base {
	case "ARRAY", "MAP", "ROW":
		return nil, fmt.Errorf("%s type without type arguments in %q", base, text)
	}
	return &trinoType{base: base, text: text, params: params}, nil
}

// numericParams reports whether every parenthesized parameter list in a
// scalar type holds only integers, as in DECIMAL(10, 2) or
// INTERVAL DAY(2) TO SECOND(3).
func numericParams(text string) bool {
	inParams := false
	for _, c := range text {
		switch {
		case c == '(':
			inParams = true
		case c == ')':
			inParams = false
		case inParams && (c < '0' || c > '9') && c != ',' && c != ' ' && c != '\t':
			return false
		}
	}
	return true
}

// matchingParen returns the index of the parenthesis closing the one at
// open, skipping over double-quoted identifiers.
func matchingParen(text string, open int) (int, error) {
	depth := 0
	inQuote := false
	for i := open; i < len(text); i++ {
		c := text[i]
		switch {
		case inQuote:
			if c == '"' {
				if i+1 < len(text) && text[i+1] == '"' {
					i++ // escaped quote
				} else {
					inQuote = false
				}
			}
		case c == '"':
			inQuote = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, fmt.Errorf("unbalanced parentheses in type %q", text)
}

// splitTopLevel splits a type argument list on the commas that are not
// nested in parentheses or quoted identifiers.
func splitTopLevel(text string) ([]string, error) {
	var parts []string
	depth := 0
	inQuote := false
	start := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case inQuote:
			if c == '"' {
				if i+1 < len(text) && text[i+1] == '"' {
					i++
				} else {
					inQuote = false
				}
			}
		case c == '"':
			inQuote = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced parentheses in %q", text)
			}
		case c == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + 1
		}
	}
	if depth != 0 || inQuote {
		return nil, fmt.Errorf("unbalanced parentheses or quotes in %q", text)
	}
	if last := strings.TrimSpace(text[start:]); last != "" || len(parts) > 0 {
		parts = append(parts, last)
	}
	if slices.Contains(parts, "") {
		return nil, fmt.Errorf("empty type argument in %q", text)
	}
	return parts, nil
}

// anonymousMultiWordType matches the scalar types whose names are several
// words, so a ROW field like "TIMESTAMP(3) WITH TIME ZONE" is read as an
// anonymous field of that type rather than a field named TIMESTAMP(3).
var anonymousMultiWordType = regexp.MustCompile(
	`^(TIME|TIMESTAMP)(\s*\(\s*\d+\s*\))?\s+WITH(OUT)?\s+TIME\s+ZONE$|` +
		`^INTERVAL\s+(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)(\s*\([\d\s,]+\))?(\s+TO\s+(MONTH|HOUR|MINUTE|SECOND)(\s*\([\d\s,]+\))?)?$|` +
		`^DOUBLE\s+PRECISION$`)

// splitRowField splits a ROW field into its name and type. Fields are either
// "name type" or, for anonymous fields, just "type". The name may be a
// double-quoted identifier.
func splitRowField(field string) (name, typeText string, err error) {
	field = strings.TrimSpace(field)
	if strings.HasPrefix(field, `"`) {
		var b strings.Builder
		for i := 1; i < len(field); i++ {
			if field[i] == '"' {
				if i+1 < len(field) && field[i+1] == '"' {
					b.WriteByte('"')
					i++
					continue
				}
				rest := strings.TrimSpace(field[i+1:])
				if rest == "" {
					return "", "", fmt.Errorf("ROW field %q has no type", field)
				}
				return b.String(), rest, nil
			}
			b.WriteByte(field[i])
		}
		return "", "", fmt.Errorf("unterminated quoted ROW field name in %q", field)
	}

	upper := strings.ToUpper(field)
	if anonymousMultiWordType.MatchString(upper) {
		return "", field, nil
	}
	// The name is the first word, unless the first word is immediately
	// followed by a parenthesis (then it is the type, e.g. DECIMAL(10, 2)).
	idx := strings.IndexAny(field, " \t\n(")
	if idx < 0 || field[idx] == '(' {
		return "", field, nil
	}
	return field[:idx], strings.TrimSpace(field[idx+1:]), nil
}

func normalizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// rowFieldName returns the Arrow field name of a ROW field. The client
// reports type signatures upper-cased, so the original case of a field name
// is lost. Names are lower-cased, which matches unquoted identifiers (Trino
// folds them to lower case) and the names Hive and Iceberg tables use, but
// not quoted names with upper-case letters, like "MixedCase". Anonymous
// fields are named like the client names them: field0, field1, ...
// Field names are not made unique, so ROW(field1 integer, integer) gives two
// fields named field1.
func rowFieldName(f trinoField, index int) string {
	if f.name == "" {
		return "field" + strconv.Itoa(index)
	}
	return strings.ToLower(f.name)
}

// nestedArrowField converts a parsed Trino type into an Arrow field. Scalar
// types go through ConvertRawColumnType, so a scalar nested in an ARRAY, MAP
// or ROW gets the same Arrow type as a top-level column of the same Trino
// type. Its metadata has the same keys where the type text carries them
// (precision, scale, length), but not ones only the client reports, like the
// precision of TIME WITH TIME ZONE.
//
// Top-level ARRAY columns that scanTypeToListMap covers (simple element
// types, up to three levels deep) keep their existing mapping (e.g.
// ARRAY(INTEGER) as list<int64>, ARRAY(DECIMAL) as list<string>) for backward
// compatibility, so they can differ from the same ARRAY nested in a ROW or
// MAP, or nested deeper, which goes through this function.
func (m *trinoTypeConverter) nestedArrowField(name string, t *trinoType) (arrow.Field, error) {
	nestedMeta := func() arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: t.text,
			sqlwrapper.MetaKeyColumnName:       name,
		})
	}

	switch t.base {
	case "ARRAY":
		elem, err := m.nestedArrowField("item", t.children[0].typ)
		if err != nil {
			return arrow.Field{}, err
		}
		return arrow.Field{Name: name, Type: arrow.ListOfField(elem), Nullable: true, Metadata: nestedMeta()}, nil

	case "MAP":
		key, err := m.nestedArrowField("key", t.children[0].typ)
		if err != nil {
			return arrow.Field{}, err
		}
		if !isSupportedMapKeyType(key.Type) {
			// The JSON protocol sends map keys as strings: dates and
			// timestamps in their text form, VARBINARY as base64, ROW and
			// ARRAY keys as Trino's display text. Only the key types the
			// client returns as plain numbers, booleans or strings for
			// top-level columns are converted back; other keys stay
			// strings rather than reimplementing the client's parsing of
			// dates, times and other types here. The metadata keeps
			// the full Trino type text, e.g. TIMESTAMP(3), without separate
			// precision or length keys, which do not apply to a string.
			key.Type = arrow.BinaryTypes.String
			key.Metadata = arrow.MetadataFrom(map[string]string{
				sqlwrapper.MetaKeyDatabaseTypeName: t.children[0].typ.text,
				sqlwrapper.MetaKeyColumnName:       "key",
			})
		}
		key.Nullable = false
		item, err := m.nestedArrowField("value", t.children[1].typ)
		if err != nil {
			return arrow.Field{}, err
		}
		return arrow.Field{Name: name, Type: arrow.MapOfFields(key, item), Nullable: true, Metadata: nestedMeta()}, nil

	case "ROW":
		fields := make([]arrow.Field, len(t.children))
		for i, child := range t.children {
			f, err := m.nestedArrowField(rowFieldName(child, i), child.typ)
			if err != nil {
				return arrow.Field{}, err
			}
			fields[i] = f
		}
		return arrow.Field{Name: name, Type: arrow.StructOf(fields...), Nullable: true, Metadata: nestedMeta()}, nil
	}

	colType := sqlwrapper.ColumnType{
		Name:             name,
		DatabaseTypeName: t.base,
		Nullable:         true,
	}
	switch {
	case t.base == "DECIMAL" && len(t.params) == 2:
		colType.Precision, colType.Scale = &t.params[0], &t.params[1]
	case (t.base == "VARCHAR" || t.base == "CHAR") && len(t.params) == 1:
		colType.Length = &t.params[0]
	case t.base == "VARCHAR":
		// the client reports unbounded VARCHAR columns with the maximum length
		unbounded := int64(math.MaxInt32)
		colType.Length = &unbounded
	case strings.HasPrefix(t.base, "TIME"): // TIME and TIMESTAMP, with or without time zone
		if len(t.params) == 1 {
			colType.Precision = &t.params[0]
		}
	case strings.HasPrefix(t.base, "INTERVAL "):
		// Keep the qualifier, e.g. "INTERVAL DAY(2) TO SECOND(3)", which
		// the interval conversion and inserters read from the metadata.
		colType.DatabaseTypeName = strings.ToUpper(t.text)
		// Report precision and scale like the client does for a top-level
		// column: the leading field precision, and the fractional second
		// precision if the qualifier ends in SECOND, else 0.
		if len(t.params) > 0 {
			scale := int64(0)
			if strings.HasSuffix(t.base, "SECOND") && len(t.params) == 2 {
				scale = t.params[1]
			}
			colType.Precision, colType.Scale = &t.params[0], &scale
		}
	}

	arrowType, nullable, metadata, err := m.ConvertRawColumnType(colType)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: name, Type: arrowType, Nullable: nullable, Metadata: metadata}, nil
}

// isSupportedMapKeyType reports whether a map key of this Arrow type can be
// recovered from the string the JSON protocol sends it as.
func isSupportedMapKeyType(dt arrow.DataType) bool {
	switch dt.(type) {
	case *arrow.StringType, *arrow.LargeStringType,
		*arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Float32Type, *arrow.Float64Type,
		*arrow.BooleanType,
		*arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type,
		*extensions.UUIDType:
		return true
	}
	return false
}

// convertMapKey turns the string form of a map key back into the value the
// key's inserter expects, matching what the client returns for a top-level
// column of that type.
func convertMapKey(dt arrow.DataType, key string) (any, error) {
	switch dt.(type) {
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
		return strconv.ParseInt(key, 10, 64)
	case *arrow.Float32Type, *arrow.Float64Type:
		return strconv.ParseFloat(key, 64)
	case *arrow.BooleanType:
		return strconv.ParseBool(key)
	default:
		// strings, decimals and UUIDs arrive as strings for top-level
		// columns too
		return key, nil
	}
}

// rowValue is the part of trino.Row the struct inserter uses. It is an
// interface so tests can provide ROW values, as trino.Row has no exported
// constructor.
type rowValue interface {
	Len() int
	Name(i int) string
	Value(i int) any
}

var _ rowValue = trino.Row{}

// structInserter appends Trino ROW values to a struct builder.
type structInserter struct {
	builder  *array.StructBuilder
	children []sqlwrapper.Inserter
}

func (ins *structInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	if r, ok := sqlValue.(trino.Row); ok && !r.Valid {
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

// mapInserter appends Trino MAP values to a map builder. Entries are sorted
// by key so the output is deterministic, as the client returns a Go map.
type mapInserter struct {
	builder    *array.MapBuilder
	keyType    arrow.DataType
	decimalKey bool
	keyIns     sqlwrapper.Inserter
	valueIns   sqlwrapper.Inserter
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
	entries := make([]mapEntry, 0, len(m))
	for k := range m {
		key, err := convertMapKey(ins.keyType, k)
		if err != nil {
			return fmt.Errorf("MAP key %q: %w", k, err)
		}
		e := mapEntry{raw: k, key: key}
		if ins.decimalKey {
			// decimal keys stay strings for the inserter, but sort by value
			if e.decimal, ok = new(big.Rat).SetString(k); !ok {
				return fmt.Errorf("MAP key %q: invalid decimal", k)
			}
		}
		entries = append(entries, e)
	}
	slices.SortFunc(entries, compareMapEntries)

	ins.builder.Append(true)
	for _, e := range entries {
		if err := ins.keyIns.AppendValue(e.key); err != nil {
			return fmt.Errorf("MAP key %q: %w", e.raw, err)
		}
		if err := ins.valueIns.AppendValue(m[e.raw]); err != nil {
			return fmt.Errorf("MAP value for key %q: %w", e.raw, err)
		}
	}
	return nil
}

// mapEntry is a MAP key: the string the protocol sent it as, the value the
// key inserter gets, and for decimal keys the value to sort by.
type mapEntry struct {
	raw     string
	key     any
	decimal *big.Rat
}

func isDecimalType(dt arrow.DataType) bool {
	switch dt.(type) {
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		return true
	}
	return false
}

// compareMapEntries orders numeric and boolean keys by value and all other
// keys by their string form. NaN sorts after all other floating point keys,
// as in Trino. All keys of a map have the same type, as convertMapKey
// converts them by the key type.
func compareMapEntries(a, b mapEntry) int {
	if a.decimal != nil && b.decimal != nil {
		return a.decimal.Cmp(b.decimal)
	}
	switch x := a.key.(type) {
	case int64:
		if y, ok := b.key.(int64); ok {
			return cmp.Compare(x, y)
		}
	case float64:
		if y, ok := b.key.(float64); ok {
			switch xNaN, yNaN := math.IsNaN(x), math.IsNaN(y); {
			case xNaN && yNaN:
				return 0
			case xNaN:
				return 1
			case yNaN:
				return -1
			}
			return cmp.Compare(x, y)
		}
	case bool:
		if y, ok := b.key.(bool); ok {
			switch {
			case x == y:
				return 0
			case !x:
				return -1
			default:
				return 1
			}
		}
	}
	return strings.Compare(a.raw, b.raw)
}
