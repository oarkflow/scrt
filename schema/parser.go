package schema

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/oarkflow/scrt/temporal"
)

// Parse reads schema definitions from the SCRT DSL.
func Parse(r io.Reader) (*Document, error) {
	scanner := bufio.NewScanner(r)
	doc := &Document{
		Schemas: make(map[string]*Schema),
		Data:    make(map[string][]map[string]interface{}),
	}

	var current *Schema
	var awaitingName bool
	var currentDataSchema string
	var fieldBlock bool

	finishCurrent := func() error {
		if current == nil {
			return nil
		}
		if _, exists := doc.Schemas[current.Name]; exists {
			return fmt.Errorf("duplicate schema %q", current.Name)
		}
		doc.Schemas[current.Name] = current
		current = nil
		fieldBlock = false
		return nil
	}

	startSchema := func(name string) error {
		if err := finishCurrent(); err != nil {
			return err
		}
		if name == "" {
			return errors.New("schema name cannot be empty")
		}
		current = &Schema{Name: name}
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if fieldBlock && current != nil && currentDataSchema == "" && !strings.HasPrefix(line, "@") {
			field, err := parseField(line)
			if err != nil {
				return nil, err
			}
			current.Fields = append(current.Fields, field)
			continue
		}

		if awaitingName {
			if err := startSchema(line); err != nil {
				return nil, err
			}
			awaitingName = false
			fieldBlock = false
			continue
		}

		switch {
		case strings.HasPrefix(line, "@schema"):
			fieldBlock = false
			currentDataSchema = ""
			rest := strings.TrimSpace(strings.TrimPrefix(line, "@schema"))
			if strings.HasPrefix(rest, ":") {
				rest = strings.TrimSpace(rest[1:])
			}
			if rest == "" {
				awaitingName = true
				continue
			}
			if err := startSchema(rest); err != nil {
				return nil, err
			}

		case strings.HasPrefix(line, "@field"):
			fieldBlock = false
			currentDataSchema = ""
			if current == nil {
				return nil, errors.New("@field outside of schema")
			}
			field, err := parseField(strings.TrimSpace(strings.TrimPrefix(line, "@field")))
			if err != nil {
				return nil, err
			}
			current.Fields = append(current.Fields, field)

		case strings.HasPrefix(strings.ToLower(line), "fields"):
			if current == nil {
				return nil, errors.New("fields block outside of schema")
			}
			fieldBlock = true
			continue
		case strings.HasPrefix(line, "@"):
			fieldBlock = false
			awaitingName = false
			if err := finishCurrent(); err != nil {
				return nil, err
			}

			// Check if it's a data row (contains =) or section marker
			if strings.Contains(line, "=") && currentDataSchema != "" {
				// This is a data row like @MsgID=1002, not a section marker
				sch, exists := doc.Schemas[currentDataSchema]
				if exists {
					row, err := parseDataRow(line, sch)
					if err != nil {
						return nil, fmt.Errorf("parsing data row for %s: %w", currentDataSchema, err)
					}
					doc.Data[currentDataSchema] = append(doc.Data[currentDataSchema], row)
				}
				continue
			}

			// Data section marker: @Message, @User, etc.
			schemaName := strings.TrimSpace(strings.TrimPrefix(line, "@"))
			currentDataSchema = schemaName
			continue

		default:
			// If we're in a data section, parse the row
			if currentDataSchema != "" {
				sch, exists := doc.Schemas[currentDataSchema]
				if !exists {
					// Schema not yet defined, skip
					continue
				}
				row, err := parseDataRow(line, sch)
				if err != nil {
					return nil, fmt.Errorf("parsing data row for %s: %w", currentDataSchema, err)
				}
				doc.Data[currentDataSchema] = append(doc.Data[currentDataSchema], row)
			}
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if awaitingName {
		return nil, errors.New("schema name expected after @schema")
	}
	if err := finishCurrent(); err != nil {
		return nil, err
	}
	if err := doc.finalize(); err != nil {
		return nil, err
	}
	return doc, nil
}

func parseField(body string) (Field, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return Field{}, errors.New("empty @field declaration")
	}
	name, typ, attrChunk, err := splitFieldParts(body)
	if err != nil {
		return Field{}, err
	}
	field := Field{Name: name, RawType: typ}
	lower := strings.ToLower(typ)
	switch {
	case lower == "uint64" || lower == "uint":
		field.Kind = KindUint64
	case lower == "string" || lower == "str" || lower == "text":
		field.Kind = KindString
	case lower == "bool" || lower == "boolean":
		field.Kind = KindBool
	case lower == "int64" || lower == "int" || lower == "integer":
		field.Kind = KindInt64
	case lower == "float64" || lower == "float" || lower == "double":
		field.Kind = KindFloat64
	case lower == "bytes" || lower == "blob":
		field.Kind = KindBytes
	case lower == "date":
		field.Kind = KindDate
	case lower == "datetime":
		field.Kind = KindDateTime
	case lower == "timestamp":
		field.Kind = KindTimestamp
	case lower == "timestamptz":
		field.Kind = KindTimestampTZ
	case lower == "duration":
		field.Kind = KindDuration
	case strings.HasPrefix(lower, "ref:"):
		field.Kind = KindRef
		parts := strings.Split(typ, ":")
		if len(parts) != 3 {
			return Field{}, fmt.Errorf("invalid ref declaration: %q", typ)
		}
		field.TargetSchema = parts[1]
		field.TargetField = parts[2]
	default:
		return Field{}, fmt.Errorf("unsupported field type %q", typ)
	}

	if attrChunk != "" {
		attrs := splitFieldAttributes(attrChunk)
		for _, attr := range attrs {
			attr = strings.TrimSpace(attr)
			if attr == "" {
				continue
			}
			lower := strings.ToLower(attr)
			switch {
			case lower == "auto_increment" || lower == "autoincrement" || lower == "serial":
				field.AutoIncrement = true
			case strings.HasPrefix(lower, "default="):
				val := strings.TrimSpace(attr[len("default="):])
				if err := assignFieldDefault(&field, val); err != nil {
					return Field{}, err
				}
			case strings.HasPrefix(lower, "default:"):
				val := strings.TrimSpace(attr[len("default:"):])
				if err := assignFieldDefault(&field, val); err != nil {
					return Field{}, err
				}
			default:
				// keep normalized attribute for hashing/reference
			}
			field.Attributes = append(field.Attributes, lower)
		}
	}

	return field, nil
}

func splitFieldParts(body string) (string, string, string, error) {
	body = strings.TrimSpace(body)
	firstSep := strings.IndexAny(body, " \t")
	if firstSep == -1 {
		return "", "", "", fmt.Errorf("invalid @field declaration: %q", body)
	}
	name := strings.TrimSpace(body[:firstSep])
	remaining := strings.TrimSpace(body[firstSep+1:])
	if name == "" || remaining == "" {
		return "", "", "", fmt.Errorf("invalid @field declaration: %q", body)
	}
	secondSep := strings.IndexAny(remaining, " \t")
	if secondSep == -1 {
		return name, remaining, "", nil
	}
	typ := strings.TrimSpace(remaining[:secondSep])
	attrs := strings.TrimSpace(remaining[secondSep+1:])
	return name, typ, attrs, nil
}

func splitFieldAttributes(input string) []string {
	var (
		attrs []string
		buf   strings.Builder
		quote rune
	)
	flush := func() {
		part := strings.TrimSpace(buf.String())
		if part != "" {
			attrs = append(attrs, part)
		}
		buf.Reset()
	}
	for _, r := range input {
		switch r {
		case '"', '\'', '`':
			if quote == 0 {
				quote = r
			} else if quote == r {
				quote = 0
			}
			buf.WriteRune(r)
		case '|', ',', ' ', '\t':
			if quote != 0 {
				buf.WriteRune(r)
			} else {
				flush()
			}
		default:
			buf.WriteRune(r)
		}
	}
	flush()
	return attrs
}

func assignFieldDefault(field *Field, literal string) error {
	if field == nil {
		return errors.New("nil field for default assignment")
	}
	if field.Kind == KindRef {
		field.pendingDefault = literal
		return nil
	}
	parsed, err := parseDefaultLiteral(field.Kind, literal)
	if err != nil {
		return err
	}
	field.Default = parsed
	return nil
}

func parseDataRow(line string, sch *Schema) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	rawFields := parseCSVLine(line)
	valueTokensRemaining := countValueTokens(rawFields)
	fieldIdx := 0

	skipAuto := func(valuesRemaining int) {
		for fieldIdx < len(sch.Fields) && sch.Fields[fieldIdx].AutoIncrement {
			nonAuto := countNonAutoFields(sch.Fields, fieldIdx)
			if valuesRemaining > nonAuto {
				return
			}
			fieldIdx++
		}
	}

	for _, rawField := range rawFields {
		rawField = strings.TrimSpace(rawField)
		if rawField == "" {
			fieldIdx++
			continue
		}

		if strings.HasPrefix(rawField, "@") {
			idx, err := applyExplicitFieldAssignment(row, sch, rawField[1:])
			if err != nil {
				return nil, err
			}
			if idx >= 0 && idx >= fieldIdx {
				fieldIdx = idx + 1
			}
			continue
		}

		valuesRemaining := valueTokensRemaining
		skipAuto(valuesRemaining)

		if fieldIdx >= len(sch.Fields) {
			return nil, fmt.Errorf("too many fields in data row")
		}

		field := sch.Fields[fieldIdx]
		val, err := parseValue(rawField, &field)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}
		row[field.Name] = val
		fieldIdx++
		valueTokensRemaining--
	}

	return row, nil
}

func countValueTokens(fields []string) int {
	count := 0
	for _, field := range fields {
		trimmed := strings.TrimSpace(field)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "@") {
			continue
		}
		count++
	}
	return count
}

func countNonAutoFields(fields []Field, start int) int {
	count := 0
	for i := start; i < len(fields); i++ {
		if !fields[i].AutoIncrement {
			count++
		}
	}
	return count
}

func applyExplicitFieldAssignment(row map[string]interface{}, sch *Schema, expr string) (int, error) {
	parts := strings.SplitN(strings.TrimSpace(expr), "=", 2)
	if len(parts) != 2 {
		return -1, fmt.Errorf("invalid field assignment %q", expr)
	}
	fieldName := normalizeAssignmentTarget(parts[0])
	field := findFieldByName(sch, fieldName)
	if field == nil {
		return -1, fmt.Errorf("field %s not found in schema", fieldName)
	}
	val, err := parseValue(parts[1], field)
	if err != nil {
		return -1, fmt.Errorf("field %s: %w", fieldName, err)
	}
	row[fieldName] = val
	idx, ok := sch.FieldIndex(fieldName)
	if !ok {
		return -1, nil
	}
	return idx, nil
}

func normalizeAssignmentTarget(token string) string {
	trimmed := strings.TrimSpace(token)
	refParts := strings.Split(trimmed, ":")
	if len(refParts) >= 2 {
		return refParts[1]
	}
	return refParts[0]
}

func findFieldByName(sch *Schema, name string) *Field {
	for i := range sch.Fields {
		if sch.Fields[i].Name == name {
			return &sch.Fields[i]
		}
	}
	return nil
}

func parseCSVLine(line string) []string {
	var fields []string
	var current strings.Builder
	inQuote := false
	quote := rune(0)

	for _, r := range line {
		switch {
		case (r == '"' || r == '\'') && !inQuote:
			inQuote = true
			quote = r
			current.WriteRune(r)
		case r == quote && inQuote:
			inQuote = false
			current.WriteRune(r)
			quote = 0
		case r == ',' && !inQuote:
			fields = append(fields, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		fields = append(fields, strings.TrimSpace(current.String()))
	}
	return fields
}

func parseValue(raw string, field *Field) (interface{}, error) {
	raw = strings.TrimSpace(raw)
	if field == nil {
		return raw, nil
	}

	kind := field.ValueKind()

	switch kind {
	case KindUint64:
		var v uint64
		_, err := fmt.Sscanf(raw, "%d", &v)
		if err != nil {
			return nil, fmt.Errorf("invalid uint64: %q", raw)
		}
		return v, nil

	case KindInt64:
		var v int64
		_, err := fmt.Sscanf(raw, "%d", &v)
		if err != nil {
			return nil, fmt.Errorf("invalid int64: %q", raw)
		}
		return v, nil

	case KindFloat64:
		var v float64
		_, err := fmt.Sscanf(raw, "%f", &v)
		if err != nil {
			return nil, fmt.Errorf("invalid float64: %q", raw)
		}
		return v, nil

	case KindBool:
		lower := strings.ToLower(raw)
		if lower == "true" || lower == "1" {
			return true, nil
		}
		if lower == "false" || lower == "0" {
			return false, nil
		}
		return nil, fmt.Errorf("invalid bool: %q", raw)

	case KindString:
		if len(raw) >= 2 && (raw[0] == '"' || raw[0] == '\'') {
			unquoted := raw[1 : len(raw)-1]
			return unquoted, nil
		}
		return raw, nil

	case KindBytes:
		if len(raw) >= 2 && (raw[0] == '"' || raw[0] == '\'') {
			unquoted := raw[1 : len(raw)-1]
			return []byte(unquoted), nil
		}
		return []byte(raw), nil

	case KindDate:
		val, err := temporal.ParseDate(raw)
		if err != nil {
			return nil, err
		}
		return val, nil

	case KindDateTime:
		val, err := temporal.ParseDateTime(raw)
		if err != nil {
			return nil, err
		}
		return val, nil

	case KindTimestamp:
		val, err := temporal.ParseTimestamp(raw)
		if err != nil {
			return nil, err
		}
		return val, nil

	case KindTimestampTZ:
		val, err := temporal.ParseTimestampTZ(raw)
		if err != nil {
			return nil, err
		}
		return val, nil

	case KindDuration:
		val, err := temporal.ParseDuration(raw)
		if err != nil {
			return nil, err
		}
		return val, nil

	case KindRef:
		return raw, fmt.Errorf("unresolved ref kind for value %q", raw)
	default:
		return raw, nil
	}
}
