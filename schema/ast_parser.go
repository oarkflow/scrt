package schema

import (
	"fmt"
	"strings"
)

// ParseAST parses SCRT DSL into a lightweight AST using a tokenized line parser.
func ParseAST(src string) (*ASTDocument, error) {
	doc := &ASTDocument{Schemas: make([]ASTSchema, 0)}
	var current *ASTSchema
	lines := strings.Split(src, "\n")
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "@") {
			continue
		}
		tokens := LexLine(line)
		if len(tokens) < 2 || tokens[0].Type != ASTTokenAt || tokens[1].Type != ASTTokenWord {
			continue
		}
		kw := strings.ToLower(tokens[1].Literal)
		switch kw {
		case "schema":
			name, err := astParseSchemaName(line)
			if err != nil {
				return nil, err
			}
			doc.Schemas = append(doc.Schemas, ASTSchema{Name: name})
			current = &doc.Schemas[len(doc.Schemas)-1]
		case "field":
			if current == nil {
				return nil, fmt.Errorf("@field outside schema")
			}
			field, err := astParseField(line)
			if err != nil {
				return nil, err
			}
			current.Fields = append(current.Fields, field)
		case "index":
			if current == nil {
				return nil, fmt.Errorf("@index outside schema")
			}
			idx, err := astParseIndex(line)
			if err != nil {
				return nil, err
			}
			current.Indexes = append(current.Indexes, idx)
		case "unique":
			if current == nil {
				return nil, fmt.Errorf("@unique outside schema")
			}
			uniq, err := astParseUnique(line)
			if err != nil {
				return nil, err
			}
			current.Indexes = append(current.Indexes, uniq)
		case "relation":
			if current == nil {
				return nil, fmt.Errorf("@relation outside schema")
			}
			rel, err := astParseRelation(line)
			if err != nil {
				return nil, err
			}
			current.Relations = append(current.Relations, rel)
		default:
			// Ignore other directives in AST parser for now.
		}
	}
	return doc, nil
}

func astParseSchemaName(line string) (string, error) {
	body := strings.TrimSpace(strings.TrimPrefix(line, "@schema"))
	if strings.HasPrefix(body, ":") {
		body = strings.TrimSpace(body[1:])
	}
	if body == "" {
		return "", fmt.Errorf("schema name missing")
	}
	return strings.Trim(body, "\"'`"), nil
}

func astParseField(line string) (ASTField, error) {
	// @ field Name Type [attrs...]
	body := strings.TrimSpace(strings.TrimPrefix(line, "@field"))
	name, typ, attrChunk, err := splitFieldParts(body)
	if err != nil {
		return ASTField{}, err
	}
	field := ASTField{Name: strings.Trim(name, "\"'`"), Type: strings.Trim(typ, "\"'`")}
	for _, seg := range splitFieldAttributes(attrChunk) {
		attr := strings.Trim(seg, "\"'`")
		if attr == "" {
			continue
		}
		if strings.Contains(attr, ":") {
			parts := strings.SplitN(attr, ":", 2)
			field.Attributes = append(field.Attributes, ASTAttribute{Key: parts[0], Value: parts[1]})
			continue
		}
		if strings.Contains(attr, "=") {
			parts := strings.SplitN(attr, "=", 2)
			field.Attributes = append(field.Attributes, ASTAttribute{Key: parts[0], Value: parts[1]})
			continue
		}
		field.Attributes = append(field.Attributes, ASTAttribute{Key: attr})
	}
	if field.Name == "" || field.Type == "" {
		return ASTField{}, fmt.Errorf("invalid @field declaration")
	}
	return field, nil
}

func astParseIndex(line string) (ASTIndex, error) {
	body := strings.TrimSpace(strings.TrimPrefix(line, "@index"))
	idx, err := parseIndex(body)
	if err != nil {
		return ASTIndex{}, err
	}
	return ASTIndex{Name: idx.Name, Fields: append([]string(nil), idx.Fields...), Unique: idx.Unique}, nil
}

func astParseUnique(line string) (ASTIndex, error) {
	body := strings.TrimSpace(strings.TrimPrefix(line, "@unique"))
	idx, err := parseUnique(body)
	if err != nil {
		return ASTIndex{}, err
	}
	return ASTIndex{Name: idx.Name, Fields: append([]string(nil), idx.Fields...), Unique: idx.Unique}, nil
}

func astParseRelation(line string) (ASTRelation, error) {
	// @ relation Field Target [onDelete:* onUpdate:*]
	body := strings.TrimSpace(strings.TrimPrefix(line, "@relation"))
	rel, err := parseRelation(body)
	if err != nil {
		return ASTRelation{}, err
	}
	return ASTRelation{
		Field:        rel.Field,
		TargetSchema: rel.TargetSchema,
		TargetField:  rel.TargetField,
		OnDelete:     rel.OnDelete,
		OnUpdate:     rel.OnUpdate,
	}, nil
}
