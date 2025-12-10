package schema

import "fmt"

// finalize resolves reference kinds and pending defaults after parsing.
func (d *Document) finalize() error {
	if d == nil {
		return nil
	}
	for _, sch := range d.Schemas {
		if err := d.resolveSchemaKinds(sch); err != nil {
			return err
		}
	}
	return nil
}

func (d *Document) resolveSchemaKinds(s *Schema) error {
	if s == nil {
		return nil
	}
	for i := range s.Fields {
		if _, err := d.resolveFieldKind(s, i, make(map[string]bool)); err != nil {
			return err
		}
	}
	return nil
}

func (d *Document) resolveFieldKind(s *Schema, idx int, stack map[string]bool) (FieldKind, error) {
	field := &s.Fields[idx]
	if field.ResolvedKind != KindInvalid {
		return field.ResolvedKind, nil
	}
	if field.Kind != KindRef {
		field.ResolvedKind = field.Kind
		if field.pendingDefault != "" && field.Default == nil {
			def, err := parseDefaultLiteral(field.ResolvedKind, field.pendingDefault)
			if err != nil {
				return KindInvalid, fmt.Errorf("scrt: schema %s field %s default: %w", s.Name, field.Name, err)
			}
			field.Default = def
			field.pendingDefault = ""
		}
		return field.ResolvedKind, nil
	}

	key := s.Name + "." + field.Name
	if stack[key] {
		return KindInvalid, fmt.Errorf("scrt: circular reference detected for %s", key)
	}
	stack[key] = true
	targetSchema, ok := d.Schemas[field.TargetSchema]
	if !ok {
		return KindInvalid, fmt.Errorf("scrt: schema %s field %s references unknown schema %s", s.Name, field.Name, field.TargetSchema)
	}
	targetIdx, ok := targetSchema.FieldIndex(field.TargetField)
	if !ok {
		return KindInvalid, fmt.Errorf("scrt: schema %s field %s references unknown field %s.%s", s.Name, field.Name, field.TargetSchema, field.TargetField)
	}
	kind, err := d.resolveFieldKind(targetSchema, targetIdx, stack)
	if err != nil {
		return KindInvalid, err
	}
	field.ResolvedKind = kind
	delete(stack, key)

	if field.pendingDefault != "" && field.Default == nil {
		def, err := parseDefaultLiteral(field.ResolvedKind, field.pendingDefault)
		if err != nil {
			return KindInvalid, fmt.Errorf("scrt: schema %s field %s default: %w", s.Name, field.Name, err)
		}
		field.Default = def
		field.pendingDefault = ""
	}
	return kind, nil
}
