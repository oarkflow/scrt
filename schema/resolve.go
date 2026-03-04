package schema

import (
	"fmt"
	"strings"
)

// finalize resolves reference kinds and pending defaults after parsing.
func (d *Document) finalize() error {
	if d == nil {
		return nil
	}
	for _, sch := range d.Schemas {
		if err := d.resolveSchemaKinds(sch); err != nil {
			return err
		}
		if err := d.resolveSchemaRelations(sch); err != nil {
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
		if err := d.resolveComplexField(s, i); err != nil {
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
		// Allow deferred cross-document references (e.g. schema documents uploaded separately).
		// In that case we keep ref metadata and fall back to uint64 value kind for now.
		field.ResolvedKind = KindUint64
		return field.ResolvedKind, nil
	}
	if field.TargetField == "" {
		inferred, err := inferReferenceTargetField(targetSchema)
		if err != nil {
			return KindInvalid, fmt.Errorf("scrt: schema %s field %s: %w", s.Name, field.Name, err)
		}
		field.TargetField = inferred
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

func inferReferenceTargetField(target *Schema) (string, error) {
	if target == nil {
		return "", fmt.Errorf("nil target schema")
	}
	for _, f := range target.Fields {
		if f.PrimaryKey {
			return f.Name, nil
		}
	}
	for _, f := range target.Fields {
		if strings.EqualFold(f.Name, "ID") {
			return f.Name, nil
		}
	}
	return "", fmt.Errorf("cannot infer reference field for schema %s; add a primary key, an ID field, or use explicit ref:%s:<Field>", target.Name, target.Name)
}

func (d *Document) resolveSchemaRelations(s *Schema) error {
	if s == nil {
		return nil
	}
	seen := make(map[string]bool, len(s.Relations))
	for _, rel := range s.Relations {
		seen[strings.ToLower(rel.Field)] = true
	}
	// Inline relation actions on reference fields are treated as relation declarations.
	for _, field := range s.Fields {
		if field.Kind != KindRef || field.TargetSchema == "" || field.TargetField == "" {
			continue
		}
		if field.OnDelete == "" && field.OnUpdate == "" {
			continue
		}
		key := strings.ToLower(field.Name)
		if seen[key] {
			continue
		}
		onDelete := field.OnDelete
		if onDelete == "" {
			onDelete = "restrict"
		}
		onUpdate := field.OnUpdate
		if onUpdate == "" {
			onUpdate = "restrict"
		}
		s.Relations = append(s.Relations, Relation{
			Name:         field.Name,
			Field:        field.Name,
			TargetSchema: field.TargetSchema,
			TargetField:  field.TargetField,
			OnDelete:     strings.ToLower(onDelete),
			OnUpdate:     strings.ToLower(onUpdate),
		})
		seen[key] = true
	}
	for i := range s.Relations {
		rel := &s.Relations[i]
		if rel.OnDelete == "" {
			rel.OnDelete = "restrict"
		}
		if rel.OnUpdate == "" {
			rel.OnUpdate = "restrict"
		}
		if !isValidRelationAction(rel.OnDelete) {
			return fmt.Errorf("scrt: schema %s relation %s has invalid onDelete action %s", s.Name, rel.Name, rel.OnDelete)
		}
		if !isValidRelationAction(rel.OnUpdate) {
			return fmt.Errorf("scrt: schema %s relation %s has invalid onUpdate action %s", s.Name, rel.Name, rel.OnUpdate)
		}
		if rel.Field == "" || rel.TargetSchema == "" || rel.TargetField == "" {
			return fmt.Errorf("scrt: schema %s has invalid relation %+v", s.Name, *rel)
		}
		field, ok := s.FieldByName(rel.Field)
		if !ok {
			return fmt.Errorf("scrt: schema %s relation %s references unknown field %s", s.Name, rel.Name, rel.Field)
		}
		targetSchema, ok := d.Schemas[rel.TargetSchema]
		if ok {
			if _, ok := targetSchema.FieldByName(rel.TargetField); !ok {
				return fmt.Errorf("scrt: schema %s relation %s references unknown field %s.%s", s.Name, rel.Name, rel.TargetSchema, rel.TargetField)
			}
		}
		if field.Kind == KindRef {
			if field.TargetSchema == "" {
				field.TargetSchema = rel.TargetSchema
			}
			if field.TargetField == "" {
				field.TargetField = rel.TargetField
			}
		}
	}
	return nil
}

func (d *Document) resolveComplexField(s *Schema, idx int) error {
	field := &s.Fields[idx]
	if field.IsObject {
		if field.ObjectSchema == "" {
			return fmt.Errorf("scrt: schema %s field %s object type is empty", s.Name, field.Name)
		}
		if _, ok := d.Schemas[field.ObjectSchema]; !ok {
			return fmt.Errorf("scrt: schema %s field %s references unknown object schema %s", s.Name, field.Name, field.ObjectSchema)
		}
	}
	if field.IsArray && field.ArrayElemType == "" {
		return fmt.Errorf("scrt: schema %s field %s array element type is empty", s.Name, field.Name)
	}
	if field.IsMap && (field.MapKeyType == "" || field.MapValueType == "") {
		return fmt.Errorf("scrt: schema %s field %s map key/value type must be set", s.Name, field.Name)
	}
	return nil
}

func isValidRelationAction(action string) bool {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "restrict", "cascade", "set_null", "no_action":
		return true
	default:
		return false
	}
}
