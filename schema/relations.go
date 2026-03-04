package schema

import "strings"

type IncomingRelation struct {
	SourceSchema string
	Relation     Relation
}

// IncomingRelations returns all relations that target the provided schema/field.
// If targetField is empty, all relations targeting targetSchema are returned.
func (d *Document) IncomingRelations(targetSchema, targetField string) []IncomingRelation {
	if d == nil || targetSchema == "" {
		return nil
	}
	var out []IncomingRelation
	for sourceName, sch := range d.Schemas {
		if sch == nil {
			continue
		}
		for _, rel := range sch.Relations {
			if !strings.EqualFold(rel.TargetSchema, targetSchema) {
				continue
			}
			if targetField != "" && !strings.EqualFold(rel.TargetField, targetField) {
				continue
			}
			out = append(out, IncomingRelation{SourceSchema: sourceName, Relation: rel})
		}
	}
	return out
}
