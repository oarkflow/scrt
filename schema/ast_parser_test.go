package schema_test

import (
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestParseASTBasic(t *testing.T) {
	src := `@schema:User
@field ID uint64 serial pk readonly
@field Email string format:email
@unique(ID, Email)

@schema:Order
@field ID uint64 serial pk
@field UserID ref:User:ID onDelete:cascade onUpdate:restrict
@field Tags []string
@field Meta map[string]string
@field Shipping object:Address
@relation UserID User.ID onDelete:restrict onUpdate:cascade
`
	doc, err := schema.ParseAST(src)
	if err != nil {
		t.Fatalf("parse ast: %v", err)
	}
	if len(doc.Schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(doc.Schemas))
	}
	if len(doc.Schemas[0].Indexes) != 1 || !doc.Schemas[0].Indexes[0].Unique {
		t.Fatalf("expected unique index in first schema")
	}
	if len(doc.Schemas[1].Relations) != 1 {
		t.Fatalf("expected one explicit relation in second schema")
	}
}
