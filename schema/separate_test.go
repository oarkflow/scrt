package schema_test

import (
	"strings"
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestSeparateLoading(t *testing.T) {
	definitions := `
@schema:User
@field ID int pk
@field Name string
`
	data := `
@User
1, "Alice"
2, "Bob"
`

	doc := schema.NewDocument()
	if err := doc.Load(strings.NewReader(definitions)); err != nil {
		t.Fatalf("failed to load definitions: %v", err)
	}

	if err := doc.Load(strings.NewReader(data)); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	if len(doc.Schemas) != 1 {
		t.Errorf("expected 1 schema, got %d", len(doc.Schemas))
	}

	users, ok := doc.Data["User"]
	if !ok {
		t.Fatal("expected User data")
	}
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
}

func TestInlineLoading(t *testing.T) {
	content := `
@schema:User
@field ID int pk
@field Name string

@User
1, "Alice"
2, "Bob"
`
	doc := schema.NewDocument()
	if err := doc.Load(strings.NewReader(content)); err != nil {
		t.Fatalf("failed to load inline content: %v", err)
	}

	if len(doc.Schemas) != 1 {
		t.Errorf("expected 1 schema, got %d", len(doc.Schemas))
	}
	users, ok := doc.Data["User"]
	if !ok {
		t.Fatal("expected User data")
	}
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
}
