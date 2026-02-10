package schema_test

import (
	"strings"
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestParseFunctionsAndQueries(t *testing.T) {
	src := `
@function:MyFunc(a int, b string) string
	return a + b
	// some comment

@query:GetUser(id int)
	SELECT * FROM users
	WHERE id = :id

@schema:Dummy
@field ID int

@function:AnotherFunc() void
	print("hello")
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(doc.Functions) != 2 {
		t.Fatalf("expected 2 functions, got %d", len(doc.Functions))
	}
	f1, ok := doc.Functions["MyFunc"]
	if !ok {
		t.Fatalf("MyFunc missing")
	}
	if len(f1.Args) != 2 {
		t.Fatalf("MyFunc args mismatch")
	}
	if f1.Args[0].Name != "a" || f1.Args[0].Type != "int" {
		t.Fatalf("MyFunc arg0 mismatch")
	}
	if f1.ReturnType != "string" {
		t.Fatalf("MyFunc return type mismatch")
	}
	if !strings.Contains(f1.Body, "return a + b") {
		t.Fatalf("MyFunc body mismatch: %q", f1.Body)
	}

	q1, ok := doc.Queries["GetUser"]
	if !ok {
		t.Fatalf("GetUser missing")
	}
	if len(q1.Args) != 1 {
		t.Fatalf("GetUser args mismatch")
	}
	if !strings.Contains(q1.SQL, "SELECT * FROM users") {
		t.Fatalf("GetUser SQL mismatch: %q", q1.SQL)
	}
}
