package schema_test

import (
	"os"
	"strings"
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestParseSampleFile(t *testing.T) {
	f, err := os.Open("../data.scrt")
	if err != nil {
		t.Fatalf("open sample: %v", err)
	}
	defer f.Close()

	doc, err := schema.Parse(f)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(doc.Schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(doc.Schemas))
	}

	msg, ok := doc.Schema("Message")
	if !ok {
		t.Fatalf("message schema missing")
	}
	if len(msg.Fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(msg.Fields))
	}
	if msg.Fields[0].AutoIncrement == false {
		t.Fatalf("expected auto increment on MsgID")
	}
	if _, ok := msg.FieldIndex("Text"); !ok {
		t.Fatalf("missing field index for Text")
	}
}

func TestParseDefaultsAndTypes(t *testing.T) {
	src := `@schema Example
@field Flag bool default=true
@field Count int64 default=-42
@field Score float64 default=3.14
@field Payload bytes default=0x4142
@field Label string default="Hello World"
@field Ref ref:Other:ID default=99
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	model, ok := doc.Schema("Example")
	if !ok {
		t.Fatalf("schema not found")
	}
	cases := []struct {
		name string
		kind schema.FieldKind
		chk  func(*schema.DefaultValue) bool
	}{
		{"Flag", schema.KindBool, func(v *schema.DefaultValue) bool { return v != nil && v.Bool }},
		{"Count", schema.KindInt64, func(v *schema.DefaultValue) bool { return v != nil && v.Int == -42 }},
		{"Score", schema.KindFloat64, func(v *schema.DefaultValue) bool { return v != nil && v.Float == 3.14 }},
		{"Payload", schema.KindBytes, func(v *schema.DefaultValue) bool { return v != nil && string(v.Bytes) == "AB" }},
		{"Label", schema.KindString, func(v *schema.DefaultValue) bool { return v != nil && v.String == "Hello World" }},
		{"Ref", schema.KindRef, func(v *schema.DefaultValue) bool { return v != nil && v.Uint == 99 }},
	}
	if len(model.Fields) != len(cases) {
		t.Fatalf("expected %d fields, got %d", len(cases), len(model.Fields))
	}
	for _, c := range cases {
		idx, ok := model.FieldIndex(c.name)
		if !ok {
			t.Fatalf("missing field %s", c.name)
		}
		f := model.Fields[idx]
		if f.Kind != c.kind {
			t.Fatalf("field %s kind mismatch", c.name)
		}
		if !c.chk(f.Default) {
			t.Fatalf("field %s default mismatch: %+v", c.name, f.Default)
		}
	}
}
