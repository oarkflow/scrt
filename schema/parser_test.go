package schema_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/temporal"
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
@schema Other
@field ID uint64
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

func TestParseAutoIncrementDataFlexibility(t *testing.T) {
	src := `@schema Message
@field MsgID uint64 auto_increment
@field User ref:User:ID
@field Text string
@field Lang string

@schema User
@field ID uint64 auto_increment
@field Name string

@Message
1001, "Hello World!", "en"
@Message
@MsgID=77, 1002, "Hi again", "en"

@User
1001, "John Doe"
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	messageRows, ok := doc.Records("Message")
	if !ok {
		t.Fatalf("missing Message records")
	}
	if len(messageRows) != 2 {
		t.Fatalf("expected 2 message rows, got %d", len(messageRows))
	}
	if _, exists := messageRows[0]["MsgID"]; exists {
		t.Fatalf("unexpected MsgID value for auto-increment row")
	}
	if user, ok := messageRows[0]["User"].(uint64); !ok || user != 1001 {
		t.Fatalf("expected User=1001, got %+v", messageRows[0]["User"])
	}
	if msgID, ok := messageRows[1]["MsgID"].(uint64); !ok || msgID != 77 {
		t.Fatalf("expected MsgID override 77, got %+v", messageRows[1]["MsgID"])
	}
	if user, ok := messageRows[1]["User"].(uint64); !ok || user != 1002 {
		t.Fatalf("expected User=1002, got %+v", messageRows[1]["User"])
	}
	userRows, ok := doc.Records("User")
	if !ok || len(userRows) != 1 {
		t.Fatalf("expected 1 user row, got %d", len(userRows))
	}
	if id, ok := userRows[0]["ID"].(uint64); !ok || id != 1001 {
		t.Fatalf("expected explicit user ID, got %+v", userRows[0]["ID"])
	}
}

func TestParseTemporalDefaults(t *testing.T) {
	src := `@schema Temporal
@field Day date default=2025-01-02
@field Seen datetime default="2025-01-02 15:04:05"
@field Stamp timestamptz default="2025-01-02T15:04:05-05:00"
@field Epoch timestamp default=1704210000
@field Window duration default=1d2h
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sch, ok := doc.Schema("Temporal")
	if !ok {
		t.Fatalf("schema missing")
	}
	checks := []struct {
		name   string
		kind   schema.FieldKind
		expect func(*schema.DefaultValue) bool
	}{
		{"Day", schema.KindDate, func(v *schema.DefaultValue) bool {
			return v != nil && temporal.DecodeDate(v.Int).Equal(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
		}},
		{"Seen", schema.KindDateTime, func(v *schema.DefaultValue) bool {
			expected := time.Date(2025, 1, 2, 15, 4, 5, 0, time.UTC)
			return v != nil && temporal.DecodeInstant(v.Int).Equal(expected)
		}},
		{"Stamp", schema.KindTimestampTZ, func(v *schema.DefaultValue) bool {
			return v != nil && v.String != ""
		}},
		{"Epoch", schema.KindTimestamp, func(v *schema.DefaultValue) bool {
			expected := time.Unix(1704210000, 0).UTC()
			return v != nil && temporal.DecodeInstant(v.Int).Equal(expected)
		}},
		{"Window", schema.KindDuration, func(v *schema.DefaultValue) bool {
			expected := 26 * time.Hour
			return v != nil && time.Duration(v.Int) == expected
		}},
	}
	for _, chk := range checks {
		f, ok := sch.FieldIndex(chk.name)
		if !ok {
			t.Fatalf("missing field %s", chk.name)
		}
		field := sch.Fields[f]
		if field.Kind != chk.kind {
			t.Fatalf("field %s kind mismatch: got %d", chk.name, field.Kind)
		}
		if !chk.expect(field.Default) {
			t.Fatalf("default mismatch for %s: %+v", chk.name, field.Default)
		}
	}
}
