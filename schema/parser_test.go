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

func TestParseCustomSchemaTypeShorthand(t *testing.T) {
	src := `@schema:User
@field ID uint64 serial
@field Name string

@schema:Product
@field SKU string pk
@field Name string

@schema:Order
@field OrderID uint64 serial
@field Customer User
@field Product Product
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	order, ok := doc.Schema("Order")
	if !ok {
		t.Fatalf("order schema missing")
	}
	customerIdx, ok := order.FieldIndex("Customer")
	if !ok {
		t.Fatalf("customer field missing")
	}
	customer := order.Fields[customerIdx]
	if customer.Kind != schema.KindRef {
		t.Fatalf("expected Customer kind ref, got %d", customer.Kind)
	}
	if customer.TargetSchema != "User" || customer.TargetField != "ID" {
		t.Fatalf("expected Customer target User.ID, got %s.%s", customer.TargetSchema, customer.TargetField)
	}
	if customer.ValueKind() != schema.KindUint64 {
		t.Fatalf("expected Customer value kind uint64, got %d", customer.ValueKind())
	}

	productIdx, ok := order.FieldIndex("Product")
	if !ok {
		t.Fatalf("product field missing")
	}
	product := order.Fields[productIdx]
	if product.Kind != schema.KindRef {
		t.Fatalf("expected Product kind ref, got %d", product.Kind)
	}
	if product.TargetSchema != "Product" || product.TargetField != "SKU" {
		t.Fatalf("expected Product target Product.SKU, got %s.%s", product.TargetSchema, product.TargetField)
	}
	if product.ValueKind() != schema.KindString {
		t.Fatalf("expected Product value kind string, got %d", product.ValueKind())
	}
}

func TestParseAdvancedFieldConstraints(t *testing.T) {
	src := `@schema:Account
@field ID uint64 serial pk
@field Username string minlength:3 maxlength:32 pattern:"^[a-z0-9_]+$" description:"Public handle"
@field Email string format:email unique
@field Status string enum:"Active|Inactive|Blocked" default="Active"
@field Age ?int min:0 max:150
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	account, ok := doc.Schema("Account")
	if !ok {
		t.Fatalf("schema missing")
	}
	usernameIdx, _ := account.FieldIndex("Username")
	username := account.Fields[usernameIdx]
	if username.MinLength == nil || *username.MinLength != 3 {
		t.Fatalf("expected Username minlength=3, got %+v", username.MinLength)
	}
	if username.MaxLength == nil || *username.MaxLength != 32 {
		t.Fatalf("expected Username maxlength=32, got %+v", username.MaxLength)
	}
	if username.Pattern != "^[a-z0-9_]+$" {
		t.Fatalf("unexpected Username pattern %q", username.Pattern)
	}
	if username.Description != "Public handle" {
		t.Fatalf("unexpected Username description %q", username.Description)
	}

	emailIdx, _ := account.FieldIndex("Email")
	email := account.Fields[emailIdx]
	if email.Format != "email" {
		t.Fatalf("expected Email format=email, got %q", email.Format)
	}

	statusIdx, _ := account.FieldIndex("Status")
	status := account.Fields[statusIdx]
	if len(status.Enum) != 3 {
		t.Fatalf("expected 3 enum values, got %d", len(status.Enum))
	}
	if status.Enum[0] != "Active" || status.Enum[2] != "Blocked" {
		t.Fatalf("unexpected enum values: %+v", status.Enum)
	}

	ageIdx, _ := account.FieldIndex("Age")
	age := account.Fields[ageIdx]
	if !age.Nullable {
		t.Fatalf("expected Age nullable=true")
	}
	if age.Minimum == nil || *age.Minimum != 0 {
		t.Fatalf("expected Age min=0, got %+v", age.Minimum)
	}
	if age.Maximum == nil || *age.Maximum != 150 {
		t.Fatalf("expected Age max=150, got %+v", age.Maximum)
	}
}

func TestValidateRowWithAdvancedConstraints(t *testing.T) {
	src := `@schema:Account
@field ID uint64 serial pk
@field Username string minlength:3 maxlength:16 pattern:"^[a-z0-9_]+$"
@field Email string format:email
@field Status string enum:"Active|Inactive"
@field Age ?int min:0 max:150
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	account, ok := doc.Schema("Account")
	if !ok {
		t.Fatalf("schema missing")
	}

	valid := map[string]interface{}{
		"Username": "john_01",
		"Email":    "john@example.com",
		"Status":   "Active",
		"Age":      int64(33),
	}
	if err := account.ValidateRow(valid); err != nil {
		t.Fatalf("expected valid row, got %v", err)
	}

	invalid := map[string]interface{}{
		"Username": "ab",
		"Email":    "not-an-email",
		"Status":   "Paused",
		"Age":      int64(200),
	}
	if err := account.ValidateRow(invalid); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestParseComplexTypesAndRelations(t *testing.T) {
	src := `@schema:User
@field ID uint64 serial pk
@field Name string

@schema:Order
@field ID uint64 serial pk
@field CustomerID ref:User:ID
@field Tags []string
@field Metadata map[string]string
@field Shipping object:Address
@relation CustomerID User.ID onDelete:cascade onUpdate:restrict

@schema:Address
@field Street string
@field City string
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	order, ok := doc.Schema("Order")
	if !ok {
		t.Fatalf("order schema missing")
	}
	tagsIdx, _ := order.FieldIndex("Tags")
	if !order.Fields[tagsIdx].IsArray || order.Fields[tagsIdx].ArrayElemType != "string" {
		t.Fatalf("expected Tags to be []string, got %+v", order.Fields[tagsIdx])
	}
	metaIdx, _ := order.FieldIndex("Metadata")
	if !order.Fields[metaIdx].IsMap || order.Fields[metaIdx].MapKeyType != "string" || order.Fields[metaIdx].MapValueType != "string" {
		t.Fatalf("expected Metadata to be map[string]string, got %+v", order.Fields[metaIdx])
	}
	shipIdx, _ := order.FieldIndex("Shipping")
	if !order.Fields[shipIdx].IsObject || order.Fields[shipIdx].ObjectSchema != "Address" {
		t.Fatalf("expected Shipping to be object:Address, got %+v", order.Fields[shipIdx])
	}
	if len(order.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(order.Relations))
	}
	rel := order.Relations[0]
	if rel.Field != "CustomerID" || rel.TargetSchema != "User" || rel.TargetField != "ID" {
		t.Fatalf("unexpected relation target %+v", rel)
	}
	if rel.OnDelete != "cascade" || rel.OnUpdate != "restrict" {
		t.Fatalf("unexpected relation actions %+v", rel)
	}
}

func TestInlineRelationActionsOnField(t *testing.T) {
	src := `@schema:User
@field ID uint64 serial pk
@field Name string

@schema:Order
@field ID uint64 serial pk
@field CustomerID ref:User:ID onDelete:cascade onUpdate:restrict
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	order, ok := doc.Schema("Order")
	if !ok {
		t.Fatalf("order schema missing")
	}
	if len(order.Relations) != 1 {
		t.Fatalf("expected synthesized relation, got %d", len(order.Relations))
	}
	rel := order.Relations[0]
	if rel.Field != "CustomerID" || rel.TargetSchema != "User" || rel.TargetField != "ID" {
		t.Fatalf("unexpected relation target %+v", rel)
	}
	if rel.OnDelete != "cascade" || rel.OnUpdate != "restrict" {
		t.Fatalf("unexpected relation actions %+v", rel)
	}
}

func TestParseUniqueReadonlyAndNowDefault(t *testing.T) {
	src := `@schema:Account
@field ID uint64 serial pk readonly
@field Name string
@field Email string
@field CreatedAt timestamp default:now() readonly
@unique(Name, Email)
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	account, ok := doc.Schema("Account")
	if !ok {
		t.Fatalf("schema missing")
	}
	if len(account.Indexes) != 1 {
		t.Fatalf("expected 1 index from @unique, got %d", len(account.Indexes))
	}
	idx := account.Indexes[0]
	if !idx.Unique {
		t.Fatalf("expected unique index")
	}
	if len(idx.Fields) != 2 || idx.Fields[0] != "Name" || idx.Fields[1] != "Email" {
		t.Fatalf("unexpected unique fields %+v", idx.Fields)
	}
	createdIdx, _ := account.FieldIndex("CreatedAt")
	created := account.Fields[createdIdx]
	if !created.ReadOnly {
		t.Fatalf("expected CreatedAt readonly")
	}
	if created.Default == nil || created.Default.Expression != "now()" {
		t.Fatalf("expected CreatedAt default expression now(), got %+v", created.Default)
	}
}
