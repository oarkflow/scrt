package codec_test

import (
	"bytes"
	"testing"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

func buildTestSchema() *schema.Schema {
	return &schema.Schema{
		Name: "Message",
		Fields: []schema.Field{
			{Name: "MsgID", Kind: schema.KindUint64, RawType: "uint64"},
			{Name: "User", Kind: schema.KindRef, RawType: "ref:User:ID", TargetSchema: "User", TargetField: "ID"},
			{Name: "Text", Kind: schema.KindString, RawType: "string"},
			{Name: "Lang", Kind: schema.KindString, RawType: "string"},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	sch := buildTestSchema()
	var buf bytes.Buffer
	writer := codec.NewWriter(&buf, sch, 2)

	row := codec.NewRow(sch)
	if err := row.SetUint("MsgID", 1); err != nil {
		t.Fatal(err)
	}
	if err := row.SetUint("User", 1001); err != nil {
		t.Fatal(err)
	}
	if err := row.SetString("Text", "Hello World!"); err != nil {
		t.Fatal(err)
	}
	if err := row.SetString("Lang", "en"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteRow(row); err != nil {
		t.Fatalf("write row: %v", err)
	}

	row.Reset()
	if err := row.SetUint("MsgID", 2); err != nil {
		t.Fatal(err)
	}
	if err := row.SetUint("User", 1002); err != nil {
		t.Fatal(err)
	}
	if err := row.SetString("Text", "Bye World!"); err != nil {
		t.Fatal(err)
	}
	if err := row.SetString("Lang", "en"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteRow(row); err != nil {
		t.Fatalf("write row: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader := codec.NewReader(bytes.NewReader(buf.Bytes()), sch)
	decoded := codec.NewRow(sch)

	ok, err := reader.ReadRow(decoded)
	if err != nil || !ok {
		t.Fatalf("read first row: ok=%v err=%v", ok, err)
	}
	if decoded.Values()[0].Uint != 1 || decoded.Values()[2].Str != "Hello World!" {
		t.Fatalf("unexpected first row: %+v", decoded.Values())
	}

	ok, err = reader.ReadRow(decoded)
	if err != nil || !ok {
		t.Fatalf("read second row: ok=%v err=%v", ok, err)
	}
	if decoded.Values()[0].Uint != 2 || decoded.Values()[2].Str != "Bye World!" {
		t.Fatalf("unexpected second row: %+v", decoded.Values())
	}

	ok, err = reader.ReadRow(decoded)
	if err != nil {
		t.Fatalf("read eof: %v", err)
	}
	if ok {
		t.Fatalf("expected eof")
	}
}
