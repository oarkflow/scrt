package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
)

type Message struct {
	MsgID uint64
	User  uint64
	Text  string
	Lang  string
	Seen  bool
}

func main() {
	sch := &schema.Schema{
		Name: "Message",
		Fields: []schema.Field{
			{Name: "MsgID", Kind: schema.KindUint64, RawType: "uint64"},
			{Name: "User", Kind: schema.KindRef, RawType: "ref:User:ID"},
			{Name: "Text", Kind: schema.KindString, RawType: "string"},
			{Name: "Lang", Kind: schema.KindString, RawType: "string", Default: &schema.DefaultValue{Kind: schema.KindString, String: "en"}},
			{Name: "Seen", Kind: schema.KindBool, RawType: "bool", Default: &schema.DefaultValue{Kind: schema.KindBool, Bool: false}},
		},
	}

	rows := []map[string]any{
		{"MsgID": uint64(1), "User": uint64(1001), "Text": "Hello"},
		{"MsgID": uint64(2), "User": uint64(1002), "Text": "Bye", "Seen": true},
	}

	payload, err := scrt.Marshal(sch, rows)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	var messages []Message
	if err := scrt.Unmarshal(payload, sch, &messages); err != nil {
		log.Fatalf("unmarshal: %v", err)
	}

	for _, msg := range messages {
		fmt.Printf("%d -> %d %q (%s) seen=%v\n", msg.MsgID, msg.User, msg.Text, msg.Lang, msg.Seen)
	}
}
