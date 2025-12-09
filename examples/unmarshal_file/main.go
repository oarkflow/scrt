package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/scrt/schema"
)

type User struct {
	ID    uint64
	Name  string
	Email string
}

type Message struct {
	MsgID uint64
	User  uint64 // ref to User.ID
	Text  string
	Lang  string
}

func main() {
	// Parse the schema file
	doc, err := schema.ParseFile("../../data.scrt")
	if err != nil {
		log.Fatalf("failed to parse schema: %v", err)
	}

	// Get User schema
	userSchema, ok := doc.Schema("User")
	if !ok {
		log.Fatal("User schema not found")
	}

	// Get Message schema
	msgSchema, ok := doc.Schema("Message")
	if !ok {
		log.Fatal("Message schema not found")
	}

	// Print schema information
	fmt.Println("=== User Schema ===")
	fmt.Printf("Name: %s\n", userSchema.Name)
	fmt.Printf("Fingerprint: %d\n", userSchema.Fingerprint())
	fmt.Println("Fields:")
	for _, field := range userSchema.Fields {
		fmt.Printf("  - %s: %s (kind=%d)\n", field.Name, field.RawType, field.Kind)
		if field.AutoIncrement {
			fmt.Printf("    [auto_increment]\n")
		}
	}

	fmt.Println("\n=== Message Schema ===")
	fmt.Printf("Name: %s\n", msgSchema.Name)
	fmt.Printf("Fingerprint: %d\n", msgSchema.Fingerprint())
	fmt.Println("Fields:")
	for _, field := range msgSchema.Fields {
		fmt.Printf("  - %s: %s (kind=%d)\n", field.Name, field.RawType, field.Kind)
		if field.AutoIncrement {
			fmt.Printf("    [auto_increment]\n")
		}
		if field.Kind == schema.KindRef {
			fmt.Printf("    [ref -> %s.%s]\n", field.TargetSchema, field.TargetField)
		}
	}

	// Get parsed data
	userRecords, hasUserData := doc.Records("User")
	msgRecords, hasMsgData := doc.Records("Message")

	fmt.Println("\n=== User Data ===")
	if hasUserData && len(userRecords) > 0 {
		fmt.Printf("Found %d user record(s):\n", len(userRecords))
		for i, record := range userRecords {
			fmt.Printf("  [%d] ID=%v Name=%q Email=%q\n",
				i, record["ID"], record["Name"], record["Email"])
		}
	} else {
		fmt.Println("No user data found")
	}

	fmt.Println("\n=== Message Data ===")
	if hasMsgData && len(msgRecords) > 0 {
		fmt.Printf("Found %d message record(s):\n", len(msgRecords))
		for i, record := range msgRecords {
			fmt.Printf("  [%d] MsgID=%v User=%v Text=%q Lang=%q\n",
				i, record["MsgID"], record["User"], record["Text"], record["Lang"])
		}
	} else {
		fmt.Println("No message data found")
	}
}
