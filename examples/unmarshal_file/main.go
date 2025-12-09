package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/scrt"
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

type Incident struct {
	ID         uint64
	Title      string
	Started    time.Time
	Alerted    time.Time
	Resolved   time.Time
	ReportDate time.Time
	SLA        time.Duration
}

func main() {
	// Parse the schema file
	doc, err := schema.ParseFile("data.scrt")
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

	incidentSchema, hasIncidentSchema := doc.Schema("Incident")

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

	if hasIncidentSchema {
		fmt.Println("\n=== Incident Schema ===")
		fmt.Printf("Name: %s\n", incidentSchema.Name)
		fmt.Printf("Fingerprint: %d\n", incidentSchema.Fingerprint())
		fmt.Println("Fields:")
		for _, field := range incidentSchema.Fields {
			fmt.Printf("  - %s: %s (kind=%d)\n", field.Name, field.RawType, field.Kind)
		}
	}

	// Get parsed data
	userRecords, hasUserData := doc.Records("User")
	msgRecords, hasMsgData := doc.Records("Message")
	incidentRecords, hasIncidentData := doc.Records("Incident")

	var users []User
	var messages []Message
	var incidents []Incident

	if hasUserData && len(userRecords) > 0 {
		users = decodeUsers(userSchema, userRecords)
	}
	if hasMsgData && len(msgRecords) > 0 {
		messages = decodeMessages(msgSchema, msgRecords)
	}
	if hasIncidentSchema && hasIncidentData && len(incidentRecords) > 0 {
		incidents = decodeIncidents(incidentSchema, incidentRecords)
	}

	fmt.Println("\n=== User Data ===")
	if len(users) > 0 {
		fmt.Printf("Found %d user record(s):\n", len(users))
		for i, user := range users {
			fmt.Printf("  [%d] ID=%d Name=%q Email=%q\n", i, user.ID, user.Name, user.Email)
		}
	} else {
		fmt.Println("No user data found")
	}

	fmt.Println("\n=== Message Data ===")
	if len(messages) > 0 {
		fmt.Printf("Found %d message record(s):\n", len(messages))
		for i, message := range messages {
			fmt.Printf("  [%d] MsgID=%d User=%d Text=%q Lang=%q\n",
				i, message.MsgID, message.User, message.Text, message.Lang)
		}
	} else {
		fmt.Println("No message data found")
	}

	fmt.Println("\n=== Incident Data (Temporal Fields) ===")
	if len(incidents) > 0 {
		for _, incident := range incidents {
			fmt.Printf("  #%d %s\n", incident.ID, incident.Title)
			fmt.Printf("    start=%s | alert=%s | resolved=%s\n",
				incident.Started.Format(time.RFC3339),
				incident.Alerted.Format(time.RFC3339Nano),
				incident.Resolved.Format(time.RFC3339))
			fmt.Printf("    report=%s | sla=%s\n",
				incident.ReportDate.Format("2006-01-02"),
				incident.SLA)
		}
	} else {
		fmt.Println("No incident data found")
	}
}

func decodeUsers(s *schema.Schema, rows []map[string]interface{}) []User {
	payload := marshalRows(s, rows)
	var out []User
	if err := scrt.Unmarshal(payload, s, &out); err != nil {
		log.Fatalf("unmarshal users: %v", err)
	}
	return out
}

func decodeMessages(s *schema.Schema, rows []map[string]interface{}) []Message {
	payload := marshalRows(s, rows)
	var out []Message
	if err := scrt.Unmarshal(payload, s, &out); err != nil {
		log.Fatalf("unmarshal messages: %v", err)
	}
	return out
}

func decodeIncidents(s *schema.Schema, rows []map[string]interface{}) []Incident {
	payload := marshalRows(s, rows)
	var out []Incident
	if err := scrt.Unmarshal(payload, s, &out); err != nil {
		log.Fatalf("unmarshal incidents: %v", err)
	}
	return out
}

func marshalRows(s *schema.Schema, rows []map[string]interface{}) []byte {
	payload, err := scrt.Marshal(s, rows)
	if err != nil {
		log.Fatalf("marshal %s rows: %v", s.Name, err)
	}
	return payload
}
