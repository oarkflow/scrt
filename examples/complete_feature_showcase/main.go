package main

import (
	"fmt"
	"os"

	"github.com/oarkflow/scrt/schema"
)

func main() {
	// ... (Same setup as before)
	f, err := os.Open("full_features.scrt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	doc, err := schema.Parse(f)
	if err != nil {
		panic(err)
	}

	fmt.Println("=== SCRT Interpreter Show case ===")
	// Initialize the interpreter from the schema package
	vm := schema.NewInterpreter(doc)

	// Calls using the interpreted body!

	fmt.Println("\n--- 1. Creating User ( Interpreted ) ---")
	user := vm.ExecuteFunction("CreateUser", "dave_interpreted", "dave@example.com")
	fmt.Printf("Result: %v\n", user)

	// Test Constraint Violation
	fmt.Println("\n--- 1b. Creating Duplicate User ( Should Fail ) ---")
	vm.ExecuteFunction("CreateUser", "dave_interpreted", "dave@example.com")

	fmt.Println("\n--- 2. Updating Stock ( Interpreted ) ---")
	// "UPDATE Product SET StockLevel = pQty WHERE SKU = pSku"
	// UpdateProductStock(pSku, pQty)
	vm.ExecuteFunction("UpdateProductStock", "WIDGET-X", 500)

	fmt.Println("\n--- 3. Deleting Order ( Interpreted ) ---")
	// "DELETE FROM Order WHERE OrderID = oID"
	vm.ExecuteFunction("DeleteOrder", 100)

	fmt.Println("\n--- 4. Calculating Discount ( Interpreted ) ---")
	// "price * percent / 100.0"
	// if price > 1000, subtract 20 extra
	disc := vm.ExecuteFunction("CalculateDiscount", 200.0, 15.0)
	fmt.Printf("Discount (200, 15%%): %.2f\n", disc)

	disc2 := vm.ExecuteFunction("CalculateDiscount", 2000.0, 10.0)
	fmt.Printf("Discount (2000, 10%%) [Expected 180]: %.2f\n", disc2)

	fmt.Println("\n--- 4. String concat ( Interpreted ) ---")
	name := vm.ExecuteFunction("FormatFullName", "John", "Wick")
	fmt.Printf("Name: %v\n", name)

	// Queries
	fmt.Println("\n--- 5. Queries ( Interpreted ) ---")
	vm.ExecuteQuery("GetLowStockProducts", map[string]interface{}{"threshold": 600})

	fmt.Println("\n--- 6. New Features (Loop & Builtins) ---")
	sum := vm.ExecuteFunction("TestLoop", 5)
	fmt.Printf("Loop Sum (1..5): %v\n", sum) // Expected 15

	strRes := vm.ExecuteFunction("StringTest", "hello")
	fmt.Printf("StringTest('hello'): %v\n", strRes) // Expected HELLO_5

	// Verification
	fmt.Println("\n--- Data Verification ---")
	fmt.Printf("Product WIDGET-X Stock: %v\n", findOne(doc, "Product", "SKU", "WIDGET-X")["StockLevel"])
	fmt.Printf("Order 100 exists: %v\n", findOne(doc, "Order", "OrderID", uint64(100)) != nil)

	// Save
	fmt.Println("\n--- Saving Data ---")
	if err := doc.Save("full_features.scrt"); err != nil {
		fmt.Printf("Error saving: %v\n", err)
	} else {
		fmt.Println("Saved to full_features.scrt")
	}
}

func findOne(doc *schema.Document, schemaName, field string, val interface{}) map[string]interface{} {
	for _, row := range doc.Data[schemaName] {
		if fmt.Sprintf("%v", row[field]) == fmt.Sprintf("%v", val) {
			return row
		}
	}
	return nil
}
