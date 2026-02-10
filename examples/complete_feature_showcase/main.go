package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/scrt/schema"
)

// SimpleInterpreter executes the string body of SCRT functions
type SimpleInterpreter struct {
	doc *schema.Document
}

func NewInterpreter(doc *schema.Document) *SimpleInterpreter {
	return &SimpleInterpreter{doc: doc}
}

// Execute parses and runs the function body line-by-line using a very primitive logic
func (vm *SimpleInterpreter) ExecuteFunction(name string, args ...interface{}) interface{} {
	fn, ok := vm.doc.Functions[name]
	if !ok {
		panic("function not found: " + name)
	}

	// Create scope with arguments
	scope := make(map[string]interface{})
	for i, arg := range fn.Args {
		if i < len(args) {
			scope[arg.Name] = args[i]
		}
	}

	lines := strings.Split(fn.Body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		if strings.HasPrefix(line, "DELETE ") {
			// Relaxed parsing
			parts := strings.Fields(line)
			// DELETE FROM Schema WHERE Field = Val
			// 0      1    2      3     4     5 6
			if len(parts) >= 7 && parts[1] == "FROM" {
				schemaName := parts[2]
				condField := parts[4]
				condExpr := parts[6]

				targetVal := vm.eval(condExpr, scope)

				if rows, ok := vm.doc.Data[schemaName]; ok {
					var kept []map[string]interface{}
					deleted := 0
					for _, row := range rows {
						rowVal := row[condField]
						if fmt.Sprintf("%v", rowVal) != fmt.Sprintf("%v", targetVal) {
							kept = append(kept, row)
						} else {
							deleted++
						}
					}
					vm.doc.Data[schemaName] = kept
					fmt.Printf("[INTERP] Deleted %d rows from %s\n", deleted, schemaName)
				}
				continue
			}
		}

		// 1. Return statement
		if strings.HasPrefix(line, "return ") {
			expr := strings.TrimPrefix(line, "return ")
			return vm.eval(expr, scope)
		}

		// 2. Object creation: user = new User
		if strings.Contains(line, " = new ") {
			parts := strings.Split(line, "=")
			varName := strings.TrimSpace(parts[0])
			clsParts := strings.Split(strings.TrimSpace(parts[1]), " ") // "new User"
			className := clsParts[1]

			// Initialize map for object
			obj := make(map[string]interface{})
			// Handle schema defaults? In a real impl, yes.
			// Add auto-increment ID simulation
			if rows, ok := vm.doc.Data[className]; ok {
				obj["ID"] = uint64(len(rows) + 1) // simple auto-inc
			}
			scope[varName] = obj
			fmt.Printf("[INTERP] Created new %s in scope as %s\n", className, varName)
			continue
		}

		// 3. Assignment: user.Email = uEmail
		if strings.Contains(line, "=") && !strings.Contains(line, "UPDATE") {
			parts := strings.Split(line, "=")
			lhs := strings.TrimSpace(parts[0])
			rhs := strings.TrimSpace(parts[1])

			if strings.Contains(lhs, ".") {
				// Object field assignment
				path := strings.Split(lhs, ".")
				varName := path[0]
				fieldName := path[1]

				obj, exists := scope[varName]
				if exists {
					if mapObj, ok := obj.(map[string]interface{}); ok {
						mapObj[fieldName] = vm.eval(rhs, scope)
					}
				}
			} else {
				// Variable assignment
				scope[lhs] = vm.eval(rhs, scope)
			}
			continue
		}

		// 4. INSERT: insert user
		if strings.HasPrefix(line, "insert ") {
			varName := strings.TrimSpace(strings.TrimPrefix(line, "insert "))
			if obj, ok := scope[varName]; ok {
				// We need to know which schema it belongs to.
				// In this dumb interpreter, we looked it up during 'new' or imply it
				// let's cheat and assume 'User' if it has ID and Username etc ?
				// Or check the function return type?
				// For the demo, let's hardcode based on variable name or infer from struct
				// Better: "new User" put a _type metadata

				// Just for the showcase examples:
				targetSchema := ""
				mapObj := obj.(map[string]interface{})
				if _, ok := mapObj["Username"]; ok {
					targetSchema = "User"
					// Apply defaults hack
					if _, ok := mapObj["CreatedAt"]; !ok {
						mapObj["CreatedAt"] = time.Now()
					}
				}

				if targetSchema != "" {
					if err := vm.checkConstraints(targetSchema, mapObj); err != nil {
						fmt.Printf("[INTERP] Constraint violation: %v\n", err)
						return nil // Abort function
					}
					vm.doc.Data[targetSchema] = append(vm.doc.Data[targetSchema], mapObj)
					fmt.Printf("[INTERP] Inserted record into %s: %v\n", targetSchema, mapObj)
				}
			}
			continue
		}

		// 5. UPDATE: UPDATE Product SET StockLevel = pQty WHERE SKU = pSku
		// Case-insensitive check would be better but simplified here
		if strings.HasPrefix(line, "UPDATE ") {
			// Parsing "UPDATE Product SET StockLevel = pQty WHERE SKU = pSku"
			parts := strings.Fields(line)
			schemaName := parts[1] // Product
			// SET at parts[2]
			// Field=Val at parts[3]...
			// This is fragile parsing, but sufficient for the fixed input
			// "StockLevel" "=" "pQty"
			field := parts[3]
			// parts[4] is "="
			valExpr := parts[5]
			// "WHERE" at parts[6]
			// "SKU" at parts[7]
			// "=" at parts[8]
			condRef := parts[9]

			val := vm.eval(valExpr, scope)
			refVal := vm.eval(condRef, scope)

			// Execute Update
			count := 0
			if rows, ok := vm.doc.Data[schemaName]; ok {
				condField := parts[7]
				for _, row := range rows {
					// Check condition
					// Basic equality check
					rowVal, _ := row[condField]
					if fmt.Sprintf("%v", rowVal) == fmt.Sprintf("%v", refVal) {
						// Update
						row[field] = val
						count++
					}
				}
			}
			fmt.Printf("[INTERP] Updated %d rows in %s\n", count, schemaName)
			continue
		}

		// 6. DELETE: DELETE FROM Order WHERE OrderID = oID
		if strings.HasPrefix(line, "DELETE FROM ") {
			// handled above
			continue
		}
	}
	return nil
}

// checkConstraints verifies PK, Unique, and Index constraints
func (vm *SimpleInterpreter) checkConstraints(schemaName string, record map[string]interface{}) error {
	schemaObj, ok := vm.doc.Schemas[schemaName]
	if !ok {
		return nil
	}
	existingRows := vm.doc.Data[schemaName]

	// 1. Check Field Constraints (PK, Unique)
	for _, field := range schemaObj.Fields {
		if !field.PrimaryKey && !field.Unique {
			continue
		}

		newVal, hasVal := record[field.Name]
		if !hasVal || newVal == nil {
			if field.PrimaryKey {
				return fmt.Errorf("primary key %s cannot be null", field.Name)
			}
			continue // Unique constraint usually allows multiple nulls unless specified otherwise, but strict SQL unique often allows ONE null or multiple depending on DB. We'll allow nulls.
		}

		for _, row := range existingRows {
			existingVal, ok := row[field.Name]
			if !ok || existingVal == nil {
				continue
			}
			if fmt.Sprintf("%v", existingVal) == fmt.Sprintf("%v", newVal) {
				return fmt.Errorf("duplicate value '%v' for unique/pk field '%s'", newVal, field.Name)
			}
		}
	}

	// 2. Check Index Constraints (Unique Indexes only for now)
	for _, idx := range schemaObj.Indexes {
		if !idx.Unique {
			continue
		}

		// Composite unique check
		for _, row := range existingRows {
			match := true
			for _, fieldName := range idx.Fields {
				newVal := record[fieldName]
				existingVal := row[fieldName]

				// Handle nil/missing comparison
				// If any part of the composite key is different, the row is different.
				if fmt.Sprintf("%v", existingVal) != fmt.Sprintf("%v", newVal) {
					match = false
					break
				}
			}
			if match {
				return fmt.Errorf("duplicate entry for unique index '%s'", idx.Name)
			}
		}
	}

	return nil
}

// eval evaluates simple expressions like "price * percent / 100.0" or variables
func (vm *SimpleInterpreter) eval(expr string, scope map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)

	// String literal
	if strings.HasPrefix(expr, "\"") {
		return strings.Trim(expr, "\"")
	}

	// Variable lookup
	if val, ok := scope[expr]; ok {
		return val
	}

	// Boolean
	if expr == "true" { return true }
	if expr == "false" { return false }

	// Number (try integer)
	if i, err := strconv.Atoi(expr); err == nil {
		return i
	}
	// Number (try float)
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}

	// Math expression: "price * percent / 100.0"
	// Extremely naive math parser for the specific example

	// String concat or simple Addition
	if strings.Contains(expr, "+") {
		parts := strings.Split(expr, "+")
		res := ""
		isNumber := true
		sum := 0.0

		// Check if we are doing string concat or number addition
		// For this dumb interpreter, if any part is string, string concat
		vals := make([]interface{}, len(parts))
		for i, p := range parts {
			val := vm.eval(strings.TrimSpace(p), scope)
			vals[i] = val
			if _, ok := val.(string); ok {
				isNumber = false
			}
		}

		if isNumber {
			for _, v := range vals {
				sum += toFloat(v)
			}
			return sum
		} else {
			for _, v := range vals {
				res += fmt.Sprintf("%v", v)
			}
			return res
		}
	}

	if strings.Contains(expr, "*") {
		// This is very specific to the example "a * b / c"
		// tokenize by space
		tokens := strings.Fields(expr)
		// [price, *, percent, /, 100.0]
		if len(tokens) == 5 && tokens[1] == "*" && tokens[3] == "/" {
			v1 := toFloat(vm.eval(tokens[0], scope))
			v2 := toFloat(vm.eval(tokens[2], scope))
			v3 := toFloat(vm.eval(tokens[4], scope))
			return v1 * v2 / v3
		}
	}

	return nil
}

// ExecuteQuery parses and runs the SQL query dynamically
func (e *SimpleInterpreter) ExecuteQuery(queryName string, params map[string]interface{}) {
	q, ok := e.doc.Queries[queryName]
	if !ok {
		fmt.Printf("Error: Query %s not found\n", queryName)
		return
	}
	fmt.Printf("Executing SQL for: %s\n", q.Name)

	// Very basic SQL Parser: "SELECT ... FROM ... WHERE ... ORDER BY ..."
	sql := strings.ReplaceAll(q.SQL, "\n", " ")
	parts := strings.Fields(sql)

	if len(parts) < 4 || strings.ToUpper(parts[0]) != "SELECT" {
		fmt.Println("Error: Only SELECT statements supported in this demo engine")
		return
	}

	// 1. Find FROM
	fromIdx := -1
	for i, p := range parts {
		if strings.ToUpper(p) == "FROM" {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 {
		fmt.Println("Error: Missing FROM clause")
		return
	}
	schemaName := parts[fromIdx+1]
	alias := ""
	if fromIdx+2 < len(parts) && strings.ToUpper(parts[fromIdx+2]) != "WHERE" {
		alias = parts[fromIdx+2]
	}

	rows, ok := e.doc.Data[schemaName]
	if !ok {
		fmt.Printf("Error: Schema %s not found\n", schemaName)
		return
	}

	// 2. Select Fields
	fieldPart := strings.Join(parts[1:fromIdx], " ")
	rawFields := strings.Split(fieldPart, ",")
	var selectedFields []string
	for _, f := range rawFields {
		f = strings.TrimSpace(f)
		if alias != "" && strings.HasPrefix(f, alias+".") {
			f = strings.TrimPrefix(f, alias+".")
		}
		selectedFields = append(selectedFields, f)
	}

	// 3. Apply Filtering (WHERE)
	var filteredRows []map[string]interface{}
	whereIdx := -1
	for i, p := range parts {
		if strings.ToUpper(p) == "WHERE" {
			whereIdx = i
			break
		}
	}

	if whereIdx != -1 && whereIdx+3 < len(parts) {
		condFieldRaw := parts[whereIdx+1]
		condOp := parts[whereIdx+2]
		condParamRaw := parts[whereIdx+3]

		condField := condFieldRaw
		if alias != "" && strings.HasPrefix(condField, alias+".") {
			condField = strings.TrimPrefix(condField, alias+".")
		}

		var paramVal interface{}
		if strings.HasPrefix(condParamRaw, ":") {
			paramName := strings.TrimPrefix(condParamRaw, ":")
			if val, ok := params[paramName]; ok {
				paramVal = val
			}
		}

		for _, row := range rows {
			val, ok := row[condField]
			if !ok { continue }

			match := false
			switch condOp {
			case "=":
				match = fmt.Sprintf("%v", val) == fmt.Sprintf("%v", paramVal)
			case "<":
				match = toFloat(val) < toFloat(paramVal)
			case ">":
				match = toFloat(val) > toFloat(paramVal)
			}
			if match {
				filteredRows = append(filteredRows, row)
			}
		}
	} else {
		filteredRows = rows
	}

	// 4. Print Results
	fmt.Println("  Results:")
	for _, row := range filteredRows {
		display := make([]string, 0, len(selectedFields))
		for _, field := range selectedFields {
			if val, ok := row[field]; ok {
				display = append(display, fmt.Sprintf("%v", val))
			} else {
				display = append(display, "NULL")
			}
		}
		fmt.Printf("    - %s\n", strings.Join(display, " | "))
	}
}

func toFloat(v interface{}) float64 {
	switch i := v.(type) {
	case float64: return i
	case int: return float64(i)
	case int64: return float64(i)
	}
	return 0.0
}

func main() {
	// ... (Same setup as before)
	f, err := os.Open("full_features.scrt")
	if err != nil { panic(err) }
	defer f.Close()

	doc, err := schema.Parse(f)
	if err != nil { panic(err) }

	fmt.Println("=== SCRT Interpreter Show case ===")
	vm := NewInterpreter(doc)

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
	disc := vm.ExecuteFunction("CalculateDiscount", 200.0, 15.0)
	fmt.Printf("Discount: %.2f\n", disc)

	fmt.Println("\n--- 4. String concat ( Interpreted ) ---")
	name := vm.ExecuteFunction("FormatFullName", "John", "Wick")
	fmt.Printf("Name: %v\n", name)

	// Queries
	fmt.Println("\n--- 5. Queries ( Interpreted ) ---")
	vm.ExecuteQuery("GetLowStockProducts", map[string]interface{}{"threshold": 600})

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

func findOne(doc *schema.Document, schema, field string, val interface{}) map[string]interface{} {
	for _, row := range doc.Data[schema] {
		if fmt.Sprintf("%v", row[field]) == fmt.Sprintf("%v", val) {
			return row
		}
	}
	return nil
}
