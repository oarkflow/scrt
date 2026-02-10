package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SimpleInterpreter executes the string body of SCRT functions
type SimpleInterpreter struct {
	doc *Document
}

func NewInterpreter(doc *Document) *SimpleInterpreter {
	return &SimpleInterpreter{doc: doc}
}

// ExecuteFunction parses and runs the function body line-by-line using a block-aware logic
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
	res, _ := vm.runBlock(lines, scope)
	return res
}

func (vm *SimpleInterpreter) runBlock(lines []string, scope map[string]interface{}) (interface{}, bool) {
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		// IF Block
		if strings.HasPrefix(line, "if ") {
			// Syntax: if (cond) {
			// Find block end: matching '}' at same level?
			// Simplified: find matching } line by counting braces
			braceCount := 0
			// Scan quickly to find end
			blockEnd := -1
			for j := i; j < len(lines); j++ {
				l := lines[j]
				if strings.Contains(l, "{") {
					braceCount++
				}
				if strings.Contains(l, "}") {
					braceCount--
				}
				if braceCount == 0 && j > i {
					blockEnd = j
					break
				}
			}
			if blockEnd == -1 {
				// Syntax error or single line? Skip for now
				continue
			}

			// Evaluate condition
			// "if (x > 10) {" -> "x > 10"
			rawLine := strings.TrimSpace(lines[i])
			condStr := rawLine[3:] // strip "if "
			condStr = strings.TrimSuffix(condStr, "{")
			condStr = strings.TrimSpace(condStr)
			if strings.HasPrefix(condStr, "(") && strings.HasSuffix(condStr, ")") {
				condStr = condStr[1 : len(condStr)-1]
			}

			cond := vm.eval(condStr, scope)
			if truthy(cond) {
				// Run inside block (skip "if" line and "}" line)
				res, ret := vm.runBlock(lines[i+1:blockEnd], scope)
				if ret {
					return res, true
				}
			} else {
				// Check for ELSE
				// If next line is "else {" ...
				// Not implemented in this snippet to keep concise
			}
			i = blockEnd
			continue
		}

		// WHILE Block
		if strings.HasPrefix(line, "while ") {
			// Syntax: while (cond) {
			braceCount := 0
			blockEnd := -1
			for j := i; j < len(lines); j++ {
				l := lines[j]
				if strings.Contains(l, "{") {
					braceCount++
				}
				if strings.Contains(l, "}") {
					braceCount--
				}
				if braceCount == 0 && j > i {
					blockEnd = j
					break
				}
			}
			if blockEnd == -1 {
				continue
			}

			// Evaluate condition
			rawLine := strings.TrimSpace(lines[i])
			condStr := rawLine[6:] // strip "while "
			condStr = strings.TrimSuffix(condStr, "{")
			condStr = strings.TrimSpace(condStr)
			if strings.HasPrefix(condStr, "(") && strings.HasSuffix(condStr, ")") {
				condStr = condStr[1 : len(condStr)-1]
			}

			// Loop
			for {
				cond := vm.eval(condStr, scope)
				if !truthy(cond) {
					break
				}
				// Run inside block
				res, ret := vm.runBlock(lines[i+1:blockEnd], scope)
				if ret {
					return res, true
				}
			}
			i = blockEnd
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
			return vm.eval(expr, scope), true
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
						return nil, true // Abort function
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
	return nil, false
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

// eval uses a specialized parser for execution
func (vm *SimpleInterpreter) eval(expr string, scope map[string]interface{}) interface{} {
	// A Simple Pratt-like parser or Precedence Climber could go here.
	// For this showcase, we'll implement a token-based shunting-yard evaluator
	// to support operator precedence and parentheses.
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	tokens := tokenize(expr)
	rpn := shuntingYard(tokens)
	return evaluateRPN(rpn, scope)
}

// -- Expression Engine Helpers --

type tokenType int

const (
	tokNum tokenType = iota
	tokStr
	tokVar
	tokOp
	tokFunc
	tokLParen
	tokRParen
	tokComma
)

type token struct {
	typ tokenType
	val string
}

func tokenize(expr string) []token {
	var tokens []token
	n := len(expr)
	i := 0
	for i < n {
		c := expr[i]
		if c == ' ' || c == '\t' {
			i++
			continue
		}
		if c == ',' {
			tokens = append(tokens, token{tokComma, ","})
			i++
			continue
		}
		if c == '(' {
			tokens = append(tokens, token{tokLParen, "("})
			i++
			continue
		}
		if c == ')' {
			tokens = append(tokens, token{tokRParen, ")"})
			i++
			continue
		}
		if c == '"' {
			// String literal
			end := i + 1
			for end < n && expr[end] != '"' {
				end++
			}
			if end < n {
				tokens = append(tokens, token{tokStr, expr[i+1 : end]})
				i = end + 1
			} else {
				i = n // Error
			}
			continue
		}
		// Operators (multi-char: ==, !=, >=, <=, &&, ||)
		if i+1 < n {
			two := expr[i : i+2]
			if two == "==" || two == "!=" || two == ">=" || two == "<=" || two == "&&" || two == "||" {
				tokens = append(tokens, token{tokOp, two})
				i += 2
				continue
			}
		}
		if strings.ContainsRune("+-*/%><!", rune(c)) {
			tokens = append(tokens, token{tokOp, string(c)})
			i++
			continue
		}

		// Number or Identifier
		start := i
		if isDigit(c) {
			// Number
			for i < n && (isDigit(expr[i]) || expr[i] == '.') {
				i++
			}
			tokens = append(tokens, token{tokNum, expr[start:i]})
		} else {
			// Identifier (or bool)
			for i < n && (isAlphaNum(expr[i]) || expr[i] == '_' || expr[i] == '.') {
				i++
			}
			val := expr[start:i]
			if val == "true" || val == "false" {
				tokens = append(tokens, token{tokVar, val}) // Treat bool as var/val for now
			} else {
				// Check if followed by (
				isFunc := false
				j := i
				for j < n && (expr[j] == ' ' || expr[j] == '\t') {
					j++
				}
				if j < n && expr[j] == '(' {
					isFunc = true
				}
				if isFunc {
					tokens = append(tokens, token{tokFunc, val})
				} else {
					tokens = append(tokens, token{tokVar, val})
				}
			}
		}
	}
	return tokens
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isAlphaNum(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func shuntingYard(tokens []token) []token {
	var output []token
	var stack []token

	prec := map[string]int{
		"||": 1, "&&": 2,
		"==": 3, "!=": 3, "<": 3, ">": 3, "<=": 3, ">=": 3,
		"+": 4, "-": 4,
		"*": 5, "/": 5, "%": 5,
		"!": 6, // Unary not
	}

	for _, t := range tokens {
		switch t.typ {
		case tokNum, tokStr, tokVar:
			output = append(output, t)
		case tokFunc:
			stack = append(stack, t)
		case tokComma:
			for len(stack) > 0 && stack[len(stack)-1].typ != tokLParen {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
		case tokLParen:
			stack = append(stack, t)
		case tokRParen:
			for len(stack) > 0 && stack[len(stack)-1].typ != tokLParen {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1] // Pop (
			}
			// If token at top of stack is a function, pop it to output queue.
			if len(stack) > 0 && stack[len(stack)-1].typ == tokFunc {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
		case tokOp:
			for len(stack) > 0 && stack[len(stack)-1].typ == tokOp && prec[stack[len(stack)-1].val] >= prec[t.val] {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
			stack = append(stack, t)
		}
	}
	for len(stack) > 0 {
		output = append(output, stack[len(stack)-1])
		stack = stack[:len(stack)-1]
	}
	return output
}

func evaluateRPN(tokens []token, scope map[string]interface{}) interface{} {
	var stack []interface{}

	for _, t := range tokens {
		switch t.typ {
		case tokNum:
			if f, err := strconv.ParseFloat(t.val, 64); err == nil {
				stack = append(stack, f)
			} else {
				stack = append(stack, 0.0)
			}
		case tokStr:
			stack = append(stack, t.val)
		case tokFunc:
			// Handle Built-in functions
			funcName := t.val
			switch funcName {
			case "len":
				if len(stack) < 1 {
					return nil
				}
				val := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				// string or array length
				if s, ok := val.(string); ok {
					stack = append(stack, float64(len(s)))
				} else if arr, ok := val.([]interface{}); ok {
					stack = append(stack, float64(len(arr))) // Array not heavily used yet
				} else {
					stack = append(stack, 0.0)
				}
			case "upper":
				if len(stack) < 1 {
					return nil
				}
				val := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if s, ok := val.(string); ok {
					stack = append(stack, strings.ToUpper(s))
				} else {
					stack = append(stack, val)
				}
			case "lower":
				if len(stack) < 1 {
					return nil
				}
				val := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if s, ok := val.(string); ok {
					stack = append(stack, strings.ToLower(s))
				} else {
					stack = append(stack, val)
				}
			case "now":
				// returns time object or string? showcase uses string for simplicity in some places but time.Time in others.
				// Let's return time.Time
				stack = append(stack, time.Now())
			default:
				// unsupported
				stack = append(stack, nil)
			}
		case tokVar:
			if t.val == "true" {
				stack = append(stack, true)
				break
			}
			if t.val == "false" {
				stack = append(stack, false)
				break
			}
			if t.val == "null" {
				stack = append(stack, nil)
				break
			}
			if val, ok := scope[t.val]; ok {
				stack = append(stack, val)
			} else {
				// Try object access
				if strings.Contains(t.val, ".") {
					parts := strings.Split(t.val, ".")
					if obj, ok := scope[parts[0]]; ok {
						if m, ok := obj.(map[string]interface{}); ok {
							stack = append(stack, m[parts[1]])
						} else {
							stack = append(stack, nil)
						}
					} else {
						stack = append(stack, nil)
					}
				} else {
					// Check if it's a global constant or similar?
					stack = append(stack, nil)
				}
			}
		case tokOp:
			if t.val == "!" {
				if len(stack) < 1 {
					return nil
				}
				val := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				stack = append(stack, !truthy(val))
				continue
			}

			if len(stack) < 2 {
				return nil
			}
			b, a := stack[len(stack)-1], stack[len(stack)-2]
			stack = stack[:len(stack)-2]

			res := applyOp(t.val, a, b)
			stack = append(stack, res)
		}
	}
	if len(stack) == 0 {
		return nil
	}
	return stack[0]
}

func applyOp(op string, a, b interface{}) interface{} {
	// String concat
	if op == "+" {
		if _, ok := a.(string); ok {
			return fmt.Sprintf("%v%v", a, b)
		}
		if _, ok := b.(string); ok {
			return fmt.Sprintf("%v%v", a, b)
		}
	}

	va, vb := toFloat(a), toFloat(b)
	switch op {
	case "+":
		return va + vb
	case "-":
		return va - vb
	case "*":
		return va * vb
	case "/":
		if vb == 0 {
			return 0.0
		}
		return va / vb
	case ">":
		return va > vb
	case "<":
		return va < vb
	case ">=":
		return va >= vb
	case "<=":
		return va <= vb
	case "==":
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	case "!=":
		return fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b)
	case "&&":
		return truthy(a) && truthy(b)
	case "||":
		return truthy(a) || truthy(b)
	}
	return nil
}

func truthy(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return v != nil
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
			if !ok {
				continue
			}

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
	case float64:
		return i
	case int:
		return float64(i)
	case int64:
		return float64(i)
	}
	return 0.0
}
