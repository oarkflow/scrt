# SCRT Protocol Enhancement Checklist

This document tracks the implementation of essential features for the SCRT protocol interpreter and runtime.

## 1. Expression Engine [PRIORITY: HIGH]
- [x] **Arithmetic Operators**: Support for `+`, `-`, `*`, `/`, `%` with correct precedence.
- [x] **Comparison Operators**: Support for `==`, `!=`, `<`, `>`, `<=`, `>=`.
- [x] **Logical Operators**: Support for `&&`, `||`, `!`.
- [x] **Parentheses**: Support for grouping `(a + b) * c`.
- [x] **Type Coercion**: safe handling of int/float/string comparisons.

## 2. Control Flow [PRIORITY: HIGH]
- [x] **Conditional Blocks**: `if (condition) { ... }` (Basic support).
- [x] **Loops**: `while (condition) { ... }`.
- [x] **Early Return**: `return value` from nested blocks.

## 3. Function Features
- [x] **Built-in Functions**: `len()`, `upper()`, `lower()`, `now()`.
- [ ] **Function Calls**: Call other SCRT functions from within a function.
- [ ] **Local Scope**: Proper variable scoping within blocks.

## 4. Interpreter Improvements
- [x] **Tokenizer**: Basic tokenizer implemented.
- [ ] **Error Handling**: Graceful error reporting.
