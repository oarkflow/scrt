# SCRT DSL Evolution Roadmap

This roadmap extends SCRT from a fast schema + storage DSL into a complete schema language while preserving runtime efficiency and developer ergonomics.

## Design Goals

- Keep current SCRT syntax backward compatible.
- Make constraints explicit and machine-readable.
- Separate schema modeling from heavy runtime features.
- Preserve fast parse/compile path for ingest and binary encoding.

## Phase 1: Strong Field Constraints (implemented foundation)

Supported field attributes:

- `nullable`
- `format:<name>` (`email`, `uri`, `uuid`, `ipv4`, `ipv6`, `date`, `datetime`, `timestamp`, `timestamptz`, `duration`)
- `pattern:"regex"`
- `enum:"A|B|C"`
- `minlength:<n>`, `maxlength:<n>`
- `min:<n>`, `max:<n>`
- `description:"..."`
- `example:"..."`

Reference ergonomics:

- Explicit ref: `ref:Schema:Field`
- Shorthand schema type: `@field Customer User` (infers target field by `pk` then `ID`)

Validation API:

- `(*Schema).ValidateRow(map[string]interface{}) error`
- `(*Document).ValidateData() error`

## Phase 2: Composability and Modularity

Planned:

- `@namespace:<name>`
- `@import:<path-or-module>`
- Stable schema IDs and versions:
  - `@schema:Order version:2`
  - `@schema:Order id:"urn:scrt:order"`

## Phase 3: Complex Types

Planned type extensions:

- Arrays: `[]string`, `[]ref:OrderItem:ID`, `[]OrderItem`
- Maps: `map[string]string`
- Structured objects:
  - named: `@schema:Address`
  - embedded/inline blocks (optional later)

Runtime strategy:

- Keep core codec primitive-first.
- Allow complex types either:
  - normalized as separate schemas with refs, or
  - encoded as `bytes/json` envelope for selective use-cases.

## Phase 4: Relationship Semantics

Planned DSL additions:

- `@relation User hasMany Order by Customer`
- FK actions:
  - `onDelete:cascade|restrict|set_null`
  - `onUpdate:cascade|restrict`

## Phase 5: Advanced Validation Logic

Planned:

- Cross-field rules:
  - `@check Quantity > 0`
  - `@check Discount <= TotalAmount`
- Conditional validation:
  - `if/then/else` style rule blocks
- Union/polymorphism:
  - `oneOf`, `anyOf`, discriminators

## Phase 6: Tooling and Generation

Targets:

- JSON Schema export/import
- OpenAPI schema generation
- SQL DDL generation (with migrations)
- Go/TypeScript type generation
- Form/UI schema generation

## Canonical Attribute Style

Use `key:value` or `key="value"` forms on `@field` lines.

Recommended:

```scrt
@field Username string minlength:3 maxlength:32 pattern:"^[a-z0-9_]+$"
@field Email string format:email unique
@field Status string enum:"Active|Inactive|Blocked" default="Active"
@field Age ?int min:0 max:150
```
