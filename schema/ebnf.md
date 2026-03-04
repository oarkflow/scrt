# SCRT DSL EBNF (Core)

```ebnf
document        = { ws_line | schema_block | function_block | query_block | data_block } ;

schema_block    = schema_decl , { field_decl | index_decl | unique_decl | relation_decl | ws_line } ;
schema_decl     = "@schema" , [ ":" ] , ident , nl ;

field_decl      = "@field" , ident , nullable_type , { attr } , nl ;
index_decl      = "@index" , [ ":" ] , ident , "(" , field_list , ")" , [ "unique" ] , nl ;
unique_decl     = "@unique" , [ ":" ] , "(" , field_list , ")" , nl ;
relation_decl   = "@relation" , ident , relation_target , { relation_attr } , nl ;

relation_target = ident , "." , ident | ident , ":" , ident | ident , "->" , ident , "." , ident ;
relation_attr   = "onDelete" , ( ":" | "=" ) , action
                | "onUpdate" , ( ":" | "=" ) , action ;
action          = "restrict" | "cascade" | "set_null" | "no_action" ;

nullable_type   = [ "?" ] , type_ref ;

type_ref        = scalar_type
                | ref_type
                | array_type
                | map_type
                | object_type
                | custom_type ;

scalar_type     = "uint64" | "uint" | "int64" | "int" | "integer"
                | "float64" | "float" | "double"
                | "bool" | "boolean"
                | "string" | "str" | "text"
                | "bytes" | "blob"
                | "date" | "datetime" | "timestamp" | "timestamptz" | "duration" ;

ref_type        = "ref" , ":" , ident , ":" , ident ;
array_type      = "[]" , type_atom ;
map_type        = "map" , "[" , type_atom , "]" , type_atom ;
object_type     = "object" , ":" , ident ;
custom_type     = ident ;   (* shorthand ref to schema key *)
type_atom       = ref_type | object_type | scalar_type | ident ;

attr            = "readonly"
                | "serial"
                | "pk"
                | "unique"
                | "nullable"
                | "default" , ( ":" | "=" ) , default_value
                | ident
                | ident , ( ":" | "=" ) , attr_value ;
default_value   = "now()"
                | attr_value ;
attr_value      = ident | number | string_lit ;

field_list      = ident , { "," , ident } ;

function_block  = "@function" , ... ;   (* implementation-defined body *)
query_block     = "@query" , ... ;      (* implementation-defined body *)
data_block      = "@" , ident , nl , { data_row } ;
data_row        = ... ;                  (* CSV-like row grammar *)

ident           = letter , { letter | digit | "_" } ;
number          = [ "-" ] , digit , { digit } , [ "." , digit , { digit } ] ;
string_lit      = "\"" , { char } , "\"" | "'" , { char } , "'" | "`" , { char } , "`" ;

ws_line         = { " " | "\t" } , nl ;
nl              = "\n" | "\r\n" ;
letter          = "A"…"Z" | "a"…"z" ;
digit           = "0"…"9" ;
```

Notes:
- `?Type` nullable shorthand is handled by parser pre-processing on `type_ref`.
- Inline relation actions can be attached directly to `ref` fields:
  - `@field UserID ref:User:ID onDelete:cascade onUpdate:restrict`
- `@relation` is optional when inline actions are provided on a `ref` field.
