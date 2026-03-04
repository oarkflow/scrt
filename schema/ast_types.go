package schema

type ASTDocument struct {
	Schemas []ASTSchema
}

type ASTSchema struct {
	Name      string
	Fields    []ASTField
	Indexes   []ASTIndex
	Relations []ASTRelation
}

type ASTField struct {
	Name       string
	Type       string
	Attributes []ASTAttribute
}

type ASTIndex struct {
	Name   string
	Fields []string
	Unique bool
}

type ASTRelation struct {
	Field        string
	TargetSchema string
	TargetField  string
	OnDelete     string
	OnUpdate     string
}

type ASTAttribute struct {
	Key   string
	Value string
}
