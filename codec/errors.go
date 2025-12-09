package codec

import "errors"

var (
	// ErrUnknownField indicates that a field name is not present in the schema.
	ErrUnknownField = errors.New("codec: unknown field")
	// ErrMismatchedFieldCount indicates that a row does not supply values for each schema field.
	ErrMismatchedFieldCount = errors.New("codec: mismatched field count")
	// ErrSchemaFingerprintMismatch indicates that the binary stream targets a different schema.
	ErrSchemaFingerprintMismatch = errors.New("codec: schema fingerprint mismatch")
)
