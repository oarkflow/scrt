package column

import "bytes"

// BoolColumn packs boolean values as a compact byte slice.
type BoolColumn struct {
	values []byte
}

func NewBoolColumn(capacity int) *BoolColumn {
	c := normalizeCapacityHint(capacity)
	return &BoolColumn{values: make([]byte, 0, c)}
}

func (c *BoolColumn) Append(v bool) {
	if v {
		c.values = append(c.values, 1)
	} else {
		c.values = append(c.values, 0)
	}
}

func (c *BoolColumn) Encode(dst *bytes.Buffer) {
	writeUvarint(dst, uint64(len(c.values)))
	dst.Write(c.values)
}

func (c *BoolColumn) Reset() {
	c.values = c.values[:0]
}
