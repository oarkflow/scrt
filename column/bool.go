package column

import "bytes"

// BoolColumn packs boolean values as a compact byte slice.
type BoolColumn struct {
	values []byte
}

func NewBoolColumn() *BoolColumn {
	return &BoolColumn{values: make([]byte, 0, 256)}
}

func (c *BoolColumn) Append(v bool) {
	if v {
		c.values = append(c.values, 1)
	} else {
		c.values = append(c.values, 0)
	}
}

func (c *BoolColumn) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.values)))
	buf = append(buf, c.values...)
	dst.Write(buf)
}

func (c *BoolColumn) Reset() {
	c.values = c.values[:0]
}
