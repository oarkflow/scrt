package column

import "bytes"

// Int64Column encodes signed integers using zig-zag varints.
type Int64Column struct {
	values []int64
}

func NewInt64Column() *Int64Column {
	return &Int64Column{values: make([]int64, 0, 256)}
}

func (c *Int64Column) Append(v int64) {
	c.values = append(c.values, v)
}

func (c *Int64Column) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.values)))
	for _, v := range c.values {
		buf = appendVarint(buf, v)
	}
	dst.Write(buf)
}

func (c *Int64Column) Reset() {
	c.values = c.values[:0]
}
