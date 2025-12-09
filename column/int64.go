package column

import "bytes"

// Int64Column encodes signed integers using zig-zag varints.
type Int64Column struct {
	values []int64
}

func NewInt64Column(capacity int) *Int64Column {
	c := normalizeCapacityHint(capacity)
	return &Int64Column{values: make([]int64, 0, c)}
}

func (c *Int64Column) Append(v int64) {
	c.values = append(c.values, v)
}

func (c *Int64Column) Encode(dst *bytes.Buffer) {
	writeUvarint(dst, uint64(len(c.values)))
	for _, v := range c.values {
		writeVarint(dst, v)
	}
}

func (c *Int64Column) Reset() {
	c.values = c.values[:0]
}
