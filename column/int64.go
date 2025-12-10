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
	count := len(c.values)
	mode := uint64(0)
	if count > 1 {
		mode = 1
	}
	header := (uint64(count) << 1) | mode
	writeUvarint(dst, header)
	if count == 0 {
		return
	}
	if mode == 0 {
		for _, v := range c.values {
			writeVarint(dst, v)
		}
		return
	}
	writeVarint(dst, c.values[0])
	prev := c.values[0]
	for i := 1; i < count; i++ {
		delta := c.values[i] - prev
		writeVarint(dst, delta)
		prev = c.values[i]
	}
}

func (c *Int64Column) Reset() {
	c.values = c.values[:0]
}
