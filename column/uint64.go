package column

import "bytes"

// Uint64Column encodes unsigned integers via compact varints.
type Uint64Column struct {
	values []uint64
}

func NewUint64Column(capacity int) *Uint64Column {
	c := normalizeCapacityHint(capacity)
	return &Uint64Column{values: make([]uint64, 0, c)}
}

func (c *Uint64Column) Append(v uint64) {
	c.values = append(c.values, v)
}

func (c *Uint64Column) Encode(dst *bytes.Buffer) {
	count := len(c.values)
	mode := uint64(0)
	if count >= 2 && isMonotonicUint64(c.values) {
		mode = 1
	}
	header := (uint64(count) << 1) | mode
	writeUvarint(dst, header)
	if count == 0 {
		return
	}
	if mode == 0 {
		for _, v := range c.values {
			writeUvarint(dst, v)
		}
		return
	}
	writeUvarint(dst, c.values[0])
	prev := c.values[0]
	for i := 1; i < count; i++ {
		delta := c.values[i] - prev
		writeUvarint(dst, delta)
		prev = c.values[i]
	}
}

func (c *Uint64Column) Reset() {
	c.values = c.values[:0]
}

func isMonotonicUint64(values []uint64) bool {
	for i := 1; i < len(values); i++ {
		if values[i] < values[i-1] {
			return false
		}
	}
	return true
}
