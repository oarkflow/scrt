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
	writeUvarint(dst, uint64(len(c.values)))
	for _, v := range c.values {
		writeUvarint(dst, v)
	}
}

func (c *Uint64Column) Reset() {
	c.values = c.values[:0]
}
