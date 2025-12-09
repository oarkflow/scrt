package column

import "bytes"

// Uint64Column encodes unsigned integers via compact varints.
type Uint64Column struct {
	values []uint64
}

func NewUint64Column() *Uint64Column {
	return &Uint64Column{values: make([]uint64, 0, 256)}
}

func (c *Uint64Column) Append(v uint64) {
	c.values = append(c.values, v)
}

func (c *Uint64Column) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.values)))
	for _, v := range c.values {
		buf = appendUvarint(buf, v)
	}
	dst.Write(buf)
}

func (c *Uint64Column) Reset() {
	c.values = c.values[:0]
}
