package column

import "bytes"

// BytesColumn encodes byte slices with length prefixes.
type BytesColumn struct {
	values [][]byte
}

func NewBytesColumn() *BytesColumn {
	return &BytesColumn{values: make([][]byte, 0, 256)}
}

func (c *BytesColumn) Append(v []byte) {
	buf := make([]byte, len(v))
	copy(buf, v)
	c.values = append(c.values, buf)
}

func (c *BytesColumn) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.values)))
	for _, v := range c.values {
		buf = appendUvarint(buf, uint64(len(v)))
		buf = append(buf, v...)
	}
	dst.Write(buf)
}

func (c *BytesColumn) Reset() {
	for i := range c.values {
		c.values[i] = nil
	}
	c.values = c.values[:0]
}
