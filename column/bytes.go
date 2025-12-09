package column

import (
	"bytes"
	"math"
)

// BytesColumn encodes byte slices with length prefixes using a contiguous arena.
type BytesColumn struct {
	offsets []uint32
	lengths []uint32
	arena   []byte
}

func NewBytesColumn() *BytesColumn {
	return &BytesColumn{
		offsets: make([]uint32, 0, 256),
		lengths: make([]uint32, 0, 256),
		arena:   make([]byte, 0, 4096),
	}
}

func (c *BytesColumn) Append(v []byte) {
	if len(v) == 0 {
		c.offsets = append(c.offsets, uint32(len(c.arena)))
		c.lengths = append(c.lengths, 0)
		return
	}
	if len(v) > math.MaxUint32 {
		panic("bytes column value exceeds 4GB")
	}
	if len(c.arena)+len(v) > math.MaxUint32 {
		panic("bytes column arena exceeds 4GB")
	}
	c.offsets = append(c.offsets, uint32(len(c.arena)))
	c.lengths = append(c.lengths, uint32(len(v)))
	c.arena = append(c.arena, v...)
}

func (c *BytesColumn) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.offsets)))
	for i := range c.offsets {
		length := c.lengths[i]
		buf = appendUvarint(buf, uint64(length))
		start := c.offsets[i]
		end := start + length
		buf = append(buf, c.arena[start:end]...)
	}
	dst.Write(buf)
}

func (c *BytesColumn) Reset() {
	c.offsets = c.offsets[:0]
	c.lengths = c.lengths[:0]
	c.arena = c.arena[:0]
}
