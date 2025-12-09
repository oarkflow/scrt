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

func NewBytesColumn(capacity int) *BytesColumn {
	c := normalizeCapacityHint(capacity)
	arenaCap := c * 16
	if arenaCap < 4096 {
		arenaCap = 4096
	}
	return &BytesColumn{
		offsets: make([]uint32, 0, c),
		lengths: make([]uint32, 0, c),
		arena:   make([]byte, 0, arenaCap),
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
	writeUvarint(dst, uint64(len(c.offsets)))
	for i := range c.offsets {
		length := c.lengths[i]
		writeUvarint(dst, uint64(length))
		start := c.offsets[i]
		dst.Write(c.arena[start : start+length])
	}
}

func (c *BytesColumn) Reset() {
	c.offsets = c.offsets[:0]
	c.lengths = c.lengths[:0]
	c.arena = c.arena[:0]
}
