package column

import (
	"bytes"
	"math"
)

// StringColumn encodes strings through a page-local dictionary backed by arenas.
type StringColumn struct {
	dict       map[string]uint32
	indexes    []uint32
	arena      []byte
	strOffsets []uint32
	strLens    []uint32
}

func NewStringColumn() *StringColumn {
	return &StringColumn{
		dict:       make(map[string]uint32, 256),
		indexes:    make([]uint32, 0, 256),
		arena:      make([]byte, 0, 4096),
		strOffsets: make([]uint32, 0, 256),
		strLens:    make([]uint32, 0, 256),
	}
}

func (c *StringColumn) Append(v string) {
	if id, ok := c.dict[v]; ok {
		c.indexes = append(c.indexes, id)
		return
	}
	id := uint32(len(c.strOffsets))
	c.dict[v] = id
	if len(v) > math.MaxUint32 || len(c.arena)+len(v) > math.MaxUint32 {
		panic("string column value exceeds 4GB")
	}
	c.strOffsets = append(c.strOffsets, uint32(len(c.arena)))
	c.strLens = append(c.strLens, uint32(len(v)))
	c.arena = append(c.arena, v...)
	c.indexes = append(c.indexes, id)
}

func (c *StringColumn) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.strOffsets)))
	for i := range c.strOffsets {
		length := c.strLens[i]
		buf = appendUvarint(buf, uint64(length))
		start := c.strOffsets[i]
		buf = append(buf, c.arena[start:start+length]...)
	}
	buf = appendUvarint(buf, uint64(len(c.indexes)))
	for _, idx := range c.indexes {
		buf = appendUvarint(buf, uint64(idx))
	}
	dst.Write(buf)
}

func (c *StringColumn) Reset() {
	for k := range c.dict {
		delete(c.dict, k)
	}
	c.indexes = c.indexes[:0]
	c.arena = c.arena[:0]
	c.strOffsets = c.strOffsets[:0]
	c.strLens = c.strLens[:0]
}
