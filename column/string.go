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

func NewStringColumn(capacity int) *StringColumn {
	c := normalizeCapacityHint(capacity)
	arenaCap := c * 8
	if arenaCap < 4096 {
		arenaCap = 4096
	}
	return &StringColumn{
		dict:       make(map[string]uint32, c),
		indexes:    make([]uint32, 0, c),
		arena:      make([]byte, 0, arenaCap),
		strOffsets: make([]uint32, 0, c),
		strLens:    make([]uint32, 0, c),
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
	writeUvarint(dst, uint64(len(c.strOffsets)))
	for i := range c.strOffsets {
		length := c.strLens[i]
		writeUvarint(dst, uint64(length))
		start := c.strOffsets[i]
		dst.Write(c.arena[start : start+length])
	}
	writeUvarint(dst, uint64(len(c.indexes)))
	for _, idx := range c.indexes {
		writeUvarint(dst, uint64(idx))
	}
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
