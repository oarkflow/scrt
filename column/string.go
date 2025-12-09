package column

import "bytes"

// StringColumn encodes strings through a page-local dictionary.
type StringColumn struct {
	dict       map[string]uint32
	dictionary []string
	indexes    []uint32
}

func NewStringColumn() *StringColumn {
	return &StringColumn{
		dict:       make(map[string]uint32, 256),
		dictionary: make([]string, 0, 256),
		indexes:    make([]uint32, 0, 256),
	}
}

func (c *StringColumn) Append(v string) {
	if id, ok := c.dict[v]; ok {
		c.indexes = append(c.indexes, id)
		return
	}
	id := uint32(len(c.dictionary))
	c.dict[v] = id
	c.dictionary = append(c.dictionary, v)
	c.indexes = append(c.indexes, id)
}

func (c *StringColumn) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.dictionary)))
	for _, term := range c.dictionary {
		buf = appendUvarint(buf, uint64(len(term)))
		buf = append(buf, term...)
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
	c.dictionary = c.dictionary[:0]
	c.indexes = c.indexes[:0]
}
