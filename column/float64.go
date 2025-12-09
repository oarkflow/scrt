package column

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Float64Column encodes float64 values via IEEE754 little-endian bytes.
type Float64Column struct {
	values []float64
}

func NewFloat64Column() *Float64Column {
	return &Float64Column{values: make([]float64, 0, 256)}
}

func (c *Float64Column) Append(v float64) {
	c.values = append(c.values, v)
}

func (c *Float64Column) Encode(dst *bytes.Buffer) {
	buf := appendUvarint(nil, uint64(len(c.values)))
	for _, v := range c.values {
		bits := math.Float64bits(v)
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], bits)
		buf = append(buf, tmp[:]...)
	}
	dst.Write(buf)
}

func (c *Float64Column) Reset() {
	c.values = c.values[:0]
}
