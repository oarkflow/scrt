package page

import (
	"bytes"
	"encoding/binary"
)

func writeUvarint(buf *bytes.Buffer, v uint64) {
	// Inline varint encoding to avoid temporary buffer allocation
	// This is faster than binary.PutUvarint + Write for small values
	switch {
	case v < 0x80:
		buf.WriteByte(byte(v))
	case v < 0x4000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte(v >> 7))
	case v < 0x200000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte(v >> 14))
	case v < 0x10000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte(v >> 21))
	case v < 0x800000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte((v >> 21) | 0x80))
		buf.WriteByte(byte(v >> 28))
	case v < 0x40000000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte((v >> 21) | 0x80))
		buf.WriteByte(byte((v >> 28) | 0x80))
		buf.WriteByte(byte(v >> 35))
	case v < 0x2000000000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte((v >> 21) | 0x80))
		buf.WriteByte(byte((v >> 28) | 0x80))
		buf.WriteByte(byte((v >> 35) | 0x80))
		buf.WriteByte(byte(v >> 42))
	case v < 0x100000000000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte((v >> 21) | 0x80))
		buf.WriteByte(byte((v >> 28) | 0x80))
		buf.WriteByte(byte((v >> 35) | 0x80))
		buf.WriteByte(byte((v >> 42) | 0x80))
		buf.WriteByte(byte(v >> 49))
	case v < 0x8000000000000000:
		buf.WriteByte(byte(v | 0x80))
		buf.WriteByte(byte((v >> 7) | 0x80))
		buf.WriteByte(byte((v >> 14) | 0x80))
		buf.WriteByte(byte((v >> 21) | 0x80))
		buf.WriteByte(byte((v >> 28) | 0x80))
		buf.WriteByte(byte((v >> 35) | 0x80))
		buf.WriteByte(byte((v >> 42) | 0x80))
		buf.WriteByte(byte((v >> 49) | 0x80))
		buf.WriteByte(byte(v >> 56))
	default:
		// Fallback for very large values
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], v)
		buf.Write(tmp[:n])
	}
}
