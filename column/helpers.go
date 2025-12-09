package column

import (
	"bytes"
	"encoding/binary"
)

func writeUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func writeVarint(buf *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func normalizeCapacityHint(hint int) int {
	if hint <= 0 {
		return 256
	}
	return hint
}
