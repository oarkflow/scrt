package storage

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	rowIndexMagic   = "RIDX"
	rowIndexVersion = uint16(1)
	rowIndexHeader  = 4 + 2 + 2 + 8 // magic + version + reserved + row count
)

// RowLocator pinpoints a row within the SCRT payload by page offset and slot.
type RowLocator struct {
	PageOffset uint64 // byte offset where the page length varint begins
	RowInPage  uint16 // zero-based row index inside the page
}

// RowIndex offers O(1) translation from row IDs to payload offsets.
type RowIndex struct {
	locations []RowLocator
}

// RowCount reports the number of rows represented by the index.
func (ri *RowIndex) RowCount() uint64 {
	if ri == nil {
		return 0
	}
	return uint64(len(ri.locations))
}

// Lookup returns the locator for the provided row identifier.
func (ri *RowIndex) Lookup(rowID uint64) (RowLocator, bool) {
	if ri == nil || rowID >= uint64(len(ri.locations)) {
		return RowLocator{}, false
	}
	return ri.locations[rowID], true
}

// BuildRowIndex parses a SCRT payload into per-row locators.
func BuildRowIndex(payload []byte) (*RowIndex, error) {
	const streamHeaderLen = 4 + 1 + 8 // magic + version + fingerprint
	if len(payload) < streamHeaderLen {
		return nil, fmt.Errorf("storage: payload too small for header")
	}
	offset := streamHeaderLen
	locs := make([]RowLocator, 0, 1024)
	for offset < len(payload) {
		pageOffset := offset
		length, n := binary.Uvarint(payload[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("storage: malformed page length at offset %d", offset)
		}
		offset += n
		if length == 0 {
			break
		}
		end := offset + int(length)
		if end > len(payload) {
			return nil, io.ErrUnexpectedEOF
		}
		page := payload[offset:end]
		rows, consumed := binary.Uvarint(page)
		if consumed <= 0 {
			return nil, fmt.Errorf("storage: malformed row count at offset %d", offset)
		}
		pageRows := int(rows)
		for i := 0; i < pageRows; i++ {
			locs = append(locs, RowLocator{PageOffset: uint64(pageOffset), RowInPage: uint16(i)})
		}
		offset = end
	}
	return &RowIndex{locations: locs}, nil
}

// WriteTo serializes the row index to w.
func (ri *RowIndex) WriteTo(w io.Writer) error {
	if ri == nil {
		return fmt.Errorf("storage: row index is nil")
	}
	var header [rowIndexHeader]byte
	copy(header[:4], rowIndexMagic)
	binary.LittleEndian.PutUint16(header[4:6], rowIndexVersion)
	// bytes 6:8 reserved
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(ri.locations)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	var entry [10]byte
	for _, loc := range ri.locations {
		binary.LittleEndian.PutUint64(entry[:8], loc.PageOffset)
		binary.LittleEndian.PutUint16(entry[8:10], loc.RowInPage)
		if _, err := w.Write(entry[:]); err != nil {
			return err
		}
	}
	return nil
}

// ReadRowIndex restores an index previously written via WriteTo.
func ReadRowIndex(r io.Reader) (*RowIndex, error) {
	head := make([]byte, rowIndexHeader)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}
	if string(head[:4]) != rowIndexMagic {
		return nil, fmt.Errorf("storage: invalid row index magic")
	}
	version := binary.LittleEndian.Uint16(head[4:6])
	if version != rowIndexVersion {
		return nil, fmt.Errorf("storage: unsupported row index version %d", version)
	}
	count := binary.LittleEndian.Uint64(head[8:16])
	locs := make([]RowLocator, count)
	var entry [10]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(r, entry[:]); err != nil {
			return nil, err
		}
		locs[i] = RowLocator{
			PageOffset: binary.LittleEndian.Uint64(entry[:8]),
			RowInPage:  binary.LittleEndian.Uint16(entry[8:10]),
		}
	}
	return &RowIndex{locations: locs}, nil
}
