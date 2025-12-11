package storage

import (
	"fmt"

	"github.com/oarkflow/scrt/schema"
)

// Backend defines the persistence contract for SCRT payloads.
type Backend interface {
	Persist(schemaName string, sch *schema.Schema, payload []byte, opts PersistOptions) (*SnapshotMeta, error)
	LoadPayload(schemaName string) ([]byte, error)
	Delete(schemaName string) error
	NextAutoValue(schemaName string, sch *schema.Schema, field string) (uint64, error)
	LoadMeta(schemaName string) (*SnapshotMeta, error)
	ListMeta() ([]*SnapshotMeta, error)
}

// SnapshotBackend wraps SnapshotStore to satisfy the Backend interface for
// filesystem snapshots.
type SnapshotBackend struct {
	store *SnapshotStore
}

// NewSnapshotBackend constructs a file-backed Backend rooted at path.
func NewSnapshotBackend(root string) (*SnapshotBackend, error) {
	store, err := NewSnapshotStore(root)
	if err != nil {
		return nil, err
	}
	return &SnapshotBackend{store: store}, nil
}

// Persist writes the SCRT payload along with row/column indexes.
func (b *SnapshotBackend) Persist(schemaName string, sch *schema.Schema, payload []byte, opts PersistOptions) (*SnapshotMeta, error) {
	if b == nil {
		return nil, ErrBackendUnavailable
	}
	return b.store.Persist(schemaName, sch, payload, opts)
}

// LoadPayload fetches the latest snapshot for schemaName.
func (b *SnapshotBackend) LoadPayload(schemaName string) ([]byte, error) {
	if b == nil {
		return nil, ErrBackendUnavailable
	}
	return b.store.LoadPayload(schemaName)
}

// Delete removes all persisted artifacts for schemaName.
func (b *SnapshotBackend) Delete(schemaName string) error {
	if b == nil {
		return ErrBackendUnavailable
	}
	return b.store.Delete(schemaName)
}

// NextAutoValue computes the next auto-increment value for field.
func (b *SnapshotBackend) NextAutoValue(schemaName string, sch *schema.Schema, field string) (uint64, error) {
	if b == nil {
		return 0, ErrBackendUnavailable
	}
	return b.store.NextAutoValue(schemaName, sch, field)
}

func (b *SnapshotBackend) LoadMeta(schemaName string) (*SnapshotMeta, error) {
	if b == nil {
		return nil, ErrBackendUnavailable
	}
	return b.store.LoadMeta(schemaName)
}

func (b *SnapshotBackend) ListMeta() ([]*SnapshotMeta, error) {
	if b == nil {
		return nil, ErrBackendUnavailable
	}
	return b.store.ListMeta()
}

var nullBackend *SnapshotBackend

// ErrBackendUnavailable signals that no storage backend was configured.
var ErrBackendUnavailable = fmt.Errorf("storage: backend unavailable")
