package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"dkv/internal/paxos"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketAcceptor = []byte("acceptor")
	bucketChosen   = []byte("chosen")
	bucketDedup    = []byte("dedup")
	bucketMeta     = []byte("meta")

	keyMaxChosen = []byte("max_chosen_index")
)

// BoltStore implements Store backed by BoltDB.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens (or creates) a BoltDB database at the given path.
func NewBoltStore(dir string) (*BoltStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	dbPath := filepath.Join(dir, "paxos.db")
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{NoSync: false})
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}
	// Create buckets.
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketAcceptor, bucketChosen, bucketDedup, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
	}
	return &BoltStore{db: db}, nil
}

func indexKey(index int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(index))
	return b
}

// SaveAcceptorState persists acceptor state for a log index.
func (s *BoltStore) SaveAcceptorState(index int64, state *paxos.AcceptorState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAcceptor).Put(indexKey(index), data)
	})
}

// LoadAcceptorState reads persisted acceptor state.
func (s *BoltStore) LoadAcceptorState(index int64) (*paxos.AcceptorState, error) {
	var state paxos.AcceptorState
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketAcceptor).Get(indexKey(index))
		if v == nil {
			return nil
		}
		return json.Unmarshal(v, &state)
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// SaveChosenValue persists a chosen operation and updates max chosen index.
func (s *BoltStore) SaveChosenValue(index int64, op *paxos.Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketChosen).Put(indexKey(index), data); err != nil {
			return err
		}
		// Update max chosen index.
		meta := tx.Bucket(bucketMeta)
		cur := int64(0)
		if v := meta.Get(keyMaxChosen); v != nil {
			cur = int64(binary.BigEndian.Uint64(v))
		}
		if index > cur {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(index))
			return meta.Put(keyMaxChosen, b)
		}
		return nil
	})
}

// LoadChosenValue reads a chosen operation for a log index.
func (s *BoltStore) LoadChosenValue(index int64) (*paxos.Operation, error) {
	var op paxos.Operation
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketChosen).Get(indexKey(index))
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &op)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &op, nil
}

// LoadAllChosen returns every chosen log entry.
func (s *BoltStore) LoadAllChosen() (map[int64]*paxos.Operation, error) {
	result := make(map[int64]*paxos.Operation)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChosen)
		return b.ForEach(func(k, v []byte) error {
			index := int64(binary.BigEndian.Uint64(k))
			var op paxos.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return err
			}
			result[index] = &op
			return nil
		})
	})
	return result, err
}

// GetMaxChosenIndex returns the highest log index that has a chosen value.
func (s *BoltStore) GetMaxChosenIndex() (int64, error) {
	var idx int64
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMeta).Get(keyMaxChosen)
		if v != nil {
			idx = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return idx, err
}

// SaveDedupEntry persists client dedup information.
func (s *BoltStore) SaveDedupEntry(clientID string, entry *DedupEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDedup).Put([]byte(clientID), data)
	})
}

// LoadDedupEntry reads a dedup entry for the given client.
func (s *BoltStore) LoadDedupEntry(clientID string) (*DedupEntry, error) {
	var entry DedupEntry
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketDedup).Get([]byte(clientID))
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &entry)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &entry, nil
}

// Close closes the underlying BoltDB.
func (s *BoltStore) Close() error {
	return s.db.Close()
}
