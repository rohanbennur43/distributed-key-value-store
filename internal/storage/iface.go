package storage

import "dkv/internal/paxos"

// DedupEntry records the last operation seen from a client for idempotency.
type DedupEntry struct {
	LastOpID int64  `json:"last_op_id"`
	Result   string `json:"result"`     // serialized result (e.g., "" for success, error message)
	Found    bool   `json:"found"`      // for delete/get: whether key existed
	Value    string `json:"value"`      // for get: the returned value
}

// Store is the full persistence interface used by the node.
type Store interface {
	paxos.Storage

	// Dedup table
	SaveDedupEntry(clientID string, entry *DedupEntry) error
	LoadDedupEntry(clientID string) (*DedupEntry, error)
}
