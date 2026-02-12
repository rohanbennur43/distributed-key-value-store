package store

import (
	"sync"

	"dkv/internal/paxos"
	"dkv/internal/storage"
)

// OpResult is the outcome of applying a single operation.
type OpResult struct {
	Value string
	Found bool
	Err   string
}

// KVStore is the deterministic state machine backed by a map.
// Operations are applied in log order.
type KVStore struct {
	mu               sync.RWMutex
	data             map[string]string
	lastAppliedIndex int64

	// Dedup table: clientID -> entry.
	dedupMu sync.RWMutex
	dedup   map[string]*storage.DedupEntry

	// Persistence for dedup entries.
	persist storage.Store
}

// NewKVStore creates a new state machine, optionally backed by persistent storage
// for the dedup table.
func NewKVStore(persist storage.Store) *KVStore {
	return &KVStore{
		data:    make(map[string]string),
		dedup:   make(map[string]*storage.DedupEntry),
		persist: persist,
	}
}

// LastAppliedIndex returns the index of the last applied log entry.
func (kv *KVStore) LastAppliedIndex() int64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.lastAppliedIndex
}

// CheckDuplicate returns (isDuplicate, cachedResult) for a client operation.
func (kv *KVStore) CheckDuplicate(clientID string, opID int64) (bool, *storage.DedupEntry) {
	if clientID == "" {
		return false, nil
	}
	kv.dedupMu.RLock()
	defer kv.dedupMu.RUnlock()
	entry, ok := kv.dedup[clientID]
	if !ok {
		return false, nil
	}
	if opID <= entry.LastOpID {
		return true, entry
	}
	return false, nil
}

// Apply executes an operation against the state machine.
// It must be called in strict log-index order.
// Returns the result plus whether this was a duplicate that was suppressed.
func (kv *KVStore) Apply(index int64, op *paxos.Operation) (OpResult, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Safety: don't re-apply.
	if index <= kv.lastAppliedIndex {
		return OpResult{}, true
	}

	// Check dedup under the write lock (authoritative check).
	if op.ClientID != "" && op.Type != paxos.OpNoop {
		kv.dedupMu.RLock()
		entry, ok := kv.dedup[op.ClientID]
		kv.dedupMu.RUnlock()
		if ok && op.OpID <= entry.LastOpID {
			kv.lastAppliedIndex = index
			return OpResult{Value: entry.Value, Found: entry.Found}, true
		}
	}

	var result OpResult
	switch op.Type {
	case paxos.OpPut:
		kv.data[op.Key] = op.Value
		result = OpResult{}
	case paxos.OpAppend:
		kv.data[op.Key] += op.Value
		result = OpResult{}
	case paxos.OpDelete:
		_, existed := kv.data[op.Key]
		delete(kv.data, op.Key)
		result = OpResult{Found: existed}
	case paxos.OpNoop:
		// No state change.
		result = OpResult{}
	}

	kv.lastAppliedIndex = index

	// Update dedup table.
	if op.ClientID != "" && op.Type != paxos.OpNoop {
		dedupEntry := &storage.DedupEntry{
			LastOpID: op.OpID,
			Found:    result.Found,
			Value:    result.Value,
			Result:   result.Err,
		}
		kv.dedupMu.Lock()
		kv.dedup[op.ClientID] = dedupEntry
		kv.dedupMu.Unlock()

		if kv.persist != nil {
			// Best-effort persistence; errors logged but not fatal.
			_ = kv.persist.SaveDedupEntry(op.ClientID, dedupEntry)
		}
	}

	return result, false
}

// Get reads a key from the state machine. Not linearizable on its own;
// the caller must ensure the log is applied up to date.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot returns a copy of the entire KV data (for debugging).
func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp
}

// RestoreDedup loads dedup entries from persistent storage into memory.
func (kv *KVStore) RestoreDedup(entries map[string]*storage.DedupEntry) {
	kv.dedupMu.Lock()
	defer kv.dedupMu.Unlock()
	for k, v := range entries {
		kv.dedup[k] = v
	}
}
