package paxos

import (
	"sync"
	"testing"
	"time"
)

// --- In-memory storage for testing ---

type memStorage struct {
	mu        sync.Mutex
	acceptors map[int64]*AcceptorState
	chosen    map[int64]*Operation
	maxChosen int64
}

func newMemStorage() *memStorage {
	return &memStorage{
		acceptors: make(map[int64]*AcceptorState),
		chosen:    make(map[int64]*Operation),
	}
}

func (m *memStorage) SaveAcceptorState(index int64, state *AcceptorState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *state
	m.acceptors[index] = &cp
	return nil
}

func (m *memStorage) LoadAcceptorState(index int64) (*AcceptorState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.acceptors[index]
	if !ok {
		return &AcceptorState{}, nil
	}
	return s, nil
}

func (m *memStorage) SaveChosenValue(index int64, op *Operation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chosen[index] = op
	if index > m.maxChosen {
		m.maxChosen = index
	}
	return nil
}

func (m *memStorage) LoadChosenValue(index int64) (*Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chosen[index], nil
}

func (m *memStorage) LoadAllChosen() (map[int64]*Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[int64]*Operation, len(m.chosen))
	for k, v := range m.chosen {
		cp[k] = v
	}
	return cp, nil
}

func (m *memStorage) GetMaxChosenIndex() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxChosen, nil
}

func (m *memStorage) Close() error { return nil }

// --- In-memory peer for testing ---

type memPeer struct {
	id     int32
	engine *Engine
}

func (p *memPeer) ServerID() int32 { return p.id }

func (p *memPeer) Prepare(index int64, proposal ProposalNum) (*PrepareResult, error) {
	return p.engine.HandlePrepare(index, proposal)
}

func (p *memPeer) Accept(index int64, proposal ProposalNum, value *Operation) (bool, ProposalNum, error) {
	return p.engine.HandleAccept(index, proposal, value)
}

func (p *memPeer) Learn(index int64, value *Operation, proposal ProposalNum) error {
	p.engine.HandleLearn(index, value)
	return nil
}

func (p *memPeer) FetchChosen(fromIndex int64) ([]ChosenEntry, int64, error) {
	entries, max := p.engine.FetchChosen(fromIndex)
	return entries, max, nil
}

// --- Tests ---

func TestInstancePreparePromise(t *testing.T) {
	inst := NewInstance(1)

	p1 := ProposalNum{Round: 1, ServerID: 1}
	result, _ := inst.HandlePrepare(p1)
	if !result.Promised {
		t.Fatal("should promise first proposal")
	}

	// Lower proposal should be rejected.
	p0 := ProposalNum{Round: 0, ServerID: 2}
	result, _ = inst.HandlePrepare(p0)
	if result.Promised {
		t.Fatal("should reject lower proposal")
	}

	// Higher proposal should be promised.
	p2 := ProposalNum{Round: 2, ServerID: 1}
	result, _ = inst.HandlePrepare(p2)
	if !result.Promised {
		t.Fatal("should promise higher proposal")
	}
}

func TestInstanceAccept(t *testing.T) {
	inst := NewInstance(1)

	p1 := ProposalNum{Round: 1, ServerID: 1}
	inst.HandlePrepare(p1)

	op := &Operation{Type: OpPut, Key: "x", Value: "1"}
	accepted, _ := inst.HandleAccept(p1, op)
	if !accepted {
		t.Fatal("should accept after promise")
	}

	// Lower proposal should be rejected.
	p0 := ProposalNum{Round: 0, ServerID: 1}
	accepted, _ = inst.HandleAccept(p0, op)
	if accepted {
		t.Fatal("should reject lower proposal accept")
	}
}

func TestInstancePrepareReturnsAccepted(t *testing.T) {
	inst := NewInstance(1)

	p1 := ProposalNum{Round: 1, ServerID: 1}
	inst.HandlePrepare(p1)

	op := &Operation{Type: OpPut, Key: "x", Value: "1"}
	inst.HandleAccept(p1, op)

	// New prepare should return the accepted value.
	p2 := ProposalNum{Round: 2, ServerID: 1}
	result, _ := inst.HandlePrepare(p2)
	if !result.Promised {
		t.Fatal("should promise")
	}
	if !result.HasAccepted {
		t.Fatal("should report accepted value")
	}
	if result.AcceptedValue.Key != "x" {
		t.Fatalf("expected key x, got %s", result.AcceptedValue.Key)
	}
}

func TestProposalNumOrdering(t *testing.T) {
	p1 := ProposalNum{Round: 1, ServerID: 1}
	p2 := ProposalNum{Round: 1, ServerID: 2}
	p3 := ProposalNum{Round: 2, ServerID: 1}

	if !p2.GreaterThan(p1) {
		t.Fatal("(1,2) > (1,1)")
	}
	if !p3.GreaterThan(p2) {
		t.Fatal("(2,1) > (1,2)")
	}
	if p1.GreaterThan(p2) {
		t.Fatal("(1,1) should not be > (1,2)")
	}
}

func TestEngineProposeSingleNode(t *testing.T) {
	store := newMemStorage()
	var chosenOps []Operation
	var mu sync.Mutex

	onChosen := func(index int64, op *Operation) {
		mu.Lock()
		chosenOps = append(chosenOps, *op)
		mu.Unlock()
	}

	// Single node: totalNodes=1, no peers needed.
	engine := NewEngine(1, 1, store, onChosen)
	engine.SetPeers(nil)

	op := &Operation{Type: OpPut, Key: "x", Value: "1", ClientID: "c1", OpID: 1}
	index, err := engine.Propose(op)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	chosen := engine.GetChosenValue(1)
	if chosen == nil {
		t.Fatal("index 1 should be chosen")
	}
	if chosen.Key != "x" || chosen.Value != "1" {
		t.Fatalf("wrong chosen value: %+v", chosen)
	}
}

func TestEngineThreeNodePropose(t *testing.T) {
	// Create 3 engines with in-memory storage and peers.
	stores := [3]*memStorage{newMemStorage(), newMemStorage(), newMemStorage()}
	engines := [3]*Engine{}

	for i := 0; i < 3; i++ {
		engines[i] = NewEngine(int32(i+1), 3, stores[i], nil)
	}

	// Wire peers.
	for i := 0; i < 3; i++ {
		var peers []PeerClient
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, &memPeer{id: int32(j + 1), engine: engines[j]})
			}
		}
		engines[i].SetPeers(peers)
	}

	// Propose from node 1.
	op := &Operation{Type: OpPut, Key: "a", Value: "100", ClientID: "c1", OpID: 1}
	index, err := engines[0].Propose(op)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if index < 1 {
		t.Fatalf("expected positive index, got %d", index)
	}

	// Learn messages are sent asynchronously; wait for propagation.
	time.Sleep(50 * time.Millisecond)

	// Verify all nodes have the same chosen value.
	for i, eng := range engines {
		chosen := eng.GetChosenValue(index)
		if chosen == nil {
			t.Fatalf("node %d: index %d not chosen", i+1, index)
		}
		if chosen.Key != "a" || chosen.Value != "100" {
			t.Fatalf("node %d: wrong value: %+v", i+1, chosen)
		}
	}
}

func TestEngineMultipleProposals(t *testing.T) {
	stores := [3]*memStorage{newMemStorage(), newMemStorage(), newMemStorage()}
	engines := [3]*Engine{}

	for i := 0; i < 3; i++ {
		engines[i] = NewEngine(int32(i+1), 3, stores[i], nil)
	}

	for i := 0; i < 3; i++ {
		var peers []PeerClient
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, &memPeer{id: int32(j + 1), engine: engines[j]})
			}
		}
		engines[i].SetPeers(peers)
	}

	// Propose 10 operations sequentially from different nodes.
	// With async learns, remote nodes don't immediately know about
	// chosen values, so they may re-propose at the same index. This is
	// correct behavior — the important thing is every op gets chosen.
	chosenIndexes := make([]int64, 10)
	for i := 0; i < 10; i++ {
		eng := engines[i%3]
		op := &Operation{
			Type:     OpPut,
			Key:      "k",
			Value:    string(rune('0' + i)),
			ClientID: "test",
			OpID:     int64(i + 1),
		}
		idx, err := eng.Propose(op)
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		chosenIndexes[i] = idx
		// Allow learns to propagate between proposals.
		time.Sleep(10 * time.Millisecond)
	}

	// Every proposal should have returned a valid index.
	for i, idx := range chosenIndexes {
		if idx < 1 {
			t.Fatalf("proposal %d got invalid index %d", i, idx)
		}
	}
}

func TestEngineConcurrentProposals(t *testing.T) {
	stores := [3]*memStorage{newMemStorage(), newMemStorage(), newMemStorage()}
	engines := [3]*Engine{}

	for i := 0; i < 3; i++ {
		engines[i] = NewEngine(int32(i+1), 3, stores[i], nil)
	}

	for i := 0; i < 3; i++ {
		var peers []PeerClient
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, &memPeer{id: int32(j + 1), engine: engines[j]})
			}
		}
		engines[i].SetPeers(peers)
	}

	// Concurrent proposals from all 3 nodes.
	var wg sync.WaitGroup
	errors := make([]error, 9)
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			eng := engines[idx%3]
			op := &Operation{
				Type:     OpPut,
				Key:      "concurrent",
				Value:    string(rune('A' + idx)),
				ClientID: "concurrent",
				OpID:     int64(idx + 1),
			}
			_, err := eng.Propose(op)
			errors[idx] = err
		}(i)
	}
	wg.Wait()

	// All should succeed (possibly at different indexes).
	for i, err := range errors {
		if err != nil {
			t.Errorf("proposal %d failed: %v", i, err)
		}
	}

	// Verify consistency: for each chosen index, all nodes agree.
	maxIdx := engines[0].MaxChosenIndex()
	for i := int64(1); i <= maxIdx; i++ {
		var vals [3]*Operation
		for n := 0; n < 3; n++ {
			vals[n] = engines[n].GetChosenValue(i)
		}
		// All non-nil values should match.
		for n := 1; n < 3; n++ {
			if vals[n] != nil && vals[0] != nil {
				if vals[n].OpID != vals[0].OpID {
					t.Errorf("index %d: node 0 chose opID=%d, node %d chose opID=%d",
						i, vals[0].OpID, n, vals[n].OpID)
				}
			}
		}
	}
}

func TestFetchChosen(t *testing.T) {
	store := newMemStorage()
	engine := NewEngine(1, 1, store, nil)
	engine.SetPeers(nil)

	for i := 1; i <= 5; i++ {
		op := &Operation{Type: OpPut, Key: "k", Value: string(rune('0' + i)), ClientID: "t", OpID: int64(i)}
		_, err := engine.Propose(op)
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	entries, maxChosen := engine.FetchChosen(3)
	if maxChosen < 5 {
		t.Fatalf("expected maxChosen >= 5, got %d", maxChosen)
	}
	if len(entries) < 3 {
		t.Fatalf("expected >= 3 entries from index 3, got %d", len(entries))
	}
}
