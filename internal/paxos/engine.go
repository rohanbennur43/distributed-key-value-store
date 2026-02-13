package paxos

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNoMajority    = errors.New("paxos: could not reach majority")
	ErrTooManyRetries = errors.New("paxos: too many retries")
	ErrShutdown      = errors.New("paxos: engine shut down")
)

// OnChosenFunc is called when a value is chosen at a log index.
type OnChosenFunc func(index int64, op *Operation)

// Engine manages multiple Paxos instances (one per log index)
// and provides the propose interface.
type Engine struct {
	mu        sync.RWMutex
	serverID  int32
	instances map[int64]*Instance
	maxChosen int64
	storage   Storage
	peers     []PeerClient
	totalNodes int // including self

	onChosen OnChosenFunc

	shutdownCh chan struct{}
	closed     bool
}

// NewEngine creates a Paxos engine for the given server.
func NewEngine(serverID int32, totalNodes int, storage Storage, onChosen OnChosenFunc) *Engine {
	return &Engine{
		serverID:   serverID,
		instances:  make(map[int64]*Instance),
		storage:    storage,
		totalNodes: totalNodes,
		onChosen:   onChosen,
		shutdownCh: make(chan struct{}),
	}
}

// SetPeers sets the remote peer clients. Call before proposing.
func (e *Engine) SetPeers(peers []PeerClient) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.peers = peers
}

// Shutdown stops the engine.
func (e *Engine) Shutdown() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed {
		close(e.shutdownCh)
		e.closed = true
	}
}

func (e *Engine) majority() int {
	return e.totalNodes/2 + 1
}

// getInstance returns or creates the instance for a log index.
func (e *Engine) getInstance(index int64) *Instance {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.getInstanceLocked(index)
}

func (e *Engine) getInstanceLocked(index int64) *Instance {
	inst, ok := e.instances[index]
	if !ok {
		inst = NewInstance(index)
		e.instances[index] = inst
	}
	return inst
}

// MaxChosenIndex returns the highest log index with a chosen value.
func (e *Engine) MaxChosenIndex() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.maxChosen
}

// IsChosen returns whether a value has been chosen for the given index.
func (e *Engine) IsChosen(index int64) bool {
	e.mu.RLock()
	inst, ok := e.instances[index]
	e.mu.RUnlock()
	if !ok {
		return false
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return inst.Chosen
}

// GetChosenValue returns the chosen value for an index (nil if not chosen).
func (e *Engine) GetChosenValue(index int64) *Operation {
	e.mu.RLock()
	inst, ok := e.instances[index]
	e.mu.RUnlock()
	if !ok {
		return nil
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.Chosen {
		return inst.ChosenValue
	}
	return nil
}

// RestoreChosen loads previously chosen values into memory on startup.
func (e *Engine) RestoreChosen(entries map[int64]*Operation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for index, op := range entries {
		inst := e.getInstanceLocked(index)
		inst.Chosen = true
		inst.ChosenValue = op
		if index > e.maxChosen {
			e.maxChosen = index
		}
	}
}

// RestoreAcceptorState loads persisted acceptor state for an index.
func (e *Engine) RestoreAcceptorState(index int64, state *AcceptorState) {
	inst := e.getInstance(index)
	inst.RestoreAcceptor(state)
}

// nextAvailableIndex finds the first log index without a chosen value.
func (e *Engine) nextAvailableIndex() int64 {
	e.mu.RLock()
	maxChosen := e.maxChosen
	e.mu.RUnlock()

	// Start searching from index 1 (or just after maxChosen).
	start := maxChosen + 1
	if start < 1 {
		start = 1
	}

	// Look for gaps first.
	for i := int64(1); i <= maxChosen; i++ {
		if !e.IsChosen(i) {
			return i
		}
	}
	return start
}

// Propose runs Paxos to get an operation chosen in the replicated log.
// Returns the log index where the operation was chosen.
func (e *Engine) Propose(op *Operation) (int64, error) {
	maxAttempts := 60
	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-e.shutdownCh:
			return 0, ErrShutdown
		default:
		}

		index := e.nextAvailableIndex()
		chosenOp, err := e.proposeAt(index, op)
		if err != nil {
			// Back off on contention.
			jitter := time.Duration(rand.Intn(30)+10) * time.Millisecond
			time.Sleep(jitter)
			continue
		}

		// Check if our operation was chosen.
		if chosenOp.ClientID == op.ClientID && chosenOp.OpID == op.OpID {
			return index, nil
		}

		// Someone else's value was chosen at this index. Advance.
		log.Printf("[node %d] index %d: displaced, retrying", e.serverID, index)
	}
	return 0, ErrTooManyRetries
}

// proposeAt runs a single Paxos round for the given log index.
// Returns the chosen operation (which may differ from our proposed value).
func (e *Engine) proposeAt(index int64, op *Operation) (*Operation, error) {
	inst := e.getInstance(index)

	// If already chosen, ensure maxChosen is up-to-date (handles race
	// where a Learn goroutine set inst.Chosen but hasn't updated maxChosen
	// yet) and return the chosen value.
	inst.mu.Lock()
	if inst.Chosen {
		v := inst.ChosenValue
		inst.mu.Unlock()
		e.mu.Lock()
		if index > e.maxChosen {
			e.maxChosen = index
		}
		e.mu.Unlock()
		return v, nil
	}
	inst.mu.Unlock()

	proposal := inst.NextProposal(e.serverID)

	// Phase 1: Prepare
	promises, err := e.doPreparePhase(index, proposal, inst)
	if err != nil {
		return nil, err
	}

	// Determine value: Paxos requires adopting the value from the highest
	// accepted proposal seen in any promise. Use a flag so even a zero-valued
	// proposal number is correctly handled.
	value := op
	foundAccepted := false
	var highestAccepted ProposalNum
	for _, p := range promises {
		if p.HasAccepted && p.AcceptedValue != nil {
			if !foundAccepted || p.AcceptedProposal.GreaterThan(highestAccepted) {
				highestAccepted = p.AcceptedProposal
				copied := *p.AcceptedValue
				value = &copied
				foundAccepted = true
			}
		}
	}

	// Phase 2: Accept
	acceptCount, err := e.doAcceptPhase(index, proposal, value, inst)
	if err != nil {
		return nil, err
	}
	if acceptCount < e.majority() {
		return nil, ErrNoMajority
	}

	// Chosen!
	e.markChosen(index, value)

	// Notify learners asynchronously.
	go e.sendLearns(index, value, proposal)

	return value, nil
}

// doPreparePhase sends Prepare to all acceptors (including self) and collects promises.
func (e *Engine) doPreparePhase(index int64, proposal ProposalNum, inst *Instance) ([]PrepareResult, error) {
	type result struct {
		res PrepareResult
		err error
	}

	// Local prepare.
	localRes, newState := inst.HandlePrepare(proposal)
	if err := e.storage.SaveAcceptorState(index, newState); err != nil {
		return nil, fmt.Errorf("persist acceptor state: %w", err)
	}

	results := make([]PrepareResult, 0, e.totalNodes)
	if localRes.Promised {
		results = append(results, localRes)
	}

	// Remote prepares in parallel.
	e.mu.RLock()
	peers := e.peers
	e.mu.RUnlock()

	ch := make(chan result, len(peers))
	for _, p := range peers {
		go func(peer PeerClient) {
			res, err := peer.Prepare(index, proposal)
			if err != nil {
				ch <- result{err: err}
				return
			}
			ch <- result{res: *res}
		}(p)
	}

	// Collect responses.
	for range peers {
		r := <-ch
		if r.err != nil {
			continue
		}
		if r.res.Promised {
			results = append(results, r.res)
		} else {
			// Nack: learn the higher proposal number.
			inst.ObserveProposal(r.res.HighestPromised)
		}
		if len(results) >= e.majority() {
			break
		}
	}

	if len(results) < e.majority() {
		return nil, ErrNoMajority
	}
	return results, nil
}

// doAcceptPhase sends Accept to all acceptors and counts successes.
func (e *Engine) doAcceptPhase(index int64, proposal ProposalNum, value *Operation, inst *Instance) (int, error) {
	// Local accept.
	accepted, newState := inst.HandleAccept(proposal, value)
	if err := e.storage.SaveAcceptorState(index, newState); err != nil {
		return 0, fmt.Errorf("persist acceptor state: %w", err)
	}

	count := 0
	if accepted {
		count++
	}

	// Remote accepts in parallel.
	e.mu.RLock()
	peers := e.peers
	e.mu.RUnlock()

	type result struct {
		accepted bool
		highest  ProposalNum
		err      error
	}
	ch := make(chan result, len(peers))
	for _, p := range peers {
		go func(peer PeerClient) {
			ok, highest, err := peer.Accept(index, proposal, value)
			ch <- result{accepted: ok, highest: highest, err: err}
		}(p)
	}

	for range peers {
		r := <-ch
		if r.err != nil {
			continue
		}
		if r.accepted {
			count++
		} else {
			inst.ObserveProposal(r.highest)
		}
		if count >= e.majority() {
			break
		}
	}

	return count, nil
}

// markChosen records a value as chosen for the given index.
func (e *Engine) markChosen(index int64, value *Operation) {
	inst := e.getInstance(index)
	inst.mu.Lock()
	alreadyChosen := inst.Chosen
	inst.Chosen = true
	inst.ChosenValue = value
	inst.mu.Unlock()

	if alreadyChosen {
		return
	}

	// Persist.
	if err := e.storage.SaveChosenValue(index, value); err != nil {
		log.Printf("[node %d] ERROR persisting chosen value at index %d: %v", e.serverID, index, err)
	}

	e.mu.Lock()
	if index > e.maxChosen {
		e.maxChosen = index
	}
	e.mu.Unlock()

	// Fire callback.
	if e.onChosen != nil {
		e.onChosen(index, value)
	}
}

// sendLearns notifies all peers about a chosen value.
func (e *Engine) sendLearns(index int64, value *Operation, proposal ProposalNum) {
	e.mu.RLock()
	peers := e.peers
	e.mu.RUnlock()

	for _, p := range peers {
		go func(peer PeerClient) {
			_ = peer.Learn(index, value, proposal)
		}(p)
	}
}

// HandlePrepare processes an incoming Prepare RPC (acceptor role).
func (e *Engine) HandlePrepare(index int64, proposal ProposalNum) (*PrepareResult, error) {
	inst := e.getInstance(index)

	// If already chosen, return a synthetic promise with a max-valued
	// accepted proposal so the chosen value always wins adoption.
	inst.mu.Lock()
	if inst.Chosen {
		res := &PrepareResult{
			Promised:         true,
			AcceptedProposal: ProposalNum{Round: math.MaxInt64, ServerID: math.MaxInt32},
			AcceptedValue:    inst.ChosenValue,
			HasAccepted:      true,
		}
		inst.mu.Unlock()
		return res, nil
	}
	inst.mu.Unlock()

	result, newState := inst.HandlePrepare(proposal)
	if err := e.storage.SaveAcceptorState(index, newState); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return &result, nil
}

// HandleAccept processes an incoming Accept RPC (acceptor role).
func (e *Engine) HandleAccept(index int64, proposal ProposalNum, value *Operation) (bool, ProposalNum, error) {
	inst := e.getInstance(index)

	// If already chosen, accept is a no-op (but return success to avoid
	// blocking the proposer).
	inst.mu.Lock()
	if inst.Chosen {
		inst.mu.Unlock()
		return true, ProposalNum{}, nil
	}
	inst.mu.Unlock()

	accepted, newState := inst.HandleAccept(proposal, value)
	if err := e.storage.SaveAcceptorState(index, newState); err != nil {
		return false, ProposalNum{}, fmt.Errorf("persist: %w", err)
	}
	return accepted, newState.HighestPromised, nil
}

// HandleLearn processes an incoming Learn notification.
func (e *Engine) HandleLearn(index int64, value *Operation) {
	e.markChosen(index, value)
}

// FetchChosen returns all chosen entries starting from fromIndex.
func (e *Engine) FetchChosen(fromIndex int64) ([]ChosenEntry, int64) {
	e.mu.RLock()
	maxChosen := e.maxChosen
	e.mu.RUnlock()

	var entries []ChosenEntry
	for i := fromIndex; i <= maxChosen; i++ {
		op := e.GetChosenValue(i)
		if op != nil {
			entries = append(entries, ChosenEntry{Index: i, Value: *op})
		}
	}
	return entries, maxChosen
}

// ServerID returns this engine's server ID.
func (e *Engine) ServerID() int32 {
	return e.serverID
}
