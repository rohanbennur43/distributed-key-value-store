package paxos

import "sync"

// Instance manages the Paxos state for a single log index.
// Each instance acts as both acceptor and proposer for its index.
type Instance struct {
	mu    sync.Mutex
	Index int64

	// Acceptor state — must be persisted before responding.
	HighestPromised  ProposalNum
	AcceptedProposal ProposalNum
	AcceptedValue    *Operation

	// Learner state.
	Chosen      bool
	ChosenValue *Operation

	// Proposer bookkeeping (transient).
	highestSeenRound int64
}

// NewInstance creates a fresh Paxos instance for the given log index.
func NewInstance(index int64) *Instance {
	return &Instance{Index: index}
}

// RestoreAcceptor restores persisted acceptor state.
func (inst *Instance) RestoreAcceptor(state *AcceptorState) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.HighestPromised = state.HighestPromised
	inst.AcceptedProposal = state.AcceptedProposal
	inst.AcceptedValue = state.AcceptedValue
	if state.HighestPromised.Round > inst.highestSeenRound {
		inst.highestSeenRound = state.HighestPromised.Round
	}
}

// MarkChosen marks this instance as decided.
func (inst *Instance) MarkChosen(value *Operation) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.Chosen = true
	inst.ChosenValue = value
}

// NextProposal generates a proposal number higher than anything this instance has seen.
func (inst *Instance) NextProposal(serverID int32) ProposalNum {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	maxRound := inst.highestSeenRound
	if inst.HighestPromised.Round > maxRound {
		maxRound = inst.HighestPromised.Round
	}
	p := ProposalNum{Round: maxRound + 1, ServerID: serverID}
	inst.highestSeenRound = p.Round
	return p
}

// ObserveProposal updates the highest seen round (e.g., from a Nack).
func (inst *Instance) ObserveProposal(p ProposalNum) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if p.Round > inst.highestSeenRound {
		inst.highestSeenRound = p.Round
	}
}

// HandlePrepare processes a Phase 1a Prepare message (acceptor role).
// Returns the promise result. Caller must persist state before sending the response.
func (inst *Instance) HandlePrepare(proposal ProposalNum) (result PrepareResult, newState *AcceptorState) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if proposal.GreaterThan(inst.HighestPromised) {
		inst.HighestPromised = proposal
		result = PrepareResult{
			Promised:         true,
			AcceptedProposal: inst.AcceptedProposal,
			AcceptedValue:    inst.AcceptedValue,
			HasAccepted:      inst.AcceptedValue != nil,
			HighestPromised:  inst.HighestPromised,
		}
	} else {
		result = PrepareResult{
			Promised:        false,
			HighestPromised: inst.HighestPromised,
		}
	}
	newState = &AcceptorState{
		HighestPromised:  inst.HighestPromised,
		AcceptedProposal: inst.AcceptedProposal,
		AcceptedValue:    inst.AcceptedValue,
	}
	return
}

// HandleAccept processes a Phase 2a Accept message (acceptor role).
// Returns whether the value was accepted. Caller must persist before responding.
func (inst *Instance) HandleAccept(proposal ProposalNum, value *Operation) (accepted bool, newState *AcceptorState) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if proposal.GreaterOrEqual(inst.HighestPromised) {
		inst.HighestPromised = proposal
		inst.AcceptedProposal = proposal
		inst.AcceptedValue = value
		accepted = true
	}

	newState = &AcceptorState{
		HighestPromised:  inst.HighestPromised,
		AcceptedProposal: inst.AcceptedProposal,
		AcceptedValue:    inst.AcceptedValue,
	}
	return
}

// GetAcceptorState returns the current acceptor state snapshot.
func (inst *Instance) GetAcceptorState() AcceptorState {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return AcceptorState{
		HighestPromised:  inst.HighestPromised,
		AcceptedProposal: inst.AcceptedProposal,
		AcceptedValue:    inst.AcceptedValue,
	}
}
