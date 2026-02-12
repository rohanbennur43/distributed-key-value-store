package paxos

import (
	"fmt"
)

// ProposalNum uniquely identifies a proposal. Ordered by (Round, ServerID).
type ProposalNum struct {
	Round    int64
	ServerID int32
}

func (p ProposalNum) GreaterThan(o ProposalNum) bool {
	if p.Round != o.Round {
		return p.Round > o.Round
	}
	return p.ServerID > o.ServerID
}

func (p ProposalNum) GreaterOrEqual(o ProposalNum) bool {
	return p == o || p.GreaterThan(o)
}

func (p ProposalNum) IsZero() bool {
	return p.Round == 0 && p.ServerID == 0
}

func (p ProposalNum) String() string {
	return fmt.Sprintf("(%d,%d)", p.Round, p.ServerID)
}

// OpType enumerates the kinds of operations in the replicated log.
type OpType int

const (
	OpPut    OpType = 0
	OpAppend OpType = 1
	OpDelete OpType = 2
	OpNoop   OpType = 3
)

func (t OpType) String() string {
	switch t {
	case OpPut:
		return "PUT"
	case OpAppend:
		return "APPEND"
	case OpDelete:
		return "DELETE"
	case OpNoop:
		return "NOOP"
	default:
		return "UNKNOWN"
	}
}

// Operation is a single entry in the replicated log.
type Operation struct {
	Type     OpType
	Key      string
	Value    string
	ClientID string
	OpID     int64
}

func (o Operation) String() string {
	return fmt.Sprintf("%s(%s=%s, cid=%s, oid=%d)", o.Type, o.Key, o.Value, o.ClientID, o.OpID)
}

// AcceptorState is the persisted state of a Paxos acceptor for one log index.
type AcceptorState struct {
	HighestPromised  ProposalNum `json:"highest_promised"`
	AcceptedProposal ProposalNum `json:"accepted_proposal"`
	AcceptedValue    *Operation  `json:"accepted_value,omitempty"`
}

// PrepareResult is the response from a single acceptor to a Prepare RPC.
type PrepareResult struct {
	Promised         bool
	AcceptedProposal ProposalNum
	AcceptedValue    *Operation
	HasAccepted      bool
	HighestPromised  ProposalNum
}

// ChosenEntry is a committed log entry.
type ChosenEntry struct {
	Index int64
	Value Operation
}

// PeerClient is the interface for sending Paxos RPCs to a peer.
type PeerClient interface {
	ServerID() int32
	Prepare(index int64, proposal ProposalNum) (*PrepareResult, error)
	Accept(index int64, proposal ProposalNum, value *Operation) (accepted bool, highestPromised ProposalNum, err error)
	Learn(index int64, value *Operation, proposal ProposalNum) error
	FetchChosen(fromIndex int64) ([]ChosenEntry, int64, error)
}

// Storage is the persistence interface for the Paxos engine.
type Storage interface {
	SaveAcceptorState(index int64, state *AcceptorState) error
	LoadAcceptorState(index int64) (*AcceptorState, error)
	SaveChosenValue(index int64, op *Operation) error
	LoadChosenValue(index int64) (*Operation, error)
	LoadAllChosen() (map[int64]*Operation, error)
	GetMaxChosenIndex() (int64, error)
	Close() error
}
