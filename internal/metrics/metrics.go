package metrics

import "sync/atomic"

// Metrics holds atomic counters for observability.
type Metrics struct {
	AppliedOpsTotal          atomic.Int64
	DuplicateSuppressedTotal atomic.Int64
	PaxosPrepareTotal        atomic.Int64
	PaxosAcceptTotal         atomic.Int64
	PaxosProposeTotal        atomic.Int64
	PaxosProposeFailTotal    atomic.Int64
	ChosenIndex              atomic.Int64
	LastAppliedIndex         atomic.Int64
	CatchupEntriesApplied    atomic.Int64
	LearnReceivedTotal       atomic.Int64
}

// Snapshot returns all metrics as a string-keyed map.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"applied_ops_total":           m.AppliedOpsTotal.Load(),
		"duplicate_suppressed_total":  m.DuplicateSuppressedTotal.Load(),
		"paxos_prepare_total":         m.PaxosPrepareTotal.Load(),
		"paxos_accept_total":          m.PaxosAcceptTotal.Load(),
		"paxos_propose_total":         m.PaxosProposeTotal.Load(),
		"paxos_propose_fail_total":    m.PaxosProposeFailTotal.Load(),
		"chosen_index":                m.ChosenIndex.Load(),
		"last_applied_index":          m.LastAppliedIndex.Load(),
		"catchup_entries_applied":     m.CatchupEntriesApplied.Load(),
		"learn_received_total":        m.LearnReceivedTotal.Load(),
	}
}
