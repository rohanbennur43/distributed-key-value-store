package server

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"dkv/internal/paxos"
	pb "dkv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCPeer implements paxos.PeerClient over gRPC.
type GRPCPeer struct {
	serverID int32
	addr     string

	mu   sync.Mutex
	conn *grpc.ClientConn
	pxc  pb.PaxosServiceClient

	// Drop rules for fault injection.
	dropMu   sync.RWMutex
	dropRate float64
}

// NewGRPCPeer creates a peer client for a remote node.
func NewGRPCPeer(serverID int32, addr string) *GRPCPeer {
	return &GRPCPeer{
		serverID: serverID,
		addr:     addr,
	}
}

func (p *GRPCPeer) ServerID() int32 { return p.serverID }

func (p *GRPCPeer) SetDropRate(rate float64) {
	p.dropMu.Lock()
	defer p.dropMu.Unlock()
	p.dropRate = rate
}

func (p *GRPCPeer) shouldDrop() bool {
	p.dropMu.RLock()
	rate := p.dropRate
	p.dropMu.RUnlock()
	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}

func (p *GRPCPeer) connect() (pb.PaxosServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pxc != nil {
		return p.pxc, nil
	}
	conn, err := grpc.Dial(p.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second),
	)
	if err != nil {
		return nil, err
	}
	p.conn = conn
	p.pxc = pb.NewPaxosServiceClient(conn)
	return p.pxc, nil
}

func (p *GRPCPeer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		p.pxc = nil
	}
}

// Reset forces reconnection on next RPC (used after simulated partition heals).
func (p *GRPCPeer) Reset() {
	p.Close()
}

func (p *GRPCPeer) Prepare(index int64, proposal paxos.ProposalNum) (*paxos.PrepareResult, error) {
	if p.shouldDrop() {
		return nil, context.DeadlineExceeded
	}
	client, err := p.connect()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Prepare(ctx, &pb.PrepareRequest{
		LogIndex: index,
		Proposal: &pb.ProposalNumber{Round: proposal.Round, ServerId: proposal.ServerID},
	})
	if err != nil {
		return nil, err
	}

	result := &paxos.PrepareResult{
		Promised: resp.Promised,
	}
	if resp.HighestPromised != nil {
		result.HighestPromised = paxos.ProposalNum{
			Round: resp.HighestPromised.Round, ServerID: resp.HighestPromised.ServerId,
		}
	}
	if resp.HasAccepted && resp.AcceptedValue != nil {
		result.HasAccepted = true
		result.AcceptedProposal = paxos.ProposalNum{
			Round: resp.AcceptedProposal.Round, ServerID: resp.AcceptedProposal.ServerId,
		}
		result.AcceptedValue = protoToOp(resp.AcceptedValue)
	}
	return result, nil
}

func (p *GRPCPeer) Accept(index int64, proposal paxos.ProposalNum, value *paxos.Operation) (bool, paxos.ProposalNum, error) {
	if p.shouldDrop() {
		return false, paxos.ProposalNum{}, context.DeadlineExceeded
	}
	client, err := p.connect()
	if err != nil {
		return false, paxos.ProposalNum{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Accept(ctx, &pb.AcceptRequest{
		LogIndex: index,
		Proposal: &pb.ProposalNumber{Round: proposal.Round, ServerId: proposal.ServerID},
		Value:    opToProto(value),
	})
	if err != nil {
		return false, paxos.ProposalNum{}, err
	}

	var hp paxos.ProposalNum
	if resp.MinProposal != nil {
		hp = paxos.ProposalNum{Round: resp.MinProposal.Round, ServerID: resp.MinProposal.ServerId}
	}
	return resp.Accepted, hp, nil
}

func (p *GRPCPeer) Learn(index int64, value *paxos.Operation, proposal paxos.ProposalNum) error {
	if p.shouldDrop() {
		return nil // fire-and-forget
	}
	client, err := p.connect()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Learn(ctx, &pb.LearnRequest{
		LogIndex: index,
		Value:    opToProto(value),
		Proposal: &pb.ProposalNumber{Round: proposal.Round, ServerId: proposal.ServerID},
	})
	return err
}

func (p *GRPCPeer) FetchChosen(fromIndex int64) ([]paxos.ChosenEntry, int64, error) {
	client, err := p.connect()
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.FetchChosen(ctx, &pb.FetchChosenRequest{FromIndex: fromIndex})
	if err != nil {
		return nil, 0, err
	}

	entries := make([]paxos.ChosenEntry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		entries = append(entries, paxos.ChosenEntry{
			Index: e.LogIndex,
			Value: *protoToOp(e.Value),
		})
	}
	return entries, resp.MaxChosenIndex, nil
}

// --- proto conversion helpers ---

func opToProto(op *paxos.Operation) *pb.Operation {
	if op == nil {
		return nil
	}
	return &pb.Operation{
		Type:     pb.OpType(op.Type),
		Key:      op.Key,
		Value:    op.Value,
		ClientId: op.ClientID,
		OpId:     op.OpID,
	}
}

func protoToOp(pop *pb.Operation) *paxos.Operation {
	if pop == nil {
		return nil
	}
	return &paxos.Operation{
		Type:     paxos.OpType(pop.Type),
		Key:      pop.Key,
		Value:    pop.Value,
		ClientID: pop.ClientId,
		OpID:     pop.OpId,
	}
}
