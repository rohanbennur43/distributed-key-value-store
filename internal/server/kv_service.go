package server

import (
	"context"
	"fmt"
	"log"

	"dkv/internal/paxos"
	pb "dkv/proto/kvpb"
)

// KVServiceServer implements the client-facing KV gRPC service.
type KVServiceServer struct {
	pb.UnimplementedKVServiceServer
	node *Node
}

func (s *KVServiceServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Linearizable read: propose a NOOP barrier through Paxos.
	noop := &paxos.Operation{
		Type:     paxos.OpNoop,
		ClientID: req.ClientId,
		OpID:     req.OpId,
	}

	_, err := s.node.engine.Propose(noop)
	if err != nil {
		return &pb.GetResponse{Error: fmt.Sprintf("read barrier failed: %v", err)}, nil
	}

	// Apply all pending chosen entries up to the current max.
	s.node.applyUpTo(s.node.engine.MaxChosenIndex())

	value, found := s.node.store.Get(req.Key)
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *KVServiceServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Check dedup.
	if dup, entry := s.node.store.CheckDuplicate(req.ClientId, req.OpId); dup {
		log.Printf("[node %d] duplicate PUT suppressed: client=%s op=%d", s.node.id, req.ClientId, req.OpId)
		s.node.metrics.DuplicateSuppressedTotal.Add(1)
		return &pb.PutResponse{Error: entry.Result}, nil
	}

	op := &paxos.Operation{
		Type:     paxos.OpPut,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientId,
		OpID:     req.OpId,
	}

	s.node.metrics.PaxosProposeTotal.Add(1)
	index, err := s.node.engine.Propose(op)
	if err != nil {
		s.node.metrics.PaxosProposeFailTotal.Add(1)
		return &pb.PutResponse{Error: fmt.Sprintf("propose failed: %v", err)}, nil
	}

	// Apply all up to (and including) our index.
	s.node.applyUpTo(index)

	return &pb.PutResponse{}, nil
}

func (s *KVServiceServer) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	if dup, entry := s.node.store.CheckDuplicate(req.ClientId, req.OpId); dup {
		log.Printf("[node %d] duplicate APPEND suppressed: client=%s op=%d", s.node.id, req.ClientId, req.OpId)
		s.node.metrics.DuplicateSuppressedTotal.Add(1)
		return &pb.AppendResponse{Error: entry.Result}, nil
	}

	op := &paxos.Operation{
		Type:     paxos.OpAppend,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientId,
		OpID:     req.OpId,
	}

	s.node.metrics.PaxosProposeTotal.Add(1)
	index, err := s.node.engine.Propose(op)
	if err != nil {
		s.node.metrics.PaxosProposeFailTotal.Add(1)
		return &pb.AppendResponse{Error: fmt.Sprintf("propose failed: %v", err)}, nil
	}

	s.node.applyUpTo(index)
	return &pb.AppendResponse{}, nil
}

func (s *KVServiceServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if dup, entry := s.node.store.CheckDuplicate(req.ClientId, req.OpId); dup {
		log.Printf("[node %d] duplicate DELETE suppressed: client=%s op=%d", s.node.id, req.ClientId, req.OpId)
		s.node.metrics.DuplicateSuppressedTotal.Add(1)
		return &pb.DeleteResponse{Found: entry.Found, Error: entry.Result}, nil
	}

	op := &paxos.Operation{
		Type:     paxos.OpDelete,
		Key:      req.Key,
		ClientID: req.ClientId,
		OpID:     req.OpId,
	}

	s.node.metrics.PaxosProposeTotal.Add(1)
	index, err := s.node.engine.Propose(op)
	if err != nil {
		s.node.metrics.PaxosProposeFailTotal.Add(1)
		return &pb.DeleteResponse{Error: fmt.Sprintf("propose failed: %v", err)}, nil
	}

	s.node.applyUpTo(index)

	// Re-check: was it applied or was it a duplicate at apply time?
	_, entry := s.node.store.CheckDuplicate(req.ClientId, req.OpId)
	if entry != nil {
		return &pb.DeleteResponse{Found: entry.Found}, nil
	}
	return &pb.DeleteResponse{}, nil
}
