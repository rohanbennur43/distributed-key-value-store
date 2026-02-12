package server

import (
	"context"

	"dkv/internal/paxos"
	pb "dkv/proto/kvpb"
)

// AdminServiceServer implements the admin gRPC service.
type AdminServiceServer struct {
	pb.UnimplementedAdminServiceServer
	node *Node
}

func (s *AdminServiceServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy:          true,
		ServerId:         s.node.id,
		ChosenIndex:      s.node.engine.MaxChosenIndex(),
		LastAppliedIndex: s.node.store.LastAppliedIndex(),
	}, nil
}

func (s *AdminServiceServer) Metrics(ctx context.Context, req *pb.MetricsRequest) (*pb.MetricsResponse, error) {
	return &pb.MetricsResponse{
		Counters: s.node.metrics.Snapshot(),
	}, nil
}

func (s *AdminServiceServer) SetDropRules(ctx context.Context, req *pb.SetDropRulesRequest) (*pb.SetDropRulesResponse, error) {
	// Clear existing rules.
	for _, p := range s.node.peers {
		p.SetDropRate(0)
	}
	// Apply new rules.
	for _, rule := range req.Rules {
		for _, p := range s.node.peers {
			if p.ServerID() == rule.TargetServerId {
				p.SetDropRate(rule.DropRate)
			}
		}
	}
	return &pb.SetDropRulesResponse{}, nil
}

func (s *AdminServiceServer) FetchLog(ctx context.Context, req *pb.FetchLogRequest) (*pb.FetchLogResponse, error) {
	from := req.FromIndex
	to := req.ToIndex
	if to == 0 {
		to = s.node.engine.MaxChosenIndex()
	}

	var entries []*pb.LogEntry
	for i := from; i <= to; i++ {
		op := s.node.engine.GetChosenValue(i)
		entry := &pb.LogEntry{
			Index:  i,
			Chosen: op != nil,
		}
		if op != nil {
			entry.Value = &pb.Operation{
				Type:     pb.OpType(op.Type),
				Key:      op.Key,
				Value:    op.Value,
				ClientId: op.ClientID,
				OpId:     op.OpID,
			}
		}
		entries = append(entries, entry)
	}
	return &pb.FetchLogResponse{Entries: entries}, nil
}

// opToProtoForLog converts internal Operation to proto (used in admin).
func opToProtoForLog(op *paxos.Operation) *pb.Operation {
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
