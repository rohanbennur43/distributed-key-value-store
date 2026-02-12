package server

import (
	"context"

	"dkv/internal/paxos"
	pb "dkv/proto/kvpb"
)

// PaxosServiceServer implements the inter-node Paxos gRPC service.
type PaxosServiceServer struct {
	pb.UnimplementedPaxosServiceServer
	node *Node
}

func (s *PaxosServiceServer) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	s.node.metrics.PaxosPrepareTotal.Add(1)

	proposal := paxos.ProposalNum{Round: req.Proposal.Round, ServerID: req.Proposal.ServerId}
	result, err := s.node.engine.HandlePrepare(req.LogIndex, proposal)
	if err != nil {
		return nil, err
	}

	resp := &pb.PrepareResponse{
		Promised: result.Promised,
		HighestPromised: &pb.ProposalNumber{
			Round: result.HighestPromised.Round, ServerId: result.HighestPromised.ServerID,
		},
		HasAccepted: result.HasAccepted,
	}
	if result.HasAccepted {
		resp.AcceptedProposal = &pb.ProposalNumber{
			Round: result.AcceptedProposal.Round, ServerId: result.AcceptedProposal.ServerID,
		}
		resp.AcceptedValue = opToProto(result.AcceptedValue)
	}
	return resp, nil
}

func (s *PaxosServiceServer) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	s.node.metrics.PaxosAcceptTotal.Add(1)

	proposal := paxos.ProposalNum{Round: req.Proposal.Round, ServerID: req.Proposal.ServerId}
	value := protoToOp(req.Value)

	accepted, highestPromised, err := s.node.engine.HandleAccept(req.LogIndex, proposal, value)
	if err != nil {
		return nil, err
	}

	return &pb.AcceptResponse{
		Accepted: accepted,
		MinProposal: &pb.ProposalNumber{
			Round: highestPromised.Round, ServerId: highestPromised.ServerID,
		},
	}, nil
}

func (s *PaxosServiceServer) Learn(ctx context.Context, req *pb.LearnRequest) (*pb.LearnResponse, error) {
	s.node.metrics.LearnReceivedTotal.Add(1)

	value := protoToOp(req.Value)
	s.node.engine.HandleLearn(req.LogIndex, value)
	return &pb.LearnResponse{}, nil
}

func (s *PaxosServiceServer) FetchChosen(ctx context.Context, req *pb.FetchChosenRequest) (*pb.FetchChosenResponse, error) {
	entries, maxChosen := s.node.engine.FetchChosen(req.FromIndex)

	pbEntries := make([]*pb.ChosenEntry, 0, len(entries))
	for _, e := range entries {
		pbEntries = append(pbEntries, &pb.ChosenEntry{
			LogIndex: e.Index,
			Value:    opToProto(&e.Value),
		})
	}
	return &pb.FetchChosenResponse{
		Entries:        pbEntries,
		MaxChosenIndex: maxChosen,
	}, nil
}
