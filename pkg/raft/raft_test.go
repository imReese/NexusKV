package raft

import (
	"context"
	"testing"
	"time"
)

func TestRaftLeaderElectionSingleNode(t *testing.T) {
	node, err := NewNode(NodeConfig{
		ID:               "node-1",
		ElectionTimeout:  50 * time.Millisecond,
		HeartbeatTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create raft node: %v", err)
	}

	// Wait for election to transition node to leader
	time.Sleep(120 * time.Millisecond)

	if node.Role() != RoleLeader {
		t.Fatalf("expected node to become Leader, got %s", node.Role())
	}

	if node.LeaderID() != "node-1" {
		t.Fatalf("expected leaderID to be node-1, got %s", node.LeaderID())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}

func TestRaftRequestVoteAndAppendEntries(t *testing.T) {
	node, err := NewNode(NodeConfig{
		ID:               "node-2",
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create raft node: %v", err)
	}

	voteArgs := &RequestVoteArgs{
		Term:        1,
		CandidateID: "candidate-1",
	}
	voteReply := &RequestVoteReply{}
	node.RequestVote(voteArgs, voteReply)

	if !voteReply.VoteGranted {
		t.Fatalf("expected vote to be granted")
	}

	appendArgs := &AppendEntriesArgs{
		Term:     1,
		LeaderID: "candidate-1",
	}
	appendReply := &AppendEntriesReply{}
	node.AppendEntries(appendArgs, appendReply)

	if !appendReply.Success {
		t.Fatalf("expected append entries success")
	}

	if node.LeaderID() != "candidate-1" {
		t.Fatalf("expected leaderID candidate-1, got %s", node.LeaderID())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = node.Shutdown(ctx)
}
