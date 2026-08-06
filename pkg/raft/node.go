package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type NodeRole int

const (
	RoleFollower NodeRole = iota
	RoleCandidate
	RoleLeader
)

func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type EtcdConfig struct {
	Endpoints []string
}

type Transport struct{}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      [][]byte
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

type NodeConfig struct {
	ID               string
	Peers            []string
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	Storage          any
	WAL              any
	Transport        *Transport
	EtcdConfig       EtcdConfig
	Logger           *zap.Logger
}

type Node struct {
	mu         sync.RWMutex
	id         string
	peers      []string
	role       NodeRole
	term       uint64
	votedFor   string
	leaderID   string
	config     Config
	logger     *zap.Logger
	stopChan   chan struct{}
	resetTimer chan struct{}
}

func NewGRPCTransport() *Transport {
	return &Transport{}
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.ID == "" {
		cfg.ID = "node-0"
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 150 * time.Millisecond
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	}

	n := &Node{
		id:         cfg.ID,
		peers:      cfg.Peers,
		role:       RoleFollower,
		term:       0,
		votedFor:   "",
		leaderID:   "",
		logger:     cfg.Logger,
		stopChan:   make(chan struct{}),
		resetTimer: make(chan struct{}, 1),
	}

	go n.runLoop(cfg.ElectionTimeout, cfg.HeartbeatTimeout)
	return n, nil
}

func (n *Node) Role() NodeRole {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.term
	reply.VoteGranted = false

	if args.Term < n.term {
		return
	}

	if args.Term > n.term {
		n.term = args.Term
		n.role = RoleFollower
		n.votedFor = ""
		n.leaderID = ""
	}

	if n.votedFor == "" || n.votedFor == args.CandidateID {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.signalResetTimerUnlocked()
	}
}

func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.term
	reply.Success = false

	if args.Term < n.term {
		return
	}

	if args.Term > n.term || n.role != RoleFollower {
		n.term = args.Term
		n.role = RoleFollower
		n.votedFor = ""
	}

	n.leaderID = args.LeaderID
	reply.Success = true
	n.signalResetTimerUnlocked()
}

func (n *Node) runLoop(baseElectionTimeout, heartbeatTimeout time.Duration) {
	for {
		n.mu.RLock()
		role := n.role
		n.mu.RUnlock()

		select {
		case <-n.stopChan:
			return
		default:
		}

		switch role {
		case RoleFollower:
			n.runFollower(baseElectionTimeout)
		case RoleCandidate:
			n.runCandidate(baseElectionTimeout)
		case RoleLeader:
			n.runLeader(heartbeatTimeout)
		}
	}
}

func (n *Node) runFollower(baseTimeout time.Duration) {
	timeout := randomTimeout(baseTimeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-n.stopChan:
		return
	case <-n.resetTimer:
		return
	case <-timer.C:
		n.mu.Lock()
		n.role = RoleCandidate
		n.mu.Unlock()
	}
}

func (n *Node) runCandidate(baseTimeout time.Duration) {
	n.mu.Lock()
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	peers := n.peers
	n.mu.Unlock()

	votesReceived := 1
	votesNeeded := (len(peers)+1)/2 + 1

	if votesReceived >= votesNeeded {
		n.mu.Lock()
		n.role = RoleLeader
		n.leaderID = n.id
		n.mu.Unlock()
		return
	}

	timeout := randomTimeout(baseTimeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-n.stopChan:
		return
	case <-n.resetTimer:
		n.mu.Lock()
		n.role = RoleFollower
		n.mu.Unlock()
		return
	case <-timer.C:
		return
	}
}

func (n *Node) runLeader(heartbeatTimeout time.Duration) {
	ticker := time.NewTicker(heartbeatTimeout)
	defer ticker.Stop()

	select {
	case <-n.stopChan:
		return
	case <-ticker.C:
		n.mu.RLock()
		_ = n.term
		n.mu.RUnlock()
	}
}

func (n *Node) signalResetTimerUnlocked() {
	select {
	case n.resetTimer <- struct{}{}:
	default:
	}
}

func (n *Node) ApplyConfig(cfg Config) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.config = cfg
	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	select {
	case <-n.stopChan:
	default:
		close(n.stopChan)
	}
	return nil
}

func RegisterRaftServiceServer(server grpc.ServiceRegistrar, node *Node) {
}

func randomTimeout(base time.Duration) time.Duration {
	extra := time.Duration(rand.Int63n(int64(base)))
	return base + extra
}
