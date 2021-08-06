// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	baseTimeout int

	voteNumber int

	rejectNumber int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	for _, id := range c.peers {
		prs[id] = &Progress{}
		votes[id] = false
	}
	hardState, _, _ := c.Storage.InitialState()
	return &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		electionTimeout:  c.ElectionTick,
		baseTimeout:      c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Prs:              prs,
		votes:            votes,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	offset := uint64(len(r.RaftLog.entries)) - 1 - (r.RaftLog.LastIndex() - r.Prs[to].Next)
	ents := r.RaftLog.entries[offset:]
	var entries []*pb.Entry
	for i := 0; i < len(ents); i++ {
		entries = append(entries, &ents[i])
	}

	//index := r.RaftLog.entries[offset].Index - 1
	index := r.Prs[to].Next - 1

	logTerm, _ := r.RaftLog.Term(index)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).\
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) requestElection() {
	r.becomeCandidate()
	if len(r.votes) == 1 {
		r.becomeLeader()
		return
	}
	r.votes[r.id] = true
	r.voteNumber += 1
	index := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(index)
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   index,
			LogTerm: logTerm,
		})
	}
	r.electionTimeout = r.baseTimeout + rand.Intn(r.baseTimeout)
	r.electionElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			r.heartbeatElapsed = 0
		}
	} else {
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.requestElection()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.voteNumber, r.rejectNumber = 0, 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.State = StateCandidate
	r.voteNumber, r.rejectNumber = 0, 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	}
	r.voteNumber, r.rejectNumber = 0, 0
	/*
		ent := pb.Entry{
			Data:  nil,
			Term:  r.Term,
			Index: r.RaftLog.LastIndex() + 1,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, ent)

		if len(r.Prs) == 1 {
			r.RaftLog.committed = ent.Index
		}

		index := ent.Index - 1
		logTerm, _ := r.RaftLog.Term(index)
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppend,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				LogTerm: logTerm,
				Index:   index,
				Entries: []*pb.Entry{&ent},
				Commit:  r.RaftLog.committed,
			})
		}
	*/
	r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(lastIndex)
	if lastIndex != m.Index || term != m.Term {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
	r.Prs[r.id].Match += uint64(len(m.Entries))
	r.Prs[r.id].Next += uint64(len(m.Entries))

	if len(r.Prs) == 1 {
		r.RaftLog.committed += 1
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// resend log if rejected
	if m.Reject && m.Index == r.Prs[m.From].Next-1 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	// Never commit log from previous term and handle response once
	term, _ := r.RaftLog.Term(m.Index)
	if term != r.Term || m.Index < r.Prs[m.From].Next {
		return
	}
	// update committed index and notice peers if needed
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	if m.Index > r.RaftLog.committed {
		for r.RaftLog.committed < r.RaftLog.LastIndex() {
			nextCommit := r.RaftLog.committed + 1
			commitNum := 1
			for id, process := range r.Prs {
				if id == r.id {
					continue
				}
				if process.Match >= nextCommit {
					commitNum += 1
				}
			}
			if commitNum > len(r.Prs)/2 {
				r.RaftLog.committed += 1
				for id := range r.Prs {
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
			} else {
				break
			}
		}
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if !m.Reject {
		r.voteNumber += 1
		if r.voteNumber > len(r.votes)/2 {
			r.becomeLeader()
			for id := range r.votes {
				r.votes[id] = false
			}
		}
	} else {
		r.rejectNumber += 1
		if r.rejectNumber > len(r.votes)/2 {
			r.becomeFollower(r.Term, r.Lead)
			for id := range r.votes {
				r.votes[id] = false
			}
		}
	}

}

func (r *Raft) handleVote(m pb.Message) {
	reject := false
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if m.Term > r.Term {
		r.Term = m.Term
		r.Vote = None
		r.State = StateFollower
	}
	if r.Vote != 0 && (r.Vote != m.From) || m.Term < r.Term ||
		m.LogTerm < lastTerm || m.LogTerm == lastTerm && m.Index < lastIndex {
		reject = true
	} else {
		r.Vote = m.From
		r.Term = m.Term
		r.State = StateFollower
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Reject:  reject,
		Term:    r.Term,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	response.Term = r.Term

	if m.Term < r.Term {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}

	r.Lead = m.From

	term, err := r.RaftLog.Term(m.Index)
	if term != m.LogTerm || err != nil {
		response.Reject = true
		response.Index = m.Index
		r.msgs = append(r.msgs, response)
		return
	}

	if len(m.Entries) == 0 {
		r.RaftLog.committed = min(m.Commit, m.Index)
		response.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, response)
		return
	}

	first, _ := r.RaftLog.storage.FirstIndex()
	last := m.Entries[len(m.Entries)-1].Index

	if last < first {
		r.msgs = append(r.msgs, response)
		return
	}

	if first > m.Entries[0].Index {
		m.Entries = m.Entries[first-m.Entries[0].Index:]
	}

	offset := m.Entries[0].Index - first

	for _, entry := range m.Entries {
		if offset == uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if entry.Term != r.RaftLog.entries[offset].Term ||
			entry.Index != r.RaftLog.entries[offset].Index {
			r.RaftLog.entries = append([]pb.Entry{}, r.RaftLog.entries[:offset]...)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			if entry.Index <= r.RaftLog.stabled {
				r.RaftLog.stabled = entry.Index - 1
			}
		}
		offset += 1
	}

	response.Index = r.RaftLog.LastIndex()
	r.RaftLog.committed = min(m.Commit, response.Index)
	r.msgs = append(r.msgs, response)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term < r.Term {
		response.Term = r.Term
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}
	r.State = StateFollower
	r.electionElapsed = 0
	r.Term = m.Term
	r.Lead = m.From

	response.Index = r.RaftLog.LastIndex()
	response.LogTerm, _ = r.RaftLog.Term(response.Index)
	r.msgs = append(r.msgs, response)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
