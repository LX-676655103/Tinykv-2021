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
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const debug bool = false

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
	id   uint64
	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog
	// log replication progress of each peers
	Prs map[uint64]*Progress
	// this peer's role
	State StateType
	// votes records
	votes      map[uint64]bool
	heartbeats map[uint64]bool
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// create raft
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	var raft = &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress), // follower's nextIndex
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		heartbeats:       make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	// generate peers of region
	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: 0}
	}
	return raft
}

func (r *Raft) sendSnapshot(to uint64) {
	if _, ok := r.Prs[to]; !ok {
		return
	}
	var snapshot pb.Snapshot
	var err error = nil
	if IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot, err = r.RaftLog.storage.Snapshot()
	} else {
		snapshot = *r.RaftLog.pendingSnapshot
	}
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//println(r.id, "sendAppend to", to)

	if _, ok := r.Prs[to]; !ok {
		return false
	}
	prevIndex := r.Prs[to].Next - 1
	firstIndex := r.RaftLog.FirstIndex()

	//println(r.id, "send prevIndex:", prevIndex, "firstIndex:", r.RaftLog.FirstIndex())

	prevLogTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil || prevIndex < firstIndex-1 {
		r.sendSnapshot(to)
		return true
	}
	var entries []*pb.Entry
	n := r.RaftLog.LastIndex() + 1
	for i := prevIndex + 1; i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)

	if debug {
		fmt.Printf("%d send raft message %s from %d to %d\n", r.id, pb.MessageType_MsgAppend, r.id, to)
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return
	}
	if r.leadTransferee != None {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: r.leadTransferee})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// Commit:  r.RaftLog.committed,
		Commit: util.RaftInvalidIndex,
	}
	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends an vote RPC to the given peer.
// Returns true if a message was sent.
func (r *Raft) sendRequestVote(to uint64) bool {
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)

	//println("send -- m.From:", r.id, "m.To:", to,"m.Term:", r.Term, "m.Index:", lastIndex, "m.LogTerm:", lastLogTerm)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
		Entries: nil,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//println(r.id, "TICK", r.electionTimeout, r.heartbeatTimeout)
	switch r.State {
	case StateFollower:
		if debug {
			println(r.id, "term:", r.Term, "StateFollower", "r.electionElapsed:", r.electionElapsed, len(r.Prs))
		}

		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if debug {
			println(r.id, "term:", r.Term, "StateCandidate", "r.electionElapsed:", r.electionElapsed, len(r.Prs))
		}
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		if debug {
			println(r.id, "term:", r.Term, "StateLeader", "r.heartbeatElapsed:", r.heartbeatElapsed, len(r.Prs))
		}
		r.electionElapsed++
		num := len(r.heartbeats)
		length := len(r.Prs) / 2
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			r.heartbeats = make(map[uint64]bool)
			r.heartbeats[r.id] = true
			if num <= length {
				r.campaign()
			}
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.leadTransferee = None
	r.Lead = r.id
	r.heartbeats = make(map[uint64]bool)
	r.heartbeats[r.id] = true
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next += 1

	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// for 3A when node has been removed r.Prs is nil or like test delete itself

	// fmt.Printf("%d handle raft message %s from %d to %d\n", r.id, m.MsgType, m.From, m.To)
	if debug {
		fmt.Printf("%d handle raft message %s from %d to %d\n", r.id, m.MsgType, m.From, m.To)
		if _, ok := r.Prs[r.id]; !ok {
			fmt.Printf("%d not exist in r.Prs\n", r.id)
		}
	}
	switch r.State {
	case StateFollower:
		err := r.FollowerStep(m)
		if err != nil {
			return err
		}
	case StateCandidate:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		err := r.CandidateStep(m)
		if err != nil {
			return err
		}
	case StateLeader:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		err := r.LeaderStep(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: // ?
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader: //leader only
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: //dropped
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		length := len(r.Prs) / 2
		grant := 0
		denials := 0
		for _, status := range r.votes {
			if status {
				grant++
			} else {
				denials++
			}
		}
		if grant > length {
			r.becomeLeader()
		} else if denials > length {
			r.becomeFollower(r.Term, m.From)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.handleMsgPropose(m)
		}
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat: //error
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.heartbeats[m.From] = true
		if debug {
			println(r.id, "MsgHeartbeatResponse, m.Commit:", m.Commit,
				"r.RaftLog.committed:", r.RaftLog.committed, "m.Reject:", m.Reject)
		}
		if m.Reject || m.Commit < r.RaftLog.committed {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

// Handle the campaign to start a new election
// Once 'campaign' method is called, the node becomes candidate
// and sends `MessageType_MsgRequestVote` to peers in cluster to request votes.
func (r *Raft) campaign() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	// appendEntry
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		// 3A: conf change confirm
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				// r.msgs = append(r.msgs, m)
				return
			}
			r.PendingConfIndex = lastIndex + uint64(i) + 1
		}
	}

	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	//println("length", len(r.RaftLog.entries))
	//println("r.RaftLog.FirstIndex()", r.RaftLog.FirstIndex())
	//println("r.RaftLog.LastIndex()", r.RaftLog.LastIndex(), "\n")
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// bcastAppend
	for peer := range r.Prs {
		if peer != r.id {
			//println(r.id, "sendAppend to", peer)
			r.sendAppend(peer)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}
func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if len(r.Prs) == 0 && r.RaftLog.LastIndex() < meta.RaftInitLogIndex {
		r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, meta.RaftInitLogIndex+1)
		return
	}
	// flash the state
	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}

	// Reply reject if doesn’t contain entry at prevLogIndex term matches prevLogTerm
	lastIndex := r.RaftLog.LastIndex()
	if m.Index <= lastIndex {
		r.RaftLog.FirstIndex()
		LogTerm, err := r.RaftLog.Term(m.Index)
		//println("m.LogTerm:", m.LogTerm, "LogTerm:", LogTerm)
		if err != nil && err == ErrCompacted {
			r.Vote = None
			r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
			return
		}

		if m.LogTerm == LogTerm {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			for i, entry := range m.Entries {
				var Term uint64
				entry.Index = m.Index + uint64(i) + 1
				if entry.Index <= lastIndex {
					Term, _ = r.RaftLog.Term(entry.Index)
					if Term != entry.Term {
						firstIndex := r.RaftLog.FirstIndex()
						if len(r.RaftLog.entries) != 0 && int64(m.Index-firstIndex+1) >= 0 {
							r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-firstIndex+1]
						}
						lastIndex = r.RaftLog.LastIndex()
						r.RaftLog.entries = append(r.RaftLog.entries, *entry)
						r.RaftLog.stabled = m.Index
					}
				} else {
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
			}

			// send true AppendResponse
			r.Vote = None
			r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
			// set committed
			if m.Commit > r.RaftLog.committed {
				committed := min(m.Commit, m.Index+uint64(len(m.Entries)))
				r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
			}
			return
		}
	}
	// return reject AppendResponse
	index := max(lastIndex+1, meta.RaftInitLogIndex+1)
	r.sendAppendResponse(m.From, true, index)
	return
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	if m.Reject && m.Index < meta.RaftInitLogIndex {
		r.sendSnapshot(m.From)
		return
	}

	if m.Reject && m.Term <= r.Term {
		next := r.Prs[m.From].Next - 1
		r.Prs[m.From].Next = min(m.Index, next)
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// check if can TransferLeader
	if m.From == r.leadTransferee {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From})
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	Match := match[(len(r.Prs)-1)/2]
	matchTerm, _ := r.RaftLog.Term(Match)
	// println("match:", Match, "r.RaftLog.committed:", r.RaftLog.committed)
	// Raft 永远不会通过计算副本数目的方式去提交一个之前任期内的日志条目
	if Match > r.RaftLog.committed && matchTerm == r.Term {
		r.RaftLog.committed = Match
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
	return
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// Reply reject if term < currentTerm.
	// println("handle -- m.From:", m.From, "m.To:", m.To,"m.Term:", m.Term, "m.Index:", m.Index, "m.LogTerm:", m.LogTerm)
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
	}
	// If votedFor is null or candidateId
	if r.Vote == None || r.Vote == m.From {
		// when sender's last term is greater than receiver's term
		// or sender's last term is equal to receiver's term
		// but sender's last index is greater than or equal to follower's.
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm > lastTerm ||
			(m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Commit:  r.RaftLog.committed,
			Index:   r.RaftLog.stabled,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}

	r.electionElapsed -= r.heartbeatTimeout
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.stabled,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata

	//println("\n\n\n", meta.Index, r.RaftLog.committed)
	//println(m.Term, r.Term)

	if meta.Index <= r.RaftLog.committed ||
		m.Term < r.Term {
		return
	}
	r.Lead = m.From
	r.Term = m.Term
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	if len(r.RaftLog.entries) > 0 {
		if meta.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else if meta.Index >= r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.entries[meta.Index-
				r.RaftLog.FirstIndex():]
		}
	}
	if len(r.RaftLog.entries) == 0 {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      meta.Term,
			Index:     meta.Index,
		})
	}
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  meta.Index + 1,
		}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
}

// handle TransferLeader, leader only
func (r *Raft) handleTransferLeader(m pb.Message) {
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From

	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		r.sendAppend(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if r.id == id {
		r.Prs = make(map[uint64]*Progress)
		return
	}
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
	}

	// tp pass TestCommitAfterRemoveNode3A test
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	Match := match[(len(r.Prs)-1)/2]
	matchTerm, _ := r.RaftLog.Term(Match)
	if Match > r.RaftLog.committed && matchTerm == r.Term {
		r.RaftLog.committed = Match
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
