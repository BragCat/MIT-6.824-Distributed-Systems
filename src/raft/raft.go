package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
	CommandTerm 	int
}

type Entry struct {
	Term 	int
	Command interface{}
}

type StateType string

const (
	maxElectionTimeout 		= 450
	minElectionTimeout 		= 300
	heartBeatTime 			= 105

	leader 		StateType 	= "leader"
	candidate 	StateType 	= "candidate"
	follower 	StateType 	= "follower"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*Persister          // Object to hold this peer's persisted state
	me        			int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Paper defined persistent state
	currentTerm 		int
	votedFor 			int
	log 				[]Entry
	lastIncludedIndex 	int
	lastIncludedTerm 	int

	// Paper defined volatile state
	commitIndex 		int
	lastApplied 		int

	// Paper defined volatile state on leaders
	nextIndex 			[]int
	matchIndex 			[]int

	// My volatile state
	majorityNum 		int
	voteCnt 			int
	applyCh 			chan ApplyMsg
	state 				StateType
	electionTimer 		*time.Timer
	heartBeatTimer 		*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	log := make([]Entry, 0)
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil {
		panic("Read persisted data error.")
	} else {
		rf.currentTerm, rf.votedFor = currentTerm, votedFor
		rf.log = log
		rf.lastIncludedIndex, rf.lastIncludedTerm = lastIncludedIndex, lastIncludedTerm
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId 	int
	LastLogTerm 	int
	LastLogIndex 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Raft %v, Term %v]: receive RequestVoteArgs %v from Raft %v, currently vote for %v.",
		rf.me, rf.currentTerm, *args, args.CandidateId, rf.votedFor)

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else {
		if args.Term > rf.currentTerm {
			rf.setToFollower(args.Term)
		}
		valid := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		lastLogIndex := rf.lastIncludedIndex
		lastLogTerm := rf.lastIncludedTerm
		if len(rf.log) > 0 {
			lastLogIndex = rf.offset2index(len(rf.log) - 1)
			lastLogTerm = rf.log[len(rf.log) - 1].Term
		}
		upToDate := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		reply.Term, reply.VoteGranted = rf.currentTerm, valid && upToDate

		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
	}
	DPrintf("[Raft %v, Term %v]: reply RequestVoteReply %v to Raft %v.",
		rf.me, rf.currentTerm, *reply, args.CandidateId)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(peerId int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[Raft %v, Term %v]: send RequestVoteArgs %v to Raft %v.",
		args.CandidateId, args.Term, *args, peerId)
	ok := rf.peers[peerId].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) processRequestVoteReply(peerId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Raft %v, Term %v]: receive RequestVoteReply %v from Raft %v.",
		rf.me, rf.currentTerm, *reply, peerId)

	// Make sure this reply is to this current term
	if args.Term != rf.currentTerm || rf.state != candidate {
		return
	}

	// Abandon stale rpcs
	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.setToFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCnt++
		if rf.voteCnt >= rf.majorityNum {
			DPrintf("[Raft %v, Term %v]: become leader.", rf.me, rf.currentTerm)
			rf.state = leader

			nextIndex := rf.offset2index(len(rf.log))
			for peerId := range rf.nextIndex {
				rf.nextIndex[peerId] = nextIndex
				rf.matchIndex[peerId] = -1
			}

			rf.electionTimer.Stop()
			rf.heartBeatTimer.Reset(0)
		}
	}
}


type AppendEntriesArgs struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries 		[]Entry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 		int
	Success 	bool
	NextIndex 	int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Raft %v, Term %v]: receive AppendEntriesArgs %v from Raft %v.",
		rf.me, rf.currentTerm, *args, args.LeaderId)

	if args.Term < rf.currentTerm {
		// The leader should be set to follower
		reply.Term, reply.Success = rf.currentTerm, false
	} else {
		if args.Term > rf.currentTerm {
			rf.setToFollower(args.Term)
		} else if rf.state == leader {
			panic("multiple leaders!")
		} else if rf.state == candidate {
			rf.setToFollower(args.Term)
		} else {
			rf.electionTimer.Reset(getElectionTimeout())
		}

		if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.NextIndex = rf.lastIncludedIndex + 1
			return
		}

		if args.PrevLogIndex > rf.offset2index(len(rf.log) - 1) {
			reply.Term, reply.Success = rf.currentTerm, false
			reply.NextIndex = len(rf.log)
			return
		}

		prevLogOffset := rf.index2offset(args.PrevLogIndex)
		if prevLogOffset >= 0 && rf.log[prevLogOffset].Term != args.PrevLogTerm {
			reply.Term, reply.Success = rf.currentTerm, false
			conflictTerm := rf.log[prevLogOffset].Term
			for offset := 0; offset < len(rf.log); offset++ {
				if rf.log[offset].Term == conflictTerm {
					reply.NextIndex = rf.offset2index(offset)
					break
				}
			}
			return
		}

		// Append new entries
		DPrintf("[Raft %v, Term %v]: append log start, rf.log is %v",
			rf.me, rf.currentTerm, rf.log)
		rfOffset, argsOffset := prevLogOffset + 1, 0
		for ; rfOffset < len(rf.log) && argsOffset < len(args.Entries); {
			if rf.log[rfOffset].Term != args.Entries[argsOffset].Term {
				rf.log = append([]Entry{}, rf.log[ : rfOffset]...)
				rf.log = append(rf.log, args.Entries[argsOffset : ]...)
				argsOffset = len(args.Entries)
				break
			}
			rfOffset, argsOffset = rfOffset + 1, argsOffset + 1
		}
		if argsOffset != len(args.Entries) {
			rf.log = append(rf.log, args.Entries[argsOffset : ]...)
		}
		DPrintf("[Raft %v, Term %v]: append log finished, rf.log is %v",
			rf.me, rf.currentTerm, rf.log)

		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, rf.offset2index(len(rf.log) - 1))
			for index := rf.commitIndex + 1; index <= newCommitIndex; index++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: 	true,
					Command: 		rf.log[rf.index2offset(index)].Command,
					CommandIndex: 	index + 1,
					CommandTerm: 	rf.log[rf.index2offset(index)].Term,
				}
				DPrintf("[Raft %v, Term %v]: commit {%v} at index {%v}.",
					rf.me, rf.currentTerm, rf.log[rf.index2offset(index)].Command, index)
			}
			rf.commitIndex = newCommitIndex
		}
		reply.Term, reply.Success = rf.currentTerm, true
		reply.NextIndex = args.PrevLogIndex + 1 + len(args.Entries)
		rf.persist()
	}

	DPrintf("[Raft %v, Term %v]: reply AppendEntriesReply %v, log length = %v, commitIndex = %v.",
		rf.me, rf.currentTerm, *reply, len(rf.log), rf.commitIndex)
}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[Raft %v, Term %v]: send AppendEntriesArgs %v to Raft %v.",
		args.LeaderId, args.Term, *args, peerId)
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) processAppendEntriesReply(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Raft %v, Term %v]: receive AppendEntriesReply %v from Raft %v.",
		rf.me, rf.currentTerm, *reply, peerId)

	// Abandon stale rpcs
	if args.Term < rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.setToFollower(reply.Term)
	}

	if rf.state != leader {
		return
	}

	if reply.Success {
		if reply.NextIndex > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = reply.NextIndex
			rf.matchIndex[peerId] = reply.NextIndex - 1
			rf.updateCommitIndex(reply.NextIndex - 1)
		}
	} else {
		rf.nextIndex[peerId] = reply.NextIndex
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Raft %v, Term %v]: LastIncludedIndex is %v, log length is %v, receive InstallSnapshotArgs %v from Raft %v.",
		rf.me, rf.currentTerm, rf.lastIncludedIndex, len(rf.log), *args, args.LeaderId)

	if args.Term >= rf.currentTerm {
		if rf.state != follower {
			rf.setToFollower(args.Term)
		} else if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
		}

		if args.LastIncludedIndex > rf.lastIncludedIndex {
			lastIncludedOffset := rf.index2offset(args.LastIncludedIndex)
			if lastIncludedOffset >= len(rf.log) - 1 {
				rf.log = make([]Entry, 0)
			} else {
				if rf.log[lastIncludedOffset].Term == rf.lastIncludedTerm {
					rf.log = append([]Entry{}, rf.log[lastIncludedOffset + 1 : ]...)
				} else {
					rf.log = make([]Entry, 0)
				}
			}
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)

			rf.persist()
			rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)

			rf.applyCh <- ApplyMsg{
				CommandValid: false,
				Command:      args.Data,
				CommandIndex: 0,
				CommandTerm:  0,
			}
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(peerId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("[Raft %v, Term %v]: send InstallSnapshotArgs %v to Raft %v",
		args.LeaderId, args.Term, &args, peerId)
	ok := rf.peers[peerId].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) processInstallSnapshot(peerId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.setToFollower(reply.Term)
	}

	rf.nextIndex[peerId] = max(rf.nextIndex[peerId], args.LastIncludedIndex + 1)
	rf.matchIndex[peerId] = max(rf.matchIndex[peerId], args.LastIncludedIndex)
}

func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	lastIncludedOffset := rf.index2offset(lastIncludedIndex)

	if lastIncludedOffset < 0 || lastIncludedOffset >= len(rf.log) {
		panic("offset is out of range when taking snapshot!")
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.log[lastIncludedOffset].Term
	rf.log = append([]Entry{}, rf.log[lastIncludedOffset + 1 : ]...)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

func (rf *Raft) replayFromSnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	data := rf.persister.ReadSnapshot()
	if len(data) > 0 {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      data,
			CommandIndex: 0,
			CommandTerm:  0,
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		term, isLeader = rf.currentTerm, false
	} else {
		DPrintf("[Raft %v, Term %v]: add command %v.", rf.me, rf.currentTerm, command)

		entry := Entry{
			Term: rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.heartBeatTimer.Reset(0)
		rf.persist()

		index = rf.offset2index(len(rf.log))
		term = rf.currentTerm
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.majorityNum = (len(peers) + 1) / 2
	rf.voteCnt = 0
	rf.applyCh = applyCh

	rf.state = follower
	rf.electionTimer = time.AfterFunc(getElectionTimeout(), rf.startElection)
	rf.heartBeatTimer = time.AfterFunc(getHeartBeatTime(), rf.sendHeartBeat)
	rf.heartBeatTimer.Stop()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex
	rf.replayFromSnapshot()

	return rf
}


// my added code

func (rf *Raft) setToFollower(term int) {
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.electionTimer.Reset(getElectionTimeout())
	rf.heartBeatTimer.Stop()
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	DPrintf("[Raft %v, Term %v]: start election.", rf.me, rf.currentTerm)

	if rf.state == leader {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.state = candidate
	rf.electionTimer.Reset(getElectionTimeout())

	lastLogIndex, lastLogTerm := rf.lastIncludedIndex, rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogIndex = rf.offset2index(len(rf.log) - 1)
		lastLogTerm = rf.log[len(rf.log) - 1].Term
	}

	args := &RequestVoteArgs{
		Term: 			rf.currentTerm,
		CandidateId: 	rf.me,
		LastLogTerm: 	lastLogTerm,
		LastLogIndex: 	lastLogIndex,
	}

	rf.persist()
	rf.mu.Unlock()

	for peerId := range rf.peers {
		if peerId != rf.me {
			rf.sendRequestVoteTo(peerId, args)
		}
	}
}

func (rf *Raft) sendRequestVoteTo(peerId int, args *RequestVoteArgs) {
	rf.mu.Lock()
	if rf.state != candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	go func(peerId int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peerId, args, reply)
		if ok {
			rf.processRequestVoteReply(peerId, args, reply)
		}
	}(peerId, args)
}

func (rf *Raft) sendHeartBeat() {
	for peerId := range rf.peers {
		if peerId != rf.me {
			rf.sendHeartBeatTo(peerId)
		}
	}

	rf.mu.Lock()
	if rf.state == leader {
		rf.heartBeatTimer.Reset(getHeartBeatTime())
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeatTo(peerId int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
		// Send InstallSnapshotArgs
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}

		rf.mu.Unlock()

		go func(peerId int, args *InstallSnapshotArgs) {
			reply := &InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(peerId, args, reply)
			if ok {
				rf.processInstallSnapshot(peerId, args, reply)
			}
		}(peerId, args)
	} else {
		// Send AppendEntriesArgs
		prevLogIndex, prevLogTerm := rf.nextIndex[peerId] - 1, rf.lastIncludedTerm
		if prevLogIndex != rf.lastIncludedIndex {
			prevLogTerm = rf.log[rf.index2offset(prevLogIndex)].Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[rf.index2offset(prevLogIndex + 1) : ],
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		go func(peerId int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerId, args, reply)
			if ok {
				rf.processAppendEntriesReply(peerId, args, reply)
			}
		}(peerId, args)
	}
}

func (rf *Raft) updateCommitIndex(updatedIndex int) {

	if updatedIndex <= rf.lastIncludedIndex || updatedIndex <= rf.commitIndex {
		return
	}

	index, offset := rf.commitIndex + 1, rf.index2offset(rf.commitIndex + 1)
	newCommitIndex := rf.commitIndex
	for ; offset < len(rf.log); index, offset = index + 1, offset + 1 {
		if rf.log[offset].Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for peerId := range rf.peers {
			if rf.matchIndex[peerId] >= index {
				cnt++
			}
		}
		if cnt >= rf.majorityNum {
			newCommitIndex = index
		} else {
			break
		}
	}
	for index := rf.commitIndex + 1; index <= newCommitIndex; index++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: 	true,
			Command: 		rf.log[rf.index2offset(index)].Command,
			CommandIndex: 	index + 1,
			CommandTerm: 	rf.log[rf.index2offset(index)].Term,
		}
		DPrintf("[Raft %v, Term %v]: commit {%v} at index {%v}.",
			rf.me, rf.currentTerm, rf.log[rf.index2offset(index)].Command, index)
	}
	rf.commitIndex = newCommitIndex
}

func (rf *Raft) index2offset(index int) int {
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) offset2index(offset int) int {
	return rf.lastIncludedIndex + offset + 1
}

func getElectionTimeout() time.Duration {
	return time.Duration(minElectionTimeout +
		rand.Intn(maxElectionTimeout - minElectionTimeout + 1)) * time.Millisecond
}

func getHeartBeatTime() time.Duration {
	return heartBeatTime * time.Millisecond
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

