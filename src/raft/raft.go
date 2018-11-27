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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term int
	Command interface{}
}

type StateType string

const (
	maxElectionTimeout = 450
	minElectionTimeout = 300
	heartBeatTime = 105

	leader StateType = "leader"
	candidate StateType = "candidate"
	follower StateType = "follower"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Paper defined persistent state
	currentTerm int
	votedFor int
	log []Entry

	// Paper defined volatile state
	commitIndex int
	lastApplied int

	// Paper defined volatile state on leaders
	nextIndex []int
	matchIndex []int

	// My volatile state
	majorityNum int
	voteCnt int
	applyCh chan ApplyMsg
	state StateType
	electionTimer *time.Timer
	heartBeatTimer *time.Timer
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
	buffer := new (bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
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
	var currentTerm, votedFor int
	log := make([]Entry, 0)
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil {
		panic("Read persisted data error.")
	} else {
		rf.currentTerm, rf.votedFor = currentTerm, votedFor
		rf.log = log
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogTerm int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server %v, term %v]: receive RequestVoteArgs %v from server %v, currently vote for %v.",
		rf.me, rf.currentTerm, *args, args.CandidateId, rf.votedFor)

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else {
		if args.Term > rf.currentTerm {
			rf.setToFollower(args.Term)
		}
		valid := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		lastLogIndex := -1
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogIndex = len(rf.log) - 1
			lastLogTerm = rf.log[lastLogIndex].Term
		}
		upToDate := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		reply.Term, reply.VoteGranted = rf.currentTerm, valid && upToDate

		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
	}
	DPrintf("[server %v, term %v]: reply RequestVoteReply %v to server %v.",
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[server %v, term %v]: send RequestVoteArgs %v to server %v.",
		args.CandidateId, args.Term, *args, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) processRequestVoteReply(peerId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server %v, term %v]: receive RequestVoteReply %v from server %v.",
		rf.me, rf.currentTerm, *reply, peerId)

	// Make sure this reply is to this current term
	if args.Term != rf.currentTerm || rf.state != candidate {
		return
	}

	// Abandon stale reply
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
			DPrintf("[server %v, term %v]: become leader.", rf.me, rf.currentTerm)
			rf.state = leader

			nextIndex := len(rf.log)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = -1
			}

			rf.electionTimer.Stop()
			rf.heartBeatTimer.Reset(0)
		}
	}
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server %v, term %v]: receive AppendEntriesArgs %v from server %v.",
		rf.me, rf.currentTerm, *args, args.LeaderId)

	if args.Term < rf.currentTerm {
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

		if args.PrevLogIndex > len(rf.log) - 1 {
			reply.Term, reply.Success = rf.currentTerm, false
			return
		}
		if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term, reply.Success = rf.currentTerm, false
			return
		}

		// Append new entries
		i, j := args.PrevLogIndex + 1, 0
		for ; i < len(rf.log) && j < len(args.Entries); {
			if rf.log[i].Term != args.Entries[j].Term {
				rf.log = append([]Entry{}, rf.log[ : i]...)
				rf.log = append(rf.log, args.Entries[j : ]...)
				j = len(args.Entries)
				break
			}
			i++
			j++
		}
		if j != len(args.Entries) {
			rf.log = append(rf.log, args.Entries[j : ]...)
		}

		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, len(rf.log) - 1)
			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					true,
					rf.log[i].Command,
					i + 1,
				}
				DPrintf("[server %v, term %v]: commit {%v} at index {%v}.",
					rf.me, rf.currentTerm, rf.log[i].Command, i)
			}
			rf.commitIndex = newCommitIndex
		}
		reply.Term, reply.Success = rf.currentTerm, true
		rf.persist()
	}

	DPrintf("[server %v, term %v]: reply AppendEntriesReply %v, log length = %v, commitIndex = %v.",
		rf.me, rf.currentTerm, *reply, len(rf.log), rf.commitIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[server %v, term %v]: send AppendEntriesArgs %v to server %v.",
		args.LeaderId, args.Term, *args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) processAppendEntriesReply(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server %v, term %v]: receive AppendEntriesReply %v from server %v.",
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
		newNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
		if newNextIndex > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = newNextIndex
			rf.matchIndex[peerId] = newNextIndex - 1
			rf.updateCommitIndex()
		}
	} else {
		rf.nextIndex[peerId] = rf.nextIndex[peerId] / 2
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
		DPrintf("[server %v, term %v]: add command %v.", rf.me, rf.currentTerm, command)
		entry := Entry{
			rf.currentTerm,
			command,
		}
		rf.log = append(rf.log, entry)

		index = len(rf.log)
		term = rf.currentTerm

		rf.heartBeatTimer.Reset(0)
		rf.persist()
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

	DPrintf("[server %v, term %v]: start election.", rf.me, rf.currentTerm)

	if rf.state == leader {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.state = candidate
	rf.electionTimer.Reset(getElectionTimeout())

	lastLogIndex, lastLogTerm := -1, 0
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogTerm,
		lastLogIndex,
	}

	rf.persist()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			rf.sendRequestVoteTo(i, args)
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
	go func() {
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(peerId, args, &reply)
		if ok {
			rf.processRequestVoteReply(peerId, args, &reply)
		}
	}()
}

func (rf *Raft) sendHeartBeat() {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendHeartBeatTo(i)
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

	// Construct AppendEntriesArgs
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := 0
	entries := append([]Entry{}, rf.log[prevLogIndex + 1 : ]...)
	if  prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args := &AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		rf.commitIndex,
	}

	rf.mu.Unlock()

	go func() {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peerId, args, reply)
		if ok {
			rf.processAppendEntriesReply(peerId, args, reply)
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	oldCommitIndex := rf.commitIndex
	for i := oldCommitIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		cnt := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				cnt++
			}
		}
		if cnt + 1 >= rf.majorityNum {
			rf.commitIndex = i
		} else {
			break
		}
	}
	for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			true,
			rf.log[i].Command,
			i + 1,
		}
		DPrintf("[server %v, term %v]: commit {%v} at index {%v}.",
			rf.me, rf.currentTerm, rf.log[i].Command, i)
	}
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
