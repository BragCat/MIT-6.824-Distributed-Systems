package shardmaster


import (
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type OperationType string

const (
	JOIN 	OperationType = "JOIN"
	LEAVE	OperationType = "LEAVE"
	MOVE	OperationType = "MOVE"
	QUERY	OperationType = "QUERY"
	TIMEOUT = 1000 * time.Millisecond
)

type RequestIndex struct {
	index 	int
	term 	int
}

type RequestResult struct {
	value 		Config
	pendingCh 	chan bool
}

type ShardMaster struct {
	mu				sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg

	// Your data here.
	configs 		[]Config // indexed by config num
	persister 		*raft.Persister
	pendingRequests map[RequestIndex]*RequestResult
	exitSignal 		chan bool
}


type Op struct {
	// Your data here.
	Operation 	OperationType
	Argument 	interface{}
	CkId		int64
	Sequence	int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Operation: 	JOIN,
		Argument:	args.Servers,
		CkId:      	args.CkId,
		Sequence:  	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(&op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Operation: 	LEAVE,
		Argument:	args.GIDs,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(&op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Operation:	MOVE,
		Argument: 	*args,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(&op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Operation:	QUERY,
		Argument:	args.Num,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, reply.Config = sm.sendCommand(&op)
}

func (sm *ShardMaster) sendCommand(op *Op) (bool, Err, Config) {
	index, term, isLeader := sm.rf.Start(*op)

	if !isLeader {
		return true, "Wrong Leader", Config{}
	}

	requestIndex := RequestIndex{
		index: index,
		term:  term,
	}
	requestResult := &RequestResult{
		value:     Config{},
		pendingCh: make(chan bool),
	}

	sm.mu.Lock()
	sm.pendingRequests[requestIndex] = requestResult
	sm.mu.Unlock()

	select {
	case success := <- requestResult.pendingCh:
		if success {
			return false, "", requestResult.value
		} else {
			return false, "Apply Failed", Config{}
		}
	case <- time.After(TIMEOUT):
		return false, "Request Timeout", Config{}
	}
}


func (sm *ShardMaster) daemon() {
	for {
		select {
		case <- sm.exitSignal:
			return
		case applyMsg := <- sm.applyCh:
			config := sm.apply(applyMsg)
			sm.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex, config)
		}
	}
}

func (sm *ShardMaster) apply(applyMsg raft.ApplyMsg) *Config {

	return nil
}

func (sm *ShardMaster) cleanPendingRequests(term int, index int, config *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for requestIndex, requestResult := range sm.pendingRequests {
		if requestIndex.term < term || (requestIndex.term == term && requestIndex.index < index) {
			requestResult.pendingCh <- false
			delete(sm.pendingRequests, requestIndex)
		} else if requestIndex.term == term && requestIndex.index == index {
			requestResult.value = *config
			requestResult.pendingCh <- true
			delete(sm.pendingRequests, requestIndex)
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.exitSignal <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.persister = persister
	sm.pendingRequests = make(map[RequestIndex]*RequestResult)
	sm.exitSignal = make(chan bool)

	sm.daemon()

	return sm
}
