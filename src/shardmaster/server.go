package shardmaster


import (
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


const (
	TIMEOUT = 1000 * time.Millisecond

	JOIN	= iota
	LEAVE
	MOVE
	QUERY
)

type RequestIndex struct {
	index 	int
	term 	int
}

type RequestResult struct {
	value 		Config
	pendingCh 	chan bool
}

type StateMachine struct {
	configs 			[]Config // indexed by config num
	sequence			map[int64]int
	lastAppliedIndex	int
}

type ShardMaster struct {
	mu				sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg

	// Your data here.
	stateMachine 	StateMachine
	persister 		*raft.Persister
	pendingRequests map[RequestIndex]*RequestResult
	exitSignal 		chan bool
}


type Op struct {
	// Your data here.
	Operation 	int
	Argument 	interface{}
	CkId		int64
	Sequence	int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[ShardMaster %v]: receive JoinArgs %v", sm.me, *args)
	op := Op{
		Operation: 	JOIN,
		Argument:	args.Servers,
		CkId:      	args.CkId,
		Sequence:  	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("[ShardMaster %v]: receive LeaveArgs %v", sm.me, *args)
	op := Op{
		Operation: 	LEAVE,
		Argument:	args.GIDs,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("[ShardMaster %v]: receive MoveArgs %v", sm.me, *args)
	op := Op{
		Operation:	MOVE,
		Argument: 	*args,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = sm.sendCommand(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("[ShardMaster %v]: receive QueryArgs %v", sm.me, *args)
	op := Op{
		Operation:	QUERY,
		Argument:	args.Num,
		CkId:		args.CkId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, reply.Config = sm.sendCommand(op)
}

func (sm *ShardMaster) sendCommand(op Op) (bool, Err, Config) {
	DPrintf("[ShardMaster %v]: send command %v.", sm.me, op)
	index, term, isLeader := sm.rf.Start(op)
	index--

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
			DPrintf("[ShardMaster %v]: receive ApplyMsg %v", sm.me, applyMsg)
			config := sm.apply(applyMsg)
			sm.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex - 1, config)
		}
	}
}

func (sm *ShardMaster) apply(applyMsg raft.ApplyMsg) *Config {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if applyMsg.CommandValid {
		if sm.stateMachine.lastAppliedIndex + 1 != applyMsg.CommandIndex - 1 {
			panic("ApplyMsg isn't continuous!")
		}
		sm.stateMachine.lastAppliedIndex++
		op, ok := applyMsg.Command.(Op)

		if !ok {
			panic("ApplyMsg.Command convert to Op failed!")
		}

		if op.Operation == QUERY {
			index, ok := op.Argument.(int)
			if !ok {
				panic("QUERY operation argument convert failed.")
			}
			if index == -1 {
				index = len(sm.stateMachine.configs) - 1
			}
			return &sm.stateMachine.configs[index]
		} else if op.Sequence == sm.stateMachine.sequence[op.CkId] {
			configNum := len(sm.stateMachine.configs)
			config := Config{
				Num:	configNum,
				Shards: [NShards]int{},
				Groups: sm.stateMachine.configs[configNum - 1].Groups,
			}
			switch op.Operation {
			case JOIN:
				gidServers, ok := op.Argument.(map[int][]string)
				if !ok {
					panic("JOIN operation argument convert failed!")
				}
				for gid, servers := range gidServers {
					config.Groups[gid] = append(config.Groups[gid], servers...)
				}
				config.shuffle()

			case LEAVE:
				gids, ok := op.Argument.([]int)
				if !ok {
					panic("LEAVE operation argument convert failed!")
				}
				for _, gid := range gids {
					delete(config.Groups, gid)
				}
				config.shuffle()

			case MOVE:
				moveArgs, ok := op.Argument.(MoveArgs)
				if !ok {
					panic("MOVE operation argument convert failed!")
				}
				config.Shards[moveArgs.Shard] = moveArgs.GID
			default:
				panic("Unknown operation!")
			}
			sm.stateMachine.configs = append(sm.stateMachine.configs, config)
		}
	} else {
		// Raft read snapshot to initialize when start up
	}
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
			if config != nil {
				requestResult.value = *config
			}
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


	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.stateMachine = StateMachine{
		configs:          make([]Config, 1),
		sequence:         make(map[int64]int),
		lastAppliedIndex: -1,
	}
	sm.stateMachine.configs[0].Groups = map[int][]string{}
	sm.persister = persister
	sm.pendingRequests = make(map[RequestIndex]*RequestResult)
	sm.exitSignal = make(chan bool)

	go sm.daemon()

	return sm
}
