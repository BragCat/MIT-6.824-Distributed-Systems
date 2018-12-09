package shardkv

import (
	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)


// import "shardmaster"


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	OperationType
	Argument	interface{}
	CkId		int64
	ShardId		int
	Sequence	int
}

type RequestIndex struct {
	term	int
	index 	int
}

type RequestResult struct {
	value  		string
	pendingCh	chan bool
}

type GetResult struct {
	sequence	int
	value 		string
}

type KeyValue struct {
	key		string
	value 	string
}

type ShardStateMachine struct {
	KVs					map[string]string
	Sequence			map[int64]int
	GetResultCache		map[int64]GetResult
	ShardId				int
	ConfigNum			int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendingRequest		map[RequestIndex]*RequestResult
	stateMachine  		map[int]ShardStateMachine
	lastAppliedIndex	int
	config				shardmaster.Config
	persister 			*raft.Persister
	exitSignal			chan bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation:	GET,
		Argument:	args.Key,
		CkId:		args.CkId,
		ShardId:	args.ShardId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, reply.Value = kv.sendCommand(&op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation:	args.Op,
		Argument:	KeyValue{args.Key, args.Value},
		CkId:		args.CkId,
		ShardId:	args.ShardId,
		Sequence:	args.Sequence,
	}

	reply.WrongLeader, reply.Err, _ = kv.sendCommand(&op)
}

func (kv *ShardKV) sendCommand(op *Op) (bool, Err, string) {
	index, term, isLeader := kv.rf.Start(*op)

	if !isLeader {
		return true, ErrWrongLeader, ""
	}

	requestIndex := RequestIndex{
		term:  term,
		index: index,
	}
	requestResult := RequestResult{
		value:     "",
		pendingCh: make(chan bool),
	}

	kv.mu.Lock()
	kv.pendingRequest[requestIndex] = &requestResult
	kv.mu.Unlock()

	select {
	case success := <- requestResult.pendingCh:
		if success {
			return true, OK, requestResult.value
		} else {
			return false, ErrApplyFail, ""
		}
	case <- time.After(TIMEOUT):
		return false, ErrRequestTimeout, ""
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient td (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.exitSignal <- true
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.pendingRequest = make(map[RequestIndex]*RequestResult)
	kv.stateMachine = make(map[int]ShardStateMachine)
	kv.lastAppliedIndex	= -1
	kv.config = shardmaster.Config{}
	kv.persister = persister
	kv.exitSignal = make(chan bool)


	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.daemon()

	return kv
}


func (kv *ShardKV) daemon() {
	for {
		select {
		case applyMsg := <- kv.applyCh:
			value := kv.apply(&applyMsg)
			kv.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex, value)
		case <- kv.exitSignal:
			return
		}
	}
}


func (kv *ShardKV) apply(applyMsg *raft.ApplyMsg) string {
	return ""
}

func (kv *ShardKV) cleanPendingRequests(term int, index int, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for requestIndex, requestResult := range kv.pendingRequest {
		if requestIndex.term < term || (requestIndex.term == term && requestIndex.index < index) {
			requestResult.pendingCh <- false
			delete(kv.pendingRequest, requestIndex)
		} else if requestIndex.term == term && requestIndex.index == index {
			requestResult.value = value
			requestResult.pendingCh <- true
			delete(kv.pendingRequest, requestIndex)
		}
	}
}
