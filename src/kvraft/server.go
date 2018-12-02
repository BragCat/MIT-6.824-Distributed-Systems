package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const requestTimeOut = 100

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Value string
	CxId int64
	Sequence int
}

type RequestIndex struct {
	term int
	index int
}

type RequestResult struct {
	value string
	pendingChan chan bool
}

type StateMachine struct {
	KVs map[string]string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendingRequests map[RequestIndex]*RequestResult
	sm StateMachine
	lastAppliedIndex int
	exitSignal chan bool
}

func (kv *KVServer) sendOpLog(op *Op) (bool, Err, string) {
	index, term, isLeader := kv.rf.Start(*op)

	if !isLeader {
		return true, "Wrong leader", ""
	}

	DPrintf("[server %v]: send log %v", kv.me, *op)

	requestIndex := RequestIndex{
		term: term,
		index: index,
	}
	requestResult := RequestResult{
		value: "",
		pendingChan: make(chan bool, 1),
	}
	kv.mu.Lock()
	kv.pendingRequests[requestIndex] = &requestResult
	kv.mu.Unlock()

	select {
		case <-requestResult.pendingChan:
			return false, "", requestResult.value
		case <- time.After(requestTimeOut):
	}
	return false, "Request timeout", ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation: "Get",
		Key: args.Key,
		Value: "",
		CxId: args.CkId,
		Sequence: args.Sequence,
	}

	wrongLeader, err, value := kv.sendOpLog(&op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation: args.Op,
		Key: args.Key,
		Value: args.Value,
		CxId: args.CkId,
		Sequence: args.Sequence,
	}

	wrongLeader, err, _ := kv.sendOpLog(&op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (kv *KVServer) daemon() {
	for {
		select {
			case <- kv.exitSignal:
				return
			case applyMsg := <- kv.applyCh:
				if applyMsg.CommandValid {
					DPrintf("[server %v]: receive ApplyMsg %v", kv.me, applyMsg)
					command, ok := applyMsg.Command.(Op)
					if !ok {
						panic("ApplyMsg.Command convert failed!")
					}
					index, term := applyMsg.CommandIndex, applyMsg.CommandTerm
					kv.mu.Lock()
					if kv.lastAppliedIndex + 1 != index {
						panic("ApplyMsg.CommandIndex isn't continuous!")
					}
					kv.lastAppliedIndex = index
					requestIndex := RequestIndex{
						term: term,
						index: index,
					}
					requestResult := kv.pendingRequests[requestIndex]
					switch command.Operation {
					case "Get":
						requestResult.value = command.Value
						requestResult.pendingChan <- true
					case "Put":
						kv.sm.KVs[command.Key] = command.Value
						requestResult.pendingChan <- true
					case "Append":
						kv.sm.KVs[command.Key] += command.Value
					default:
						panic("ApplyMsg.Command operation is invalid!")
					}
					delete(kv.pendingRequests, requestIndex)
					kv.mu.Unlock()
				}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.pendingRequests = make(map[RequestIndex]*RequestResult)
	kv.sm = StateMachine {
		KVs: make(map[string]string),
	}

	go kv.daemon()

	return kv
}
