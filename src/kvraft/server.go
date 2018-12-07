package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const requestTimeOut = 100 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Value string
	CkId int64
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

type GetResult struct {
	Sequence int
	Value string
}

type StateMachine struct {
	KVs map[string]string
	Sequence map[int64]int
	GetResultCache map[int64]GetResult
	LastAppliedIndex int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	pendingRequests map[RequestIndex]*RequestResult
	sm StateMachine
	exitSignal chan bool
}

func (kv *KVServer) sendOpLog(op Op) (bool, Err, string) {
	index, term, isLeader := kv.rf.Start(op)
	index--

	if !isLeader {
		return true, "Wrong leader", ""
	}

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
	DPrintf("[KVServer %v]: send operation %v, pendingRequest size %v", kv.me, op, len(kv.pendingRequests))
	kv.mu.Unlock()

	select {
		case success := <- requestResult.pendingChan:
			DPrintf("[KVServer %v]: reply value %v to operation %v", kv.me, requestResult.value, op)
			var err Err
			if !success {
				err = "Apply failed"
			}
			return false, err, requestResult.value
		case <- time.After(requestTimeOut):
			return false, "Request timeout", ""
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation: "Get",
		Key: args.Key,
		Value: "",
		CkId: args.CkId,
		Sequence: args.Sequence,
	}

	wrongLeader, err, value := kv.sendOpLog(op)
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
		CkId: args.CkId,
		Sequence: args.Sequence,
	}

	wrongLeader, err, _ := kv.sendOpLog(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (kv *KVServer) daemon() {
	for {
		select {
		case <-kv.exitSignal:
			return
		case applyMsg := <-kv.applyCh:
			DPrintf("[KVServer %v]: receive ApplyMsg %v", kv.me, applyMsg)
			result, isSnapshot := kv.apply(applyMsg)
			kv.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex-1, result)

			if !isSnapshot {
				kv.takeSnapshot()
			}
		}
	}
}

func (kv *KVServer) apply(applyMsg raft.ApplyMsg) (result string, isSnapshot bool) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	result = ""
	isSnapshot = !applyMsg.CommandValid

	if applyMsg.CommandValid {

		op, ok := applyMsg.Command.(Op)
		if !ok {
			panic("ApplyMsg.Command convert failed!")
		}

		DPrintf("[KVServer %v]: apply operation %v of index %v, LastAppliedIndex is %v",
			kv.me, op, applyMsg.CommandIndex - 1, kv.sm.LastAppliedIndex)

		if kv.sm.LastAppliedIndex + 1 != applyMsg.CommandIndex - 1 {
			panic("ApplyMsg.CommandIndex isn't continuous!")
		}
		kv.sm.LastAppliedIndex++

		if op.Sequence < kv.sm.Sequence[op.CkId] {
			// Only take care of "Get", stale "PutAppend" operations should be abandoned
			DPrintf("[KVServer %v]: operation %v is stale", kv.me, op)
			if op.Operation == "Get" {
				lastGetResult := kv.sm.GetResultCache[op.CkId]
				if op.Sequence != lastGetResult.Sequence {
					panic("Get op is applied unordered!")
				}
				result = lastGetResult.Value
			}
		} else if op.Sequence == kv.sm.Sequence[op.CkId] {
			DPrintf("[KVServer %v]: operation %v is fresh", kv.me, op)
			switch op.Operation {
			case "Get":
				kv.sm.GetResultCache[op.CkId] = GetResult{
					Sequence: op.Sequence,
					Value:    kv.sm.KVs[op.Key],
				}
				result = kv.sm.KVs[op.Key]
			case "Put":
				kv.sm.KVs[op.Key] = op.Value
			case "Append":
				kv.sm.KVs[op.Key] += op.Value
			default:
				panic("ApplyMsg.Command operation is invalid!")
			}
			kv.sm.Sequence[op.CkId]++
		} else {
			panic("Client's sequence is not continuous!")
		}
	} else {
		buffer := bytes.NewBuffer(applyMsg.Command.([]byte))
		decoder := labgob.NewDecoder(buffer)
		var newSM StateMachine
		if decoder.Decode(&newSM) != nil {
			panic("Decode snapshot error!")
		} else {
			if newSM.LastAppliedIndex > kv.sm.LastAppliedIndex {
				DPrintf("[KVServer %v]: install snapshot %v", kv.me, newSM)
				kv.sm = newSM
			}
		}
	}
	return result, isSnapshot
}

func (kv *KVServer) cleanPendingRequests(term int, index int, result string) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for requestIndex, requestResult := range kv.pendingRequests {
		if requestIndex.term < term || (requestIndex.term == term && requestIndex.index < index) {
			requestResult.pendingChan <- false
			delete(kv.pendingRequests, requestIndex)
		} else if requestIndex.term == term && requestIndex.index == index {
			requestResult.value = result
			requestResult.pendingChan <- true
			delete(kv.pendingRequests, requestIndex)
		}
	}
}

func (kv *KVServer) takeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() <= kv.maxraftstate {
		return
	}

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.sm)
	data := buffer.Bytes()
	go func(kv *KVServer, data []byte, lastAppliedIndex int) {
		DPrintf("[KVServer %v]: take snapshot, LastAppliedIndex is %v",
			kv.me, lastAppliedIndex)
		kv.rf.TakeSnapshot(data, lastAppliedIndex)
	}(kv, data, kv.sm.LastAppliedIndex)
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
	kv.exitSignal <- true
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
	kv.persister = persister
	kv.pendingRequests = make(map[RequestIndex]*RequestResult)
	kv.sm = StateMachine {
		KVs: make(map[string]string),
		Sequence: make(map[int64]int),
		GetResultCache: make(map[int64]GetResult),
		LastAppliedIndex: -1,
	}
	kv.exitSignal = make(chan bool)

	go kv.daemon()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
