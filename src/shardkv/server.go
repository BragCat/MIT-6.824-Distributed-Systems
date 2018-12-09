package shardkv

import (
	"bytes"
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
	pendingCh	chan Err
}

type GetResult struct {
	sequence	int
	value 		string
}

type KeyValue struct {
	Key		string
	Value 	string
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
	mck					*shardmaster.Clerk
	pendingRequest		map[RequestIndex]*RequestResult
	stateMachine  		[]ShardStateMachine
	lastAppliedIndex	int
	config				shardmaster.Config
	persister 			*raft.Persister
	applyDaemonExit		chan bool
	configDaemonExit	chan bool
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
		pendingCh: make(chan Err),
	}

	kv.mu.Lock()
	kv.pendingRequest[requestIndex] = &requestResult
	kv.mu.Unlock()

	select {
	case err := <- requestResult.pendingCh:
		return false, err, requestResult.value
	case <- time.After(RequestTimeout):
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
	kv.applyDaemonExit <- true
	kv.configDaemonExit <- true
}



func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case applyMsg := <- kv.applyCh:
			value, err, isSnapshot := kv.apply(&applyMsg)
			kv.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex, value, err)
			if !isSnapshot {
				kv.mayTakeSnapshot()
			}
		case <- kv.applyDaemonExit:
			return
		}
	}
}


func (kv *ShardKV) apply(applyMsg *raft.ApplyMsg) (string, Err, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := ""
	err := OK
	isSnapshot := !applyMsg.CommandValid
	if !isSnapshot {
		// Get or PutAppend
		op, ok := applyMsg.Command.(Op)
		if !ok {
			panic("ApplyMsg.Command convert to Op fail!")
		}

		switch op.Operation {
		case GET:
			value, err = kv.applyGet(&op)
		case PUT:
			err = kv.applyPutAppend(&op)
		case APPEND:
			err = kv.applyPutAppend(&op)
		case NEWCONFIG:
			config := op.Argument.(shardmaster.Config)
			err = kv.applyNewConfig(&config)
		}
	} else {
		// Snapshot
		buffer := bytes.NewBuffer(applyMsg.Command.([]byte))
		decoder := labgob.NewDecoder(buffer)
		newSM := make([]ShardStateMachine, shardmaster.NShards)
		var lastAppliedIndex int
		if decoder.Decode(&newSM) != nil ||
			decoder.Decode(&lastAppliedIndex) != nil {
			panic("Decode snapshot error!")
		} else {
			if lastAppliedIndex > kv.lastAppliedIndex {
				kv.stateMachine = newSM
				kv.lastAppliedIndex = lastAppliedIndex
			}
		}
	}
	return value, err, isSnapshot
}

func (kv *ShardKV) applyGet(op *Op) (string, Err) {
	value := ""
	err := OK
	if op.Sequence < kv.stateMachine[op.ShardId].Sequence[op.CkId] {
		value = kv.stateMachine[op.ShardId].GetResultCache[op.CkId].value
	} else if op.Sequence == kv.stateMachine[op.ShardId].Sequence[op.CkId] {
		value = kv.stateMachine[op.ShardId].KVs[op.Argument.(string)]
		kv.stateMachine[op.ShardId].GetResultCache[op.CkId] = GetResult{
			sequence:	op.Sequence,
			value:		value,
		}
		kv.stateMachine[op.ShardId].Sequence[op.CkId]++
	} else {
		err = ErrUnorderedSeq
	}
	return value, err
}

func (kv *ShardKV) applyPutAppend(op *Op) Err {
	err := OK
	if op.Sequence == kv.stateMachine[op.ShardId].Sequence[op.CkId] {
		keyValue := op.Argument.(KeyValue)
		if op.Operation == PUT {
			kv.stateMachine[op.ShardId].KVs[keyValue.Key] = keyValue.Value
		} else {
			kv.stateMachine[op.ShardId].KVs[keyValue.Key] += keyValue.Value
		}
		kv.stateMachine[op.ShardId].Sequence[op.CkId]++
	}
	return err
}

func (kv *ShardKV) applyNewConfig(config *shardmaster.Config) Err {
	return OK
}



func (kv *ShardKV) cleanPendingRequests(term int, index int, value string, err Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for requestIndex, requestResult := range kv.pendingRequest {
		if requestIndex.term < term || (requestIndex.term == term && requestIndex.index < index) {
			requestResult.pendingCh <- err
			delete(kv.pendingRequest, requestIndex)
		} else if requestIndex.term == term && requestIndex.index == index {
			requestResult.value = value
			requestResult.pendingCh <- err
			delete(kv.pendingRequest, requestIndex)
		}
	}
}

func (kv *ShardKV) mayTakeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	threshold := kv.maxraftstate / 5 * 4
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() <= threshold {
		return
	}

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastAppliedIndex)
	data := buffer.Bytes()
	go func(kv *ShardKV, data []byte, lastAppliedIndex int) {
		kv.rf.TakeSnapshot(data, lastAppliedIndex)
	}(kv, data, kv.lastAppliedIndex)
}


func (kv *ShardKV) configDaemon() {
	for {
		select {
		case <- time.After(ConfigUpdateTime):
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				currentConfigNum := kv.config.Num
				kv.mu.Unlock()

				newConfig := kv.mck.Query(currentConfigNum + 1)
				op := Op{
					Operation: NEWCONFIG,
					Argument:  newConfig,
					CkId:      0,
					ShardId:   0,
					Sequence:  0,
				}
				kv.rf.Start(op)
			}
			return
		case <- kv.configDaemonExit:
			return
		}
	}
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KeyValue{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.pendingRequest = make(map[RequestIndex]*RequestResult)
	kv.stateMachine = make([]ShardStateMachine, shardmaster.NShards)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.stateMachine[shard] = ShardStateMachine{
			KVs:            make(map[string]string),
			Sequence:       make(map[int64]int),
			GetResultCache: make(map[int64]GetResult),
			ShardId:        shard,
			ConfigNum:      0,
		}
	}
	kv.lastAppliedIndex	= -1
	kv.config = shardmaster.Config{}
	kv.persister = persister
	kv.applyDaemonExit = make(chan bool)
	kv.configDaemonExit = make(chan bool)


	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyDaemon()
	go kv.configDaemon()

	return kv
}
