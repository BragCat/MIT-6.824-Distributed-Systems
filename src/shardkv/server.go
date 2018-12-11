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

const (
	RequestTimeout 					= 1000 * time.Millisecond
	ConfigUpdateTime				= 100 * time.Millisecond
	ShardPullTime					= 50 * time.Millisecond
	ShardCleanTime					= 50 * time.Millisecond


	GET 			OperationType 	= "GET"
	PUTAPPEND		OperationType   = "PUTAPPEND"
	NEWCONFIG 		OperationType 	= "NEWCONFIG"
	INSTALLCONFIG	OperationType 	= "INSTALLCONFIG"
	INSTALLSHARD	OperationType 	= "INSTALLSHARD"
	CLEANSHARD		OperationType 	= "CLEANSHARD"
	REMOVESHARD 	OperationType 	= "REMOVESHARD"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	OperationType
	Argument	interface{}
}

type RequestIndex struct {
	term	int
	index 	int
}

type RequestResult struct {
	value  		string
	pendingCh	chan struct{}
	err 		Err
}

type GetResult struct {
	Sequence	int
	Value 		string
	Err			Err
}

type ShardIndex struct {
	ConfigNum	int
	ShardId		int
}

type GetShardArgs struct {
	ConfigNum  int
	ShardId int
}

type GetShardReply struct {
	Success bool
	Content ShardStateMachine
}

type ShardStateMachine struct {
	KVs					map[string]string
	Sequence			map[int64]int
	GetResultCache		map[int64]GetResult
	ShardId				int
	ConfigNum			int
}

type CleanShardArgs struct {
	ConfigNum	int
	ShardId		int
}

type CleanShardReply struct {
	Success bool
}

type ShardKV struct {
	mu           			sync.Mutex
	me           			int
	rf           			*raft.Raft
	applyCh      			chan raft.ApplyMsg
	make_end     			func(string) *labrpc.ClientEnd
	gid          			int
	masters      			[]*labrpc.ClientEnd
	maxraftstate 			int // snapshot if log grows this big

	// Your definitions here.
	mck						*shardmaster.Clerk
	previousConfig			shardmaster.Config
	config					shardmaster.Config
	configInstalled			bool

	pullShardSet			map[ShardIndex][]string
	cleanShardSet			map[ShardIndex][]string

	stateMachine  			map[int]*ShardStateMachine
	pendingRequests			map[RequestIndex]*RequestResult
	lastAppliedIndex		int

	persister 				*raft.Persister

	applyDaemonExitCh		chan bool
	configDaemonExitCh		chan bool
	pullShardDaemonExitCh	chan bool
	cleanShardDaemonExitCh	chan bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation:	GET,
		Argument:	*args,
	}

	reply.WrongLeader, reply.Value, reply.Err = kv.sendCommand(&op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation:	PUTAPPEND,
		Argument:	*args,
	}

	reply.WrongLeader, _, reply.Err = kv.sendCommand(&op)
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num > args.ConfigNum {
		if _, exist := kv.stateMachine[args.ShardId]; exist {
			if kv.stateMachine[args.ShardId].ConfigNum != args.ConfigNum {
				reply.Success = false
			} else {
				reply.Success = true
				reply.Content = ShardStateMachine{
					KVs:       		make(map[string]string),
					Sequence:      	make(map[int64]int),
					GetResultCache: make(map[int64]GetResult),
					ShardId:      	args.ShardId,
					ConfigNum:      args.ConfigNum + 1,
				}

				for key, value := range kv.stateMachine[args.ShardId].KVs {
					reply.Content.KVs[key] = value
				}
				for ckId, sequence := range kv.stateMachine[args.ShardId].Sequence {
					reply.Content.Sequence[ckId] = sequence
				}
				for ckId, getResult := range kv.stateMachine[args.ShardId].GetResultCache {
					reply.Content.GetResultCache[ckId] = getResult
				}
			}
		} else {
			reply.Success = false
		}
	} else {
		reply.Success = false
	}
}

func (kv *ShardKV) CleanShard(args *CleanShardArgs, reply *CleanShardReply) {
	op := Op{
		Operation: CLEANSHARD,
		Argument:  *args,
	}
	wrongLeader, _, err := kv.sendCommand(&op)

	reply.Success = !wrongLeader && err == OK
}

func (kv *ShardKV) sendCommand(op *Op) (bool, string, Err) {

	DPrintf("[ShardKV %v, GID %v][sendCommand]: send command %v.",
		kv.me, kv.gid, *op)
	index, term, isLeader := kv.rf.Start(*op)
	index--

	if !isLeader {
		return true, "", ErrWrongLeader
	}

	requestIndex := RequestIndex{
		term:  term,
		index: index,
	}
	requestResult := RequestResult{
		pendingCh: make(chan struct{}, 0),
	}

	kv.mu.Lock()
	kv.pendingRequests[requestIndex] = &requestResult
	kv.mu.Unlock()

	select {
	case <- requestResult.pendingCh:
		DPrintf("[ShardKV %v, GID %v][sendCommand]: reply {err = %v, value = %v} to command %v",
			kv.me, kv.gid, requestResult.err, requestResult.value, *op)
		return false, requestResult.value, requestResult.err
	case <- time.After(RequestTimeout):
		DPrintf("[ShardKV %v, GID %v][sendCommand]: reply {err = %v, value = %v} to command %v",
			kv.me, kv.gid, ErrRetry, requestResult.value, *op)
		return false, "", ErrRetry
	}
}

func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case applyMsg := <- kv.applyCh:

			DPrintf("[ShardKV %v, GID %v][applyDaemon]: receive ApplyMsg %v.",
				kv.me, kv.gid, applyMsg)

			value, err := kv.apply(&applyMsg)
			if applyMsg.CommandValid {
				kv.cleanPendingRequests(applyMsg.CommandTerm, applyMsg.CommandIndex - 1, value, err)
				kv.mayTakeSnapshot()
			}
		case <- kv.applyDaemonExitCh:
			return
		}
	}
}

func (kv *ShardKV) apply(applyMsg *raft.ApplyMsg) (result string, err Err) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	result = ""
	err = OK

	if applyMsg.CommandValid {
		// Get or PutAppend
		if applyMsg.CommandIndex - 1 != kv.lastAppliedIndex + 1 {
			panic("ApplyMsg index is not continuous!")
		}
		kv.lastAppliedIndex++

		op, ok := applyMsg.Command.(Op)
		if !ok {
			panic("ApplyMsg.Command convert to Op fail!")
		}

		switch op.Operation {
		case GET:
			args := op.Argument.(GetArgs)
			result, err = kv.applyGetPutAppend(args.CkId, args.Sequence, args.Key, "", GET)

		case PUTAPPEND:
			args := op.Argument.(PutAppendArgs)
			result, err = kv.applyGetPutAppend(args.CkId, args.Sequence, args.Key, args.Value, args.Op)

		case NEWCONFIG:
			config := op.Argument.(shardmaster.Config)
			kv.applyNewConfig(&config)

		case INSTALLCONFIG:
			configNum := op.Argument.(int)
			kv.installConfig(configNum)

		case INSTALLSHARD:
			shardSM := op.Argument.(ShardStateMachine)
			kv.installShard(&shardSM)

		case CLEANSHARD:
			args := op.Argument.(CleanShardArgs)
			if !kv.cleanShard(&args) {
				err = ErrRetry
			}

		case REMOVESHARD:
			shardIndex := op.Argument.(ShardIndex)
			delete(kv.cleanShardSet, shardIndex)

		default:
			panic("Unknown command!")
		}
	} else {
		// Snapshot

		DPrintf("[ShardKV %v, GID %v][apply]: apply snapshot.",
			kv.me, kv.gid)

		buffer := bytes.NewBuffer(applyMsg.Command.([]byte))
		decoder := labgob.NewDecoder(buffer)
		var previousConfig, config shardmaster.Config
		var configInstalled bool
		pullShardSet := make(map[ShardIndex][]string)
		cleanShardSet := make(map[ShardIndex][]string)
		newSM := make(map[int]*ShardStateMachine)
		var lastAppliedIndex int
		if decoder.Decode(&previousConfig) != nil ||
			decoder.Decode(&config) != nil ||
			decoder.Decode(&configInstalled) != nil ||
			decoder.Decode(&pullShardSet) != nil ||
			decoder.Decode(&cleanShardSet) != nil ||
			decoder.Decode(&newSM) != nil ||
			decoder.Decode(&lastAppliedIndex) != nil {
			panic("Decode snapshot error!")
		} else {
			if lastAppliedIndex > kv.lastAppliedIndex {
				kv.previousConfig = previousConfig
				kv.config = config
				kv.configInstalled = configInstalled
				kv.pullShardSet = pullShardSet
				kv.cleanShardSet = cleanShardSet
				kv.stateMachine = newSM
				kv.lastAppliedIndex = lastAppliedIndex
			}
		}
	}
	return result, err
}

func (kv *ShardKV) applyGetPutAppend(ckId int64, sequence int, key string, value string,
	opType OperationType) (string, Err) {

	shardId := key2shard(key)
	result := ""
	err := OK

	if kv.config.Shards[shardId] != kv.gid {
		err = ErrWrongGroup
		return result, err
	}

	DPrintf("[ShardKV %v, GID %v][ApplyGetPutAppend]: apply Get/PutAppend command {%v, %v}.",
		kv.me, kv.gid, key, value)

	_, exist := kv.pullShardSet[ShardIndex{kv.previousConfig.Num, shardId}]
	if exist {
		err = ErrRetry
		return result, err
	}

	shardSM := kv.stateMachine[shardId]
	if sequence < shardSM.Sequence[ckId] {

		if opType == GET {
			if sequence != shardSM.GetResultCache[ckId].Sequence {
				panic("GetResult cache missed!")
			}
			result = shardSM.GetResultCache[ckId].Value
			err = shardSM.GetResultCache[ckId].Err
		}
	} else if sequence == shardSM.Sequence[ckId] {

		switch opType {
		case GET:
			result, exist = shardSM.KVs[key]
			if !exist {
				err = ErrNoKey
			}
			shardSM.GetResultCache[ckId] = GetResult{
				Sequence: sequence,
				Value:    result,
				Err:      err,
			}
		case PUT:
			shardSM.KVs[key] = value
		case APPEND:
			shardSM.KVs[key] += value
		}
		shardSM.Sequence[ckId]++

	} else {
		panic("Unordered ApplyMsg!")
	}
	return result, err
}

func (kv *ShardKV) applyNewConfig(newConfig *shardmaster.Config) {

	DPrintf("[ShardKV %v, GID %v][applyNewConfig]: current config is %v, apply new config %v.",
		kv.me, kv.gid, kv.config, newConfig)

	if newConfig.Num <= kv.config.Num {
		return
	}

	if newConfig.Num > kv.config.Num + 1 {
		panic("Config num is not continuous!")
	}

	if !kv.configInstalled {
		panic("Previous config is not installed!")
	}

	if len(kv.pullShardSet) != 0 {
		panic("PullShardSet is not empty!")
	}

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if kv.config.Shards[shard] == kv.gid {
				kv.stateMachine[shard].ConfigNum = newConfig.Num
			} else if kv.config.Shards[shard] != 0 {
				// There is data in this shard, but this shard is in charge of other groups
				previousGid := kv.config.Shards[shard]
				shardIndex := ShardIndex{
					ConfigNum: kv.config.Num,
					ShardId:   shard,
				}
				kv.pullShardSet[shardIndex] = append([]string{}, kv.config.Groups[previousGid]...)
			} else {
				// There is no data in this shard
				kv.stateMachine[shard] = &ShardStateMachine{
					KVs:            make(map[string]string),
					Sequence:       make(map[int64]int),
					GetResultCache: make(map[int64]GetResult),
					ShardId:        shard,
					ConfigNum:      newConfig.Num,
				}
			}
		}
	}
	kv.previousConfig = kv.config
	kv.config = *newConfig
	kv.configInstalled = false
}

func (kv *ShardKV) installConfig(configNum int) {

	DPrintf("[ShardKV %v, GID %v][installConfig]: install config version %v.",
		kv.me, kv.gid, configNum)

	if configNum != kv.config.Num {
		return
	}

	if len(kv.pullShardSet) != 0 {
		panic("PullShardSet is not empty!")
	}

	kv.configInstalled = true
}


func (kv *ShardKV) installShard(shardSM *ShardStateMachine) {

	DPrintf("[ShardKV %v, GID %v][installShard]: install shard state machine %v.",
		kv.me, kv.gid, *shardSM)

	shardIndex := ShardIndex{
		ConfigNum: shardSM.ConfigNum - 1,
		ShardId:   shardSM.ShardId,
	}

	if _, exist := kv.pullShardSet[shardIndex]; exist {
		if shardSM.ConfigNum != kv.config.Num {
			panic("Install wrong version Shard")
		}

		kv.stateMachine[shardSM.ShardId] = shardSM
		kv.cleanShardSet[shardIndex] = kv.pullShardSet[shardIndex]

		delete(kv.pullShardSet, shardIndex)
	}
}

func (kv *ShardKV) cleanShard(args *CleanShardArgs) bool {

	DPrintf("[ShardKV %v, GID %v][cleanShard]: clean shard (ConfigNum: %v, ShardId: %v).",
		kv.me, kv.gid, args.ConfigNum, args.ShardId)

	if kv.config.Shards[args.ShardId] == kv.gid {
		return false
	}

	if shardSM, exist := kv.stateMachine[args.ShardId]; exist {
		if shardSM.ConfigNum <= args.ConfigNum {
			delete(kv.stateMachine, args.ShardId)
			return true
		} else {
			return false
		}
	}
	return true
}

func (kv *ShardKV) cleanPendingRequests(term int, index int, value string, err Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for requestIndex, requestResult := range kv.pendingRequests {

		if requestIndex.term < term || (requestIndex.term == term && requestIndex.index < index) {

			requestResult.err = ErrRetry
			close(requestResult.pendingCh)
			delete(kv.pendingRequests, requestIndex)

		} else if requestIndex.term == term && requestIndex.index == index {

			requestResult.value = value
			requestResult.err = err
			close(requestResult.pendingCh)
			delete(kv.pendingRequests, requestIndex)
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

	DPrintf("[ShardKV %v, GID %v]: take snapshot.",
		kv.me, kv.gid)

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.previousConfig)
	encoder.Encode(kv.config)
	encoder.Encode(kv.configInstalled)
	encoder.Encode(kv.pullShardSet)
	encoder.Encode(kv.cleanShardSet)
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
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.mu.Lock()
				currentConfigNum := kv.config.Num
				configInstalled := kv.configInstalled
				kv.mu.Unlock()

				if configInstalled {
					newConfig := kv.mck.Query(-1)
					if newConfig.Num > currentConfigNum {
						if newConfig.Num > currentConfigNum + 1 {
							newConfig = kv.mck.Query(currentConfigNum + 1)
						}
						op := Op{
							Operation: NEWCONFIG,
							Argument:  newConfig,
						}
						DPrintf("[ShardKV %v, GID %v][configDaemon]: current config is %v, get new config %v, send command %v.",
							kv.me, kv.gid, kv.config, newConfig, op)
						kv.rf.Start(op)
					}
				}
			}
		case <- kv.configDaemonExitCh:
			return
		}
	}
}


func (kv *ShardKV) pullShardDaemon() {
	for {
		select {
		case <- time.After(ShardPullTime):
			kv.pullShards()
		case <- kv.pullShardDaemonExitCh:
			return
		}
	}
}

func (kv *ShardKV) pullShards() {

	kv.mu.Lock()
	pullShardSet := make(map[ShardIndex][]string)
	for shardIndex, servers := range kv.pullShardSet {
		pullShardSet[shardIndex] = append([]string{}, servers...)
	}
	kv.mu.Unlock()

	wg := sync.WaitGroup{}
	for shardIndex, servers := range pullShardSet {
		wg.Add(1)
		go kv.pullShard(shardIndex.ConfigNum, shardIndex.ShardId, servers, &wg)
	}
	wg.Wait()

	kv.mu.Lock()
	configNum := kv.config.Num
	remainPullShard := len(kv.pullShardSet)
	configInstalled := kv.configInstalled
	kv.mu.Unlock()

	if remainPullShard == 0 && !configInstalled {
		op := Op{
			Operation: INSTALLCONFIG,
			Argument:  configNum,
		}
		kv.rf.Start(op)
	}
}

func (kv *ShardKV) pullShard(configNum int, shardId int, servers []string, wg *sync.WaitGroup) {

	if valid, shardSM := kv.getShardContent(configNum, shardId, servers); valid {
		for {
			op := Op{
				Operation: INSTALLSHARD,
				Argument:  *shardSM,
			}
			wrongLeader, _, err := kv.sendCommand(&op)

			if wrongLeader || err == OK {
				break
			}
		}
	}

	wg.Done()
}

func (kv *ShardKV) getShardContent(configNum int, shardId int, servers []string) (bool, *ShardStateMachine) {

	if len(servers) == 0 {
		panic("Previous groups' server list is empty!")
	}

	args := GetShardArgs{
		ConfigNum:  configNum,
		ShardId: 	shardId,
	}
	for si, sz := 0, len(servers); si < sz; si++ {
		_, isLeader := kv.rf.GetState()

		if !isLeader {
			return false, nil
		}

		srv := kv.make_end(servers[si])
		reply := GetShardReply{}

		ok := srv.Call("ShardKV.GetShard", &args, &reply)

		if ok && reply.Success {
			return true, &(reply.Content)
		}
	}
	return false, nil
}

func (kv *ShardKV) cleanShardDaemon() {
	for {
		select {
		case <- time.After(ShardCleanTime):
			kv.cleanShards()
		case <- kv.cleanShardDaemonExitCh:
			return
		}
	}
}

func (kv *ShardKV) cleanShards() {

	_, isLeader := kv.rf.GetState()

	if isLeader {
		kv.mu.Lock()
		cleanShardSet := make(map[ShardIndex][]string)
		for shardIndex, servers := range kv.cleanShardSet {
			cleanShardSet[shardIndex] = append([]string{}, servers...)
		}
		kv.mu.Unlock()

		wg := sync.WaitGroup{}

		for shardIndex, servers := range cleanShardSet {
			wg.Add(1)
			go kv.cleanShardRequest(shardIndex.ConfigNum, shardIndex.ShardId, servers, &wg)
		}

		wg.Wait()
	}
}

func (kv *ShardKV) cleanShardRequest(configNum int, shardId int, servers []string, wg *sync.WaitGroup) {

	if kv.sendCleanRequest(configNum, shardId, servers) {
		for {
			op := Op{
				Operation: REMOVESHARD,
				Argument:  ShardIndex{
					ConfigNum: configNum,
					ShardId:   shardId,
				},
			}
			wrongLeader, _, err := kv.sendCommand(&op)

			if wrongLeader || err == OK {
				break
			}
		}
	}

	wg.Done()
}

func (kv *ShardKV) sendCleanRequest(configNum int, shardId int, servers []string) bool {
	if len(servers) == 0 {
		panic("Destination server list is empty")
	}

	args := CleanShardArgs{
		ConfigNum:  configNum,
		ShardId: 	shardId,
	}
	for si, sz := 0, len(servers); si < sz; si++ {
		_, isLeader := kv.rf.GetState()

		if !isLeader {
			return false
		}

		srv := kv.make_end(servers[si])
		reply := CleanShardReply{}

		ok := srv.Call("ShardKV.CleanShard", &args, &reply)

		if ok && reply.Success {
			return true
		}
	}
	return false
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
	go func() {
		kv.applyDaemonExitCh <- true
		kv.configDaemonExitCh <- true
		kv.pullShardDaemonExitCh <- true
		kv.cleanShardDaemonExitCh <- true
	}()
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(ShardStateMachine{})
	labgob.Register(make(map[string]string))
	labgob.Register(make(map[int64]int))
	labgob.Register(GetResult{})
	labgob.Register(make(map[int64]GetResult))
	labgob.Register(ShardIndex{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetShardArgs{})
	labgob.Register(GetShardReply{})
	labgob.Register(CleanShardArgs{})
	labgob.Register(CleanShardReply{})
	labgob.Register(make(map[int][]string))
	labgob.Register(make(map[ShardIndex][]string))
	labgob.Register(make(map[int]*ShardStateMachine))

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.previousConfig = shardmaster.Config{
		Num:	0,
		Shards: [shardmaster.NShards]int{},
		Groups:	make(map[int][]string),
	}
	kv.config = shardmaster.Config{
		Num:    0,
		Shards: [shardmaster.NShards]int{},
		Groups: make(map[int][]string),
	}
	kv.configInstalled = true

	kv.pullShardSet = make(map[ShardIndex][]string)
	kv.cleanShardSet = make(map[ShardIndex][]string)

	kv.stateMachine = make(map[int]*ShardStateMachine)
	kv.pendingRequests = make(map[RequestIndex]*RequestResult)
	kv.lastAppliedIndex	= -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister

	kv.applyDaemonExitCh = make(chan bool)
	kv.configDaemonExitCh = make(chan bool)
	kv.pullShardDaemonExitCh = make(chan bool)
	kv.cleanShardDaemonExitCh = make(chan bool)
	go kv.applyDaemon()
	go kv.configDaemon()
	go kv.pullShardDaemon()
	go kv.cleanShardDaemon()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
