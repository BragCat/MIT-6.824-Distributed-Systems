package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT 			OperationType 	= "PUT"
	APPEND 			OperationType 	= "APPEND"

	OK 						Err 	= "OK"
	ErrNoKey 				Err 	= "ErrNoKey"
	ErrRetry				Err		= "ErrRetry"
	ErrWrongGroup 			Err 	= "ErrWrongGroup"
	ErrWrongLeader 			Err		= "ErrWrongLeader"
)

type Err string
type OperationType string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   		string
	Value 		string
	Op    		OperationType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CkId		int64
	ShardId		int
	Sequence	int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key 		string
	// You'll have to add definitions here.
	CkId		int64
	ShardId		int
	Sequence	int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
