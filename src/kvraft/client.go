package raftkv

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderId int
	id int64
	sequence int
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeaderId = 0
	ck.id = nrand()
	ck.sequence = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{
		Key: key,
		CkId: ck.id,
		Sequence: ck.sequence,
	}
	ck.sequence++

	DPrintf("[client %v]: send GetArgs %v", ck.id, args)

	nServer := len(ck.servers)
	for i := ck.lastLeaderId; ; {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader && reply.Err == "" {
			ck.lastLeaderId = i
			return reply.Value
		}
		i = (i + 1) % nServer
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		CkId: ck.id,
		Sequence: ck.sequence,
	}
	ck.sequence++

	DPrintf("[client %v]: send PutAppendArgs %v", ck.id, args)

	nServer := len(ck.servers)
	for i := ck.lastLeaderId; ; {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader && reply.Err == "" {
			ck.lastLeaderId = i
			return
		}
		i = (i + 1) % nServer
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
