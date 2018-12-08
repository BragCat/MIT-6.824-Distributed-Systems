package shardmaster

import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) shuffle() {
	groupNum := len(config.Groups)
	if groupNum == 0 {
		config.Shards = [NShards]int{}
		return
	}
	shardsPerGroup := NShards / groupNum
	extraShards := NShards % groupNum
	DPrintf("[Config]: {%v} start shuffle, shardsPerGroup = %v, extraShards = %v.",
		*config, shardsPerGroup, extraShards)

	shuffleShards := make([]int, 0)
	groupLoad := make(map[int]int)
	for shard, gid  := range config.Shards {
		_, exist := config.Groups[gid]
		if exist {
			if groupLoad[gid] < shardsPerGroup {
				groupLoad[gid]++
			} else if groupLoad[gid] == shardsPerGroup {
				if extraShards > 0 {
					extraShards--
					groupLoad[gid]++
				} else {
					shuffleShards = append(shuffleShards, shard)
				}
			} else {
				shuffleShards = append(shuffleShards, shard)
			}
		} else {
			shuffleShards = append(shuffleShards, shard)
		}
	}

	DPrintf("[Config]: shuffleShards = %v, groupLoad = %v", shuffleShards, groupLoad)

	for _, shard := range shuffleShards {
		for gid := range config.Groups {
			if groupLoad[gid] < shardsPerGroup {
				config.Shards[shard] = gid
				groupLoad[gid]++
				break
			} else if groupLoad[gid] == shardsPerGroup && extraShards > 0 {
				config.Shards[shard] = gid
				groupLoad[gid]++
				extraShards--
				break
			}
		}
	}
	DPrintf("[Config]: {%v} finish shuffle.", *config)
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers 	map[int][]string // new GID -> servers mappings
	CkId 		int64
	Sequence 	int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs 		[]int
	CkId 		int64
	Sequence 	int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard 		int
	GID   		int
	CkId 		int64
	Sequence 	int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num 		int // desired config number
	CkId 		int64
	Sequence 	int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
