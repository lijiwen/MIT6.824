package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	OpNum int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	ClientId int64
	OpNum int64
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type PullDataArgs struct {
	Shards []int
	Config shardmaster.Config
	ClientId int64
	OpNum int64
	// You'll have to add definitions here.
}

type PullDataReply struct {
	WrongLeader bool
	Err         Err
	Kvs map[int]map[string]string
}
