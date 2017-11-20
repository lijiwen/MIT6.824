package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"bytes"
	"log"
	"time"
	"math"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	terms map[int]pack
	opCount map[int64]int64
	lastCigNum int
}

const (
	JoinOp = iota
	LeaveOp = iota
	MoveOp = iota
	QueryOp = iota
)

type Op struct {
	OpType int
	Args [2]interface{}
	ClientId int64
	OpNum int64
}
type pack struct {
	op *Op
	runed bool
	err Err
	value interface{}
}

type doCallReply struct {
	err Err
	ret interface{}
	isLeader bool
}

func (sm *ShardMaster) DoCall(op *Op) doCallReply {
	var reply doCallReply

	sm.mu.Lock()
	DPrintf("sm:%d, docall lock 1", sm.me)
	index, _, isLeaader := sm.rf.Start(*op)

	if isLeaader {
		DPrintf("Docall op is %v", *op)
		sm.terms[index] = pack{op, false, Err(""), ""}
	}
	sm.mu.Unlock()
	DPrintf("sm:%d, docall lease 1", sm.me)

	if isLeaader {
		t := time.NewTicker(timeout * time.Millisecond)
		i := 0
		for {
			<- t.C
			i++
			bFlag := false
			//DPrintf("tiemrtiemrtiemr")
			sm.mu.Lock()
			DPrintf("sm:%d, docall lock 2", sm.me)
			p := sm.terms[index]

			//timeout
			if i == 10 {
				reply.err = OK
				reply.isLeader = false
				delete(sm.terms, index)
				bFlag = true
			}else if p.runed {
				reply.err = p.err
				reply.isLeader = true
				reply.ret = p.value
				if p.err == Err("err leader") {
					reply.isLeader = false
					reply.err = OK
					//DPrintf("peerId is %d, reply addr is %x", sm.me, unsafe.Pointer(reply))
				}
				delete(sm.terms, index)
				//DPrintf("peerId is %d, putappend reply is %v, sm is %v", sm.me,reply, sm.sm)

				bFlag = true
			}

			sm.mu.Unlock()
			DPrintf("sm:%d, docall lease 2", sm.me)
			if bFlag {
				break
			}
		}
	}else{
		reply.isLeader = false
		reply.err = OK
	}
	return reply
}

const timeout = 100

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}

	op.OpType = JoinOp

	//serialize
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args.Servers)
	data := w.Bytes()

	op.Args[0] = data
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	DPrintf("sm id is %d", sm.me)

	doCallReply := sm.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{}

	op.OpType = LeaveOp

	//serialize
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args.GIDs)
	data := w.Bytes()

	op.Args[0] = data
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	DPrintf("sm id is %d", sm.me)

	doCallReply := sm.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{}

	op.OpType = MoveOp
	op.Args[0] =args.Shard
	op.Args[1] =args.GID
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	DPrintf("sm id is %d", sm.me)

	doCallReply := sm.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{}

	op.OpType = QueryOp
	op.Args[0] =args.Num
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	DPrintf("sm id is %d", sm.me)

	doCallReply := sm.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
		reply.Config = doCallReply.ret.(Config)
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	return
}


func (sm *ShardMaster) NextConfig() *Config{
	oldConfig := sm.configs[sm.lastCigNum]
	var newConfig Config
	newConfig.Num = oldConfig.Num + 1
	newConfig.Groups = map[int][]string{}
	newConfig.Shards = [NShards]int{}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for i, v := range oldConfig.Shards{
		newConfig.Shards[i] = v
	}
	sm.lastCigNum++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.lastCigNum]
}

func (sm *ShardMaster) getMinGid(c *Config) int {
	//init all gid count to 0
	counts := map[int]int{}
	for gid := range c.Groups {
		counts[gid] = 0
	}

	//each gid have n shard
	for _, gid := range c.Shards {
		_, ok := counts[gid]
		if ok {
			counts[gid] ++
		}
	}

	retGid, min := 0, math.MaxInt32
	for gid, count := range counts {
		if count < min{
			min = count
			retGid = gid
		}
	}

	return  retGid
}

func (sm *ShardMaster) getMaxGid(c *Config) int {
	//init all gid count to 0
	counts := map[int]int{}
	for gid := range c.Groups {
		counts[gid] = 0
	}

	//each gid have n shard
	for _, gid := range c.Shards {
		if gid == 0 {
			return 0
		}
		counts[gid] ++
	}

	retGid, max := 0, 0
	for gid, count := range counts {
		if count > max{
			max = count
			retGid = gid
		}
	}

	return  retGid
}

func (sm *ShardMaster) joinRebalance(gid int) {
	config := &sm.configs[sm.lastCigNum]
	for i := 0; i < NShards / len(config.Groups); i++ {
		maxGid := sm.getMaxGid(config)
		shard := sm.getShardByGid(config, maxGid)
		config.Shards[shard] = gid
	}
}

func (sm *ShardMaster) leaveRebalance(gid int) {
	config := &sm.configs[sm.lastCigNum]
	for i := 0; i < NShards; i++ {
		minGid := sm.getMinGid(config)
		shard := sm.getShardByGid(config, gid)
		if shard == -1 {
			break
		}
		config.Shards[shard] = minGid
	}
}

func (sm *ShardMaster) getShardByGid(config *Config, gid int) int {
	for s := 0; s < NShards; s++ {
		if config.Shards[s] == gid {
			return s
		}
	}
	return -1
}

func (op *Op) DoTask(sm *ShardMaster) (Err, interface{}){
	var ret interface{}
	switch op.OpType {
	case QueryOp:
		num := op.Args[0].(int)
		if num == -1 {
			ret = sm.configs[sm.lastCigNum]
		}else {
			ret = sm.configs[num]
		}

	case JoinOp:
		config := sm.NextConfig()
		data := op.Args[0].([]byte)
		servers := map[int][]string{}

		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&servers)

		DPrintf("Do task join, map %v", servers)

		for gid, serverStr := range servers {
			_, ok := config.Groups[gid]
			if !ok {
				config.Groups[gid] = serverStr
				sm.joinRebalance(gid)
			}
		}
		DPrintf("after join config %v", config)
		ret = nil
	case LeaveOp:
		config := sm.NextConfig()
		data := op.Args[0].([]byte)
		gids := []int{}

		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&gids)

		for _, gid := range gids {
			_, ok := config.Groups[gid]
			if ok {
				delete(config.Groups, gid)
				sm.leaveRebalance(gid)
			}
		}
		DPrintf("after leave gids %v, config %v",gids, config)
		ret = nil
	case MoveOp:
		shard := op.Args[0].(int)
		gid := op.Args[1].(int)

		config := sm.NextConfig()
		config.Shards[shard] = gid
		DPrintf("after move config %v", config)
	default:
		DPrintf("Dotask default error")
	}

	return OK, ret
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
}

// needed by shardsm tester
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

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	go sm.applyChannel()
	sm.terms = make(map[int]pack)
	sm.opCount = make(map[int64]int64)
	sm.lastCigNum = 0
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}

func (sm *ShardMaster) applyChannel (){
	for {
		apply, ok := <-sm.applyCh
		//DPrintf("applyChannel apply is %v, ok is %v", apply, ok)
		if ok {
			if apply.UseSnapshot {
				sm.mu.Lock()
				DPrintf("sm:%d, applyChannel lock1", sm.me)
				r := bytes.NewBuffer(apply.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&sm.configs)
				d.Decode(&sm.lastCigNum)
				d.Decode(&sm.opCount)
				sm.mu.Unlock()
				DPrintf("sm:%d, applyChannel lease1", sm.me)
			}else{
				sm.mu.Lock()
				DPrintf("sm:%d, applyChannel lock2", sm.me)
				p, b := sm.terms[apply.Index]
				op := p.op

				//DPrintf("apply is %v", apply)
				//DPrintf("op is %v", op)
				command := apply.Command.(Op)
				if b {
					//run sm machine
					if sm.opCount[command.ClientId] >= command.OpNum && op.OpType != QueryOp{
						sm.terms[apply.Index] = pack{op, true, OK, ""}
						//DPrintf("xxxxxx, %d", sm.opCount[command.ClientId])
					}else{
						//DPrintf("yyyyyy")
						error, value := command.DoTask(sm)
						if (*op).ClientId == command.ClientId && (*op).OpNum == command.OpNum{
							sm.terms[apply.Index] = pack{op, true,error, value}
						}else {
							sm.terms[apply.Index] = pack{op, true,Err("err leader"), ""}
						}
						sm.opCount[command.ClientId] = command.OpNum
					}
				}else{
					if sm.opCount[command.ClientId] < command.OpNum {
						command.DoTask(sm)
						sm.opCount[command.ClientId] = command.OpNum
					}
				}
				sm.mu.Unlock()
				DPrintf("sm:%d, applyChannel lease2", sm.me)
			}
		}else {
			break
		}
	}
}

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}