package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import (
	"log"
	"bytes"
	"time"
	"sync/atomic"
)

var Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType int
	Args [2]interface{}
	ClientId int64
	OpNum int64
}


func (op *Op) DoTask(kv *ShardKV) (Err, interface{}){
	var ret interface{}
	var err Err
	switch op.OpType {
	case GetOp:
		DPrintf("GetOp")
		if kv.config == nil {
			err = ErrWrongGroup
			break
		}

		key := op.Args[0].(string)
		shard := key2shard(key)
		gid := kv.config.Shards[shard]
		if gid != kv.gid{
			err = ErrWrongGroup
			break
		}

		value, ok := kv.kvs[shard][key]
		if !ok {
			err = ErrNoKey
		}else {
			err = OK
			ret = value
		}

	case PutOp:
		DPrintf("PutOp")
		if kv.config == nil {
			err = ErrWrongGroup
			break
		}
		key := op.Args[0].(string)
		value := op.Args[1].(string)
		shard := key2shard(key)
		gid := kv.config.Shards[shard]
		if gid != kv.gid{
			err = ErrWrongGroup
			break
		}
		kv.kvs[shard][key] = value
		err = OK
		DPrintf("doTask put key:%v, value:%v, current kv", key, value, kv.kvs)

		ret = nil
	case AppendOp:
		DPrintf("AppendOp")
		if kv.config == nil {
			err = ErrWrongGroup
			break
		}
		key := op.Args[0].(string)
		value := op.Args[1].(string)
		shard := key2shard(key)
		gid := kv.config.Shards[shard]
		if gid != kv.gid{
			err = ErrWrongGroup
			break
		}
		v := kv.kvs[shard][key]
		kv.kvs[shard][key] = v + value
		err = OK
		ret = nil
	case PullDataOp:
		DPrintf("PullDataOp")
		kvs := map[int]map[string]string{}
		data1 := op.Args[0].([]byte)
		data2 := op.Args[1].([]byte)
		shards := []int{}
		var config shardmaster.Config

		r := bytes.NewBuffer(data1)
		d := gob.NewDecoder(r)
		d.Decode(&shards)

		r = bytes.NewBuffer(data2)
		d = gob.NewDecoder(r)
		d.Decode(&config)

		for _, shard := range shards {
			keyvalue := kv.kvs[shard]
			if len(keyvalue) != 0 {
				kvs[shard] = make(map[string]string)
				for key, value := range keyvalue{
					kvs[shard][key] = value
				}
			}
		}

		err = OK
		ret = kvs
	case ReConfigOp:
		DPrintf("ReConfigOp")
		data1 := op.Args[0].([]byte)
		data2 := op.Args[1].([]byte)
		newKvs := [shardmaster.NShards]map[string]string{}
		var config shardmaster.Config

		r := bytes.NewBuffer(data1)
		d := gob.NewDecoder(r)
		d.Decode(&newKvs)

		r = bytes.NewBuffer(data2)
		d = gob.NewDecoder(r)
		d.Decode(&config)

		if kv.config == nil || kv.config.Num < config.Num{
			for i, keyvalue := range newKvs {
				for key, value := range keyvalue {
					kv.kvs[i][key] = value
				}
			}
			kv.config = &config
		}
		DPrintf("gid:%d, new Config is %v, new kv %v",kv.gid, kv.config, kv.kvs)
		err = OK
		ret = nil
	default:
		DPrintf("shardkv Dotask default error")
	}

	return err, ret
}

type ShardKV struct {
	mu           sync.Mutex
	reConfigMu     sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	kvs [shardmaster.NShards]map[string]string
	terms map[int]pack
	opCount map[int64]int64
	persister *raft.Persister
	mck *shardmaster.Clerk
	config *shardmaster.Config
	ticker *time.Ticker
	
	currentOpNum int64 //client op num
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

const timeout = 100

func (kv *ShardKV) DoCall(op *Op) doCallReply {
	var reply doCallReply

	kv.mu.Lock()
	//DPrintf("kv:%d, docall lock 1", kv.me)
	index, _, isLeaader := kv.rf.Start(*op)

	if isLeaader {
		DPrintf("docallreply is leader, peerId:%d , op is %v", kv.me, op.OpType)
		kv.terms[index] = pack{op, false, Err(""), ""}
	}
	kv.mu.Unlock()
	//DPrintf("kv:%d, docall lease 1", kv.me)

	if isLeaader {
		t := time.NewTicker(timeout * time.Millisecond)
		i := 0
		for {
			<- t.C
			i++
			bFlag := false
			//DPrintf("tiemrtiemrtiemr")
			kv.mu.Lock()
			//DPrintf("kv:%d, docall lock 2", kv.me)
			p := kv.terms[index]

			//timeout
			if i == 10 {
				DPrintf("docallreply is timeout, peerId:%d, op is %v",kv.me, op.OpType)
				reply.err = OK
				reply.isLeader = false
				delete(kv.terms, index)
				bFlag = true
			}else if p.runed {
				reply.err = p.err
				reply.isLeader = true
				reply.ret = p.value
				if p.err == Err("err leader") {
					reply.isLeader = false
					reply.err = OK
					//DPrintf("peerId is %d, reply addr is %x", kv.me, unsafe.Pointer(reply))
				}
				delete(kv.terms, index)
				//DPrintf("peerId is %d, putappend reply is %v, kv is %v", kv.me,reply, kv.kvs)

				bFlag = true
			}

			kv.mu.Unlock()
			//DPrintf("kv:%d, docall lease 2", kv.me)
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

const (
	GetOp = iota
	PutOp
	AppendOp
	PullDataOp
	ReConfigOp
)

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.reConfigMu.Lock()
	//DPrintf("Get reconfig mu lock")

	op := Op{}

	op.OpType = GetOp

	op.Args[0] = args.Key
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	//DPrintf("kv id is %d", kv.me)

	doCallReply := kv.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
		if reply.Err == OK{
			reply.Value = doCallReply.ret.(string)
		}
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	kv.reConfigMu.Unlock()
	//DPrintf("Get reconfig mu lease")
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.reConfigMu.Lock()
	//DPrintf("putappend reconfig mu lock")

	op := Op{}
	if args.Op == "Put" {
		op.OpType = PutOp
	}else {
		op.OpType = AppendOp
	}

	op.Args[0] = args.Key
	op.Args[1] = args.Value
	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	//DPrintf("kv id is %d", kv.me)

	doCallReply := kv.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	if kv.config != nil {
		DPrintf("Gid:%d, config num:%d, putappend : kvs:%v, reply:%v", kv.gid,kv.config.Num ,kv.kvs, reply)
	}

	kv.reConfigMu.Unlock()
	//DPrintf("putappend reconfig mu lease")
	return
}


func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {
	DPrintf("Gid %d, pulldata", kv.gid)
	kv.reConfigMu.Lock()
	//DPrintf("pulldata reconfig mu lock")

	op := Op{}

	op.OpType = PullDataOp

	//serialize
	//DPrintf("gid:%d, pulldata %v", kv.gid, args.Shards)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args.Shards)
	data1 := w.Bytes()

	op.Args[0] = data1

	w = new(bytes.Buffer)
	e = gob.NewEncoder(w)
	e.Encode(args.Config)
	data2 := w.Bytes()

	op.Args[1] = data2

	op.ClientId = args.ClientId
	op.OpNum= args.OpNum
	//DPrintf("kv id is %d", kv.me)

	doCallReply := kv.DoCall(&op)

	if doCallReply.isLeader {
		reply.WrongLeader = false
		reply.Err = doCallReply.err
		reply.Kvs = doCallReply.ret.(map[int]map[string]string)
	}else{
		reply.WrongLeader = true
		reply.Err = OK
	}
	DPrintf("Gid:%d, peerId:%d,  pulldata : kvs:%v, reply:%v", kv.gid, kv.me, kv.kvs, reply)
	kv.reConfigMu.Unlock()
	//DPrintf("pulldata reconfig mu lease")
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.ticker.Stop()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
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
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvs[i] = make(map[string]string)
	}
	kv.terms = make(map[int]pack)
	kv.opCount = make(map[int64]int64)
	kv.persister = persister
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = nil
	kv.ticker = time.NewTicker(250 * time.Millisecond)
	kv.currentOpNum = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.applyChannel()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.getConfigTiker(kv.ticker)

	return kv
}

func (kv *ShardKV) getConfigTiker(ticker *time.Ticker) {
	for {
		_, ok := <-ticker.C
		if !ok {
			break
		}
		kv.reConfigMu.Lock()
		//DPrintf("getConfigTiker reconfig mu lock")
		kv.ReConfig()
		kv.reConfigMu.Unlock()
		//DPrintf("getConfigTiker reconfig mu lease")
	}
}

func (kv *ShardKV) ReConfig(){
	newConfig := kv.mck.Query(-1)
	kv.mu.Lock()
	var num int
	if kv.config == nil {
		num = 0
	}else {
		num = kv.config.Num + 1
	}
	kv.mu.Unlock()
	for num <= newConfig.Num{
		cConfig := kv.mck.Query(num)
		increaseShard := getIncreaseShards(kv.gid, &cConfig, kv.config)
		var newKvs [shardmaster.NShards]map[string]string
		for i := 0; i < shardmaster.NShards; i++ {
			newKvs[i] = make(map[string]string)
		}
		if len(increaseShard) != 0{
			DPrintf("gid:%d, Config change, old config:%v, new config %v",kv.gid, kv.config, cConfig)
			kv.pulldataFromServers(increaseShard, &cConfig, &newKvs)
		}
		kv.callReconfig(&newKvs, &cConfig)
		num ++
	}

}

func (kv *ShardKV) pulldataFromServers(increaseShard []int, config *shardmaster.Config, newKvs *[shardmaster.NShards]map[string]string){
	kv.mu.Lock()
	var group map[int][]string
	if kv.config == nil {
		group = config.Groups
	}else {
		group = kv.config.Groups
	}
	kv.mu.Unlock()


	var wg sync.WaitGroup

	var mu sync.Mutex

	for gid, servers := range group{
		if gid == kv.gid {
			continue
		}

		var args PullDataArgs
		kv.mu.Lock()
		args.Shards = getIncreaseShardsByGid(gid, increaseShard, kv.config)
		//args.Shards = increaseShard
		kv.mu.Unlock()
		if len(args.Shards) == 0 {
			continue
		}
		args.Config = *config
		args.ClientId = kv.mck.GetClientId()
		args.OpNum = atomic.AddInt64(&kv.currentOpNum, 1)
		kv.pulldataFromServer(&args, servers, &wg, newKvs, &mu)
	}
}

func getIncreaseShardsByGid(gid int, increaseShards []int, config *shardmaster.Config) []int{
	if config == nil{
		return increaseShards
	}
	ret := []int{}
	for _, shard := range increaseShards {
		for s, g := range config.Shards {
			if g == gid && s == shard{
				ret = append(ret, s)
			}
		}
	}
	DPrintf("getIncreaseShardsByGid, gid %d, increaseShards %v, oldShards:%v, ret:%v", gid, increaseShards, config.Shards, ret)
	return ret
}

func (kv *ShardKV) callReconfig(newKvs *[shardmaster.NShards]map[string]string, config *shardmaster.Config){
	//reconfig
	op := Op{}

	op.OpType = ReConfigOp
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(*newKvs)
	data1 := w.Bytes()
	op.Args[0] = data1

	w = new(bytes.Buffer)
	e = gob.NewEncoder(w)
	e.Encode(*config)
	data2 := w.Bytes()
	op.Args[1] = data2

	op.ClientId = kv.mck.GetClientId()
	op.OpNum = atomic.AddInt64(&kv.currentOpNum, 1)

	kv.DoCall(&op)
}

func (kv *ShardKV) pulldataFromServer(args *PullDataArgs,
						servers []string,
							wg *sync.WaitGroup,
								newKvs *[shardmaster.NShards]map[string]string,
									mu *sync.Mutex){
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply PullDataReply
		ok := srv.Call("ShardKV.PullData", args, &reply)

		if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("Gid:%d, before Pulldata From group : kvs:%v, reply kv %v", kv.gid,kv.kvs, reply.Kvs)
			if len(reply.Kvs) != 0 {
				mu.Lock()
				for shard, keyvalue := range reply.Kvs {
					for key, value := range keyvalue {
						newKvs[shard][key] = value
					}
				}
				mu.Unlock()
			}
			break
		}
	}
	//wg.Done()
}

func getIncreaseShards(gid int, newConfig *shardmaster.Config, oldConfig *shardmaster.Config) []int {
	oldShards := make([]int, 0)
	newShards := make([]int, 0)
	if oldConfig != nil {
		for shard, g := range oldConfig.Shards {
			if g == gid {
				oldShards = append(oldShards, shard)
			}
		}
	}

	if newConfig != nil {
		for shard, g := range newConfig.Shards {
			if g == gid {
				newShards = append(newShards, shard)
			}
		}
	}
	ret := make([]int, 0)
	for i, newshard := range newShards {
		for _, oldshard := range oldShards {
			if newshard == oldshard {
				newShards[i] = -1
			}
		}
	}

	for _, newshard := range newShards {
		if newshard != -1 {
			ret = append(ret, newshard)
		}
	}
	return ret
	return newShards
}

func (kv *ShardKV) applyChannel (){
	for {
		apply, ok := <-kv.applyCh
		DPrintf("applyChannel peerId %d, ok is %v", kv.me, ok)
		if ok {
			if apply.UseSnapshot {
				kv.mu.Lock()
				//DPrintf("kv:%d, applyChannel lock1", kv.me)
				r := bytes.NewBuffer(apply.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&kv.kvs)
				d.Decode(&kv.opCount)
				d.Decode(&kv.config)
				kv.mu.Unlock()
				//DPrintf("kv:%d, applyChannel lease1", kv.me)
			}else{
				kv.mu.Lock()
				//DPrintf("kv:%d, applyChannel lock2", kv.me)
				p, b := kv.terms[apply.Index]
				op := p.op

				//DPrintf("apply is %v", apply)
				//DPrintf("op is %v", op)
				command := apply.Command.(Op)
				if b {
					//run kv machine
					if kv.opCount[command.ClientId] >= command.OpNum && (op.OpType != GetOp || op.OpType != PullDataOp){
						kv.terms[apply.Index] = pack{op, true, OK, ""}
						//DPrintf("xxxxxx, %d", kv.opCount[command.ClientId])
					}else{
						//DPrintf("yyyyyy")
						error, value := command.DoTask(kv)
						if (*op).ClientId == command.ClientId && (*op).OpNum == command.OpNum{
							kv.terms[apply.Index] = pack{op, true,error, value}
						}else {
							kv.terms[apply.Index] = pack{op, true,Err("err leader"), ""}
						}
						kv.opCount[command.ClientId] = command.OpNum
					}
				}else{
					if kv.opCount[command.ClientId] < command.OpNum {
						command.DoTask(kv)
						kv.opCount[command.ClientId] = command.OpNum
					}
				}
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.rf != nil{
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.kvs)
					e.Encode(kv.opCount)
					e.Encode(kv.config)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data, apply.Index)
				}
				kv.mu.Unlock()
				//DPrintf("kv:%d, applyChannel lease2", kv.me)
			}
		}else {
			break
		}
	}
}

