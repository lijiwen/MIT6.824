package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"log"
	"sync/atomic"
)

var currentClientId int64

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeader int
	// You will have to modify this struct.
	clientId int64
	currentOpNum int64
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
	ck.lastLeader = 0

	ck.clientId = atomic.AddInt64(&currentClientId, 1)
	ck.currentOpNum = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	DPrintf("client %d get key:%v", ck.clientId, key)
	var args GetArgs
	args.Key = key
	args.ClientId = ck.clientId
	args.OpNum = atomic.AddInt64(&ck.currentOpNum, 1)


	value := ""
	for {
		var gerReply GetReply
		DPrintf("client call server %d get, init reply is %v", ck.lastLeader,GetReply{})
		ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", &args, &gerReply)

		DPrintf("ck.id:%d, ck opNum:%d, reply:%v, ok:%v",ck.clientId, args.OpNum, gerReply, ok)

		if !ok {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		if gerReply.Err != OK {
			log.Println(gerReply.Err)
		}
		if gerReply.WrongLeader != true{
			value = gerReply.Value
			break
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		//DPrintf("client call server %d putappend", ck.lastLeader)
	}
	DPrintf("client %d get key:%v success", ck.clientId, key)

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.OpNum = atomic.AddInt64(&ck.currentOpNum, 1)

	for {
		var putAppendReply PutAppendReply

		DPrintf("client call server %d putappend, init reply is %v", ck.lastLeader,putAppendReply)

		ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", &args, &putAppendReply)

		if !ok {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}

		DPrintf("ck id:%d, ck opNum:%d, reply:%v, ok:%v",ck.clientId, args.OpNum,putAppendReply, ok)

		if putAppendReply.Err != OK {
			log.Println(putAppendReply.Err)
		}
		if putAppendReply.WrongLeader != true{
			break
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
	//DPrintf("xxxxxx")
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("client %d put key:%v, value:%v", ck.clientId,key, value)
	ck.PutAppend(key, value, "Put")
	DPrintf("client %d put key:%v, value:%v success", ck.clientId,key, value)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("client %d append key:%v, value:%v",ck.clientId ,key, value)
	ck.PutAppend(key, value, "Append")
	DPrintf("client %d append key:%v, value:%v success",ck.clientId ,key, value)
}
