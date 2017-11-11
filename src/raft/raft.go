package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

const(
	FollwerState = iota
	CandidateState
	LeaderState
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	term int
	l []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int
	votedFor int
	log []Log

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex []int
	matchIndex []int

	//extend
	state int
	stateCh chan int
	heartBeatTimer *time.Timer
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LeaderState {
		isleader = true
	}else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries [][]byte
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("PeerId %d receive append entries\n", rf.me)
	if rf.state == FollwerState{
		rf.ResetHeartBeatTimer()
	}
	if args.Term > rf.currentTerm{
		rf.SetCurrentTerm(args.Term)
		rf.SetVoteFor(-1)
		DPrintf("AppendEnties peerID %d become Follower\n",rf.me)
		rf.stateCh <- FollwerState
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	timeout     bool
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if rf.state == FollwerState{
		rf.ResetHeartBeatTimer()
	}

	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.SetCurrentTerm(args.Term)
		rf.SetVoteFor(-1)
		DPrintf("RequestVote peerId %d become Follower\n", rf.me)
		rf.stateCh <- FollwerState
	}

	if (rf.GetVoteFor() == -1 || rf.me == args.CandidateId) &&
		args.LastLogIndex == len(rf.log) - 1 &&
		args.LastLogTerm == rf.log[len(rf.log) - 1].term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.SetVoteFor(args.CandidateId)
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	DPrintf("PeerId %d vote for %d", rf.me, rf.GetVoteFor())
	DPrintf("PeerId %d reply PeerId %d: term is %d, vote is %p", rf.me, args.CandidateId, rf.currentTerm, reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		reply.timeout = false
	}else {
		reply.timeout = true
	}
	return ok
}

func (rf *Raft) raftService(msg ApplyMsg) {
	//var nAppend int

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	index = len(rf.log)
	term = rf.GetCurrentTerm()
	if rf.state == LeaderState {
		isLeader = true
		go func() {
			for msg := range rf.applyCh {
				go rf.raftService(msg)
			}
		}()
	}else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FollwerState
	rf.stateCh = make(chan int)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0].term = 0
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.StateMachine()

	return rf
}


func (rf *Raft) GetCurrentTerm() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}


func (rf *Raft) SetVoteFor(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = v
}

func (rf *Raft) GetVoteFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) CreateHeartBeatTimer() {
	DPrintf("peerId %d Create Timer\n", rf.me)
	rf.heartBeatTimer = time.NewTimer(gettimeout() * time.Millisecond)

	go func(){
		for {
			<-rf.heartBeatTimer.C
			DPrintf("peerId %d Follwer state send CandidateState\n", rf.me)
			rf.stateCh <- CandidateState
		}
	}()
}

func gettimeout() time.Duration {
	n := rand.Intn(150) + 400
	return time.Duration(n)
}

func (rf *Raft) ResetHeartBeatTimer() {
	DPrintf("peerId %d Reset Timer\n", rf.me)
	rf.heartBeatTimer.Reset(gettimeout() * time.Millisecond)
}

func (rf *Raft) StopHeartBeatTimer() {
	DPrintf("peerId %d Stop Timer\n", rf.me)
	rf.heartBeatTimer.Stop()
}

func (rf *Raft) StateMachine(){
	for {
		DPrintf("peerId %d StateMachine state is %d, term is %d\n", rf.me, rf.state, rf.currentTerm)

		switch rf.state {
		case FollwerState:
			if rf.heartBeatTimer == nil {
				rf.CreateHeartBeatTimer()
			}else {
				rf.ResetHeartBeatTimer()
			}

			rf.state = <-rf.stateCh
		case CandidateState:
			rf.SetVoteFor(-1)
			rf.SetCurrentTerm(rf.GetCurrentTerm() + 1)
			rf.StopHeartBeatTimer()
			go time.AfterFunc(gettimeout() * time.Millisecond, func() {
				if rf.state == CandidateState {
					rf.stateCh <- CandidateState
				}
			})

			go rf.AtCandidate()

			rf.state = <-rf.stateCh

		case LeaderState:
			rf.StopHeartBeatTimer()
			go rf.AtLeader()
			rf.state = <- rf.stateCh
		default:
			DPrintf("raft.go StateMachine default error\n")
		}
	}
}

func (rf *Raft) InitIPeerEntries(appendEntriesArgs *AppendEntriesArgs, i int) {
	appendEntriesArgs.Term = rf.currentTerm
	appendEntriesArgs.LeaderId = rf.me
	appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
	appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].term
	appendEntriesArgs.LeaderCommit = rf.commitIndex

	logTag := len(rf.log)
	for j := rf.nextIndex[i]; j < logTag; j++ {
		appendEntriesArgs.Entries = append(appendEntriesArgs.Entries, rf.log[j].l)
	}
}

func (rf *Raft) AtLeader() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = len(rf.log)
		}else {
			rf.nextIndex[i] = 1
			rf.matchIndex[i] = 1
		}
	}

	//heartbeat
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		appendEntriesArgs := new(AppendEntriesArgs)
		//init
		rf.InitIPeerEntries(appendEntriesArgs, i)

		appendEntriesReply := new(AppendEntriesReply)
		appendEntriesReply.Success = false
		appendEntriesReply.Term = -1

		ok := rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply)
		if ok {
			if appendEntriesReply.Success {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = len(rf.log)
			} else {
				if appendEntriesReply.Term > rf.currentTerm {
					DPrintf("RequestVote peerId %d become Follower\n", rf.me)
					rf.stateCh <- FollwerState
				}else {
					//
				}
			}
		}
	}
	time.Sleep((gettimeout() * time.Millisecond) / 2)
	rf.stateCh <- LeaderState
}

func (rf *Raft) AtCandidate() {
	var requestVoteArgs RequestVoteArgs
	requestVoteReplys := make([]RequestVoteReply, len(rf.peers))
	requestVoteArgs.Term = rf.currentTerm
	requestVoteArgs.CandidateId = rf.me
	requestVoteArgs.LastLogTerm = rf.log[len(rf.log) - 1].term
	requestVoteArgs.LastLogIndex = len(rf.log) - 1

	replyCh := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		requestVoteReplys[i].timeout = true
		go func(i int) {
			ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReplys[i])
			if replyCh != nil{
				replyCh <- ok
			}
		}(i)
	}

	for i := 0; i < len(rf.peers); i++{
		ok := <-replyCh
		if !ok {
			continue
		}

		var voteSuccess int
		for i := 0; i < len(requestVoteReplys); i++ {
			if !requestVoteReplys[i].timeout && requestVoteReplys[i].VoteGranted && requestVoteReplys[i].Term == rf.currentTerm{
				voteSuccess++
				//DPrintf("peerId %d, peer %d vote success\n",rf.me, i)
			}

			if requestVoteReplys[i].Term > rf.GetCurrentTerm() {
				rf.SetCurrentTerm(requestVoteReplys[i].Term)
				rf.stateCh <- FollwerState
				return
			}
		}
		//DPrintf("peerId %d term %d, vote success number is %d!!!!!!!!!!!!!!!!\n", rf.me, rf.currentTerm, voteSuccess)

		if voteSuccess == len(requestVoteReplys) / 2 + 1 {
			rf.stateCh <- LeaderState
		}
	}
	close(replyCh)
}
