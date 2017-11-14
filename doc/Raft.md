# Raft

标签（空格分隔）： MIT6.824

---

## Raft基础
首先介绍一下Raft基础，Raft中每台机器有三种状态，分别是：`Follower`,`Candidate`,`Leader`
这三种状态的状态机如下图，理解这三种状态的变迁对理解Raft至关重要

![屏幕快照 2017-11-11 18.13.23.png-185.2kB][1]

### Raft协议相关变量
![屏幕快照 2017-11-12 10.53.30.png-217.4kB][2]
分为三类：
- Persistent state on all servers
- Volatile state on all servers
- Volatile state on leaders

```
type Raft struct {
	//persistent state on all servers
	currentTerm int 
	votedFor int //每次更新currentTerm需要把votedFor置为null
	log []Log //first index is 1
	me int    // this peer's id


	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex []int //初始化为leader len(log)
	matchIndex []int //初始化为0
	
	//extend
	state int
	stateCh chan int
	heartBeatTimer *time.Timer
	applyCh chan ApplyMsg //apply send to client

	raftData interface{}
}
```
### 请求投票RPC
![屏幕快照 2017-11-12 11.03.13.png-139.3kB][3]
RequestVote有两条规则：
1. Reply false if term < currentTerm
2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
### 附加日志RPC
Raft中没有特殊的心跳RPC，统一作为附加日志RPC
![屏幕快照 2017-11-12 11.14.38.png-159.3kB][4]
AppendEntries规则：
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)

### 所有Server的相关规则
#### All server遵守
1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
2. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

#### Followers遵守
1. Respond to RPCs from candidates and leaders
2. if election timeout elapses without receiving AppendEntries
RPC from current leader or granting vote to candidate: convert to candidate（心跳定时器）

#### Candidates遵守
1. On conversion to candidate, start election:
    1. Increment currentTerm
    2. Vote for self
    3. Reset election timer
    4. Send RequestVote RPCs to all other servers
2. If votes received from majority of servers: become leader（所以需要异步统计）
3. If AppendEntries RPC received from new leader: convert to
follower（如何判定它是一个新leader?）
4. If election timeout elapses: start new election

#### Leaders遵守
1. Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2) （发送心跳）
2. If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
3. If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    1. If successful: update nextIndex and matchIndex for
follower (§5.3)
    2. If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
4. If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

## 代码框架
写完代码，调通所有bug后，最大的感触就是论文中的RPC描述真是字字珠玑，本想着某种实现和论文中等价，等到测试时却是错的。下面介绍一下在完成这个作业时的一些思路。
Raft协议主要分为以下几个部分：
1. 状态机
2. 发送及接受AppendEntries RPC
3. 发送及接受RequestVote RPC
4. 相关定时器

### 状态机

写程序第一件事情就是要确定代码的整体框架是什么，整体框架就是代码的骨架，确定后才能往上面添加功能。因为以前看过ZooKeeper ZAB协议的源码，所以知道分布式一致性协议的核心就是状态机的变迁。
下面是Raft的状态机：
```
func (rf *Raft) stateMachine(){
	for {
		//DPrintf("peerId %d stateMachine state is %d, Term is %d\n", rf.me, rf.state, rf.currentTerm)

		switch rf.state {
		case FollowerState:
		    //第一次进入状态机，创建定时器，否则重置定时器
			if rf.heartBeatTimer == nil {
				rf.createHeartBeatTimer()
			}else {
				rf.resetHeartBeatTimer()
			}

			rf.state = <-rf.stateCh
		case CandidateState:
			rf.mu.Lock()
			rf.updateNewTerm(rf.currentTerm + 1)
			rf.mu.Unlock()
			rf.stopHeartBeatTimer()
			//超时后，如果还在Candidate状态，再次请求投票
			go time.AfterFunc(gettimeout() * time.Millisecond, func() {
				if rf.state == CandidateState {
					rf.stateCh <- CandidateState
				}
			})

			go rf.atCandidate()

			rf.state = <-rf.stateCh

		case LeaderState:
			rf.stopHeartBeatTimer()
			//不断发起AppendEntries RPC
			go rf.atLeader()
			rf.state = <- rf.stateCh
		default:
			//DPrintf("raft.go stateMachine default error\n")
		}
	}
}
```
有几个关键点：
- 外层循环阻塞在状态变化的channel上
- 只有`Follower`，才需要心跳定时器，其他状态需要关闭定时器
- 选举前需要增加currentTerm，并重置voteFor

### 发送及接受RequestVote RPC
#### 发送RequestVote RPC
发送部分部分函数名为atCandidate
```
func (rf *Raft) atCandidate() {
	rf.mu.Lock()
	var requestVoteArgs RequestVoteArgs
	requestVoteReplys := make([]RequestVoteReply, len(rf.peers))
	requestVoteArgs.Term = rf.currentTerm
	requestVoteArgs.CandidateId = rf.me
	requestVoteArgs.LastLogTerm = rf.log[len(rf.log) - 1].Term
	requestVoteArgs.LastLogIndex = len(rf.log) - 1
	rf.mu.Unlock()
    
    //采用异步接受RPC的模式，每次受到reply，触发channel
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

    //我的处理逻辑是给自己发RequestVote肯定会同意，所以一共有len(rf.peers)条reply
	for i := 0; i < len(rf.peers); i++{
		ok := <-replyCh
		if !ok {
			continue
		}

        //统计该选票
		var voteSuccess int
		for i := 0; i < len(requestVoteReplys); i++ {
			if !requestVoteReplys[i].timeout && requestVoteReplys[i].VoteGranted && requestVoteReplys[i].Term == rf.currentTerm{
				voteSuccess++
			}
            //如果发现Reply Term更大，回到Follower状态
			if requestVoteReplys[i].Term > rf.getCurrentTerm() {
				rf.mu.Lock()
				rf.updateNewTerm(requestVoteReplys[i].Term)
				rf.mu.Unlock()
				rf.stateCh <- FollowerState
				rf.persist()
			}
		}
		
		//过半同意，变为leader状态，并初始化nextIndex[]，machineIndex[]
		if voteSuccess == len(requestVoteReplys) / 2 + 1 {
			//initial leader
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log) - 1
				}else {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
			rf.stateCh <- LeaderState
		}
	}
	close(replyCh)
}
```
遵守的条约：

1. On conversion to candidate, start election:
    1. Increment currentTerm
    2. Vote for self
    3. Reset election timer
    4. Send RequestVote RPCs to all other servers
2. If votes received from majority of servers: become leader（所以需要异步统计）
3. If election timeout elapses: start new election
4. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
#### 接收RequestVote RPC
```
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == FollowerState {
		rf.resetHeartBeatTimer()
	}

	//Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
    //If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
	if rf.currentTerm < args.Term {
		rf.updateNewTerm(args.Term)
		//DPrintf("RequestVote peerId %d become Follower\n", rf.me)
		rf.stateCh <- FollowerState
	}

	logIndexSelf := len(rf.log) - 1

	//判定是否更新的规则
	//1. 相同term，index更大为新
	//2. term越大越新
	if (rf.votedFor == -1 || rf.me == args.CandidateId) &&
		isNewest(args.LastLogTerm, args.LastLogIndex, rf.log[logIndexSelf].Term, logIndexSelf){
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.persist()
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}
```

遵守的条约：
1. Reply false if term < currentTerm 
2. If votedFor is null or candidateId, and candidate’s log is at
least as up-to-date as receiver’s log, grant vote 
3. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
#### 犯过的那些错
1. 只有`Follower`才需要重置心跳定时器，否则其他状态重置超时后又进入`Candidate`造成重复选举
2. 注意返回`true`的四个条件
    1. 当前term没有投票，votedFor为-1；或者自己就是Candidate
    2. Candidate的日志至少和自己一样新

### 发送和接收AppendEntries RPC
#### 发送AppendEntries RPC
在两个地方发送AppendEntries RPC：
1. Leader状态下定时发送心跳
2. `client`发来新的请求，leader附加日志后同步

向all servers发送AppendEntries RPC的入口为`sendAppendEntriesToAllServer`
```
func (rf *Raft) sendAppendEntriesToAllServer () {
	rf.mu.Lock()
	l := len(rf.peers)
	for i := 0; i < l; i++ {
	    //自己已经附加过日志，不用再次发送
		if i == rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = len(rf.log) - 1
			continue
		}
		appendEntriesArgs := new(AppendEntriesArgs)
		rf.initIPeerEntries(appendEntriesArgs, i)

		appendEntriesReply := new(AppendEntriesReply)
		appendEntriesReply.Success = false
		appendEntriesReply.Term = -1

        //在goroutine中发送
		go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply)
	}
	rf.mu.Unlock()
}
```

下面是发送到某个server的函数`sendAppendEntries`
```
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
net:
	//if we don't check this, may follower state send appenentries RPC, cause error
	if rf.state != LeaderState {
		//rf.mu.Unlock()
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	if ok {
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			//检查是否可以commit
			rf.checkCanCommited()
		} else {
			if reply.Term > rf.currentTerm {
			    //发现更大的term，改变状态为Follower
				rf.updateNewTerm(reply.Term)
				rf.stateCh <- FollowerState
				rf.persist()
			}else {
				if (rf.nextIndex[server] > 1){
					rf.nextIndex[server]--
				}
				rf.initIPeerEntries(args, server)
				rf.mu.Unlock()
				goto net
			}
		}
	}

	rf.mu.Unlock()
}
```
遵守的条约：

1. Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts 
2. If command received from client: append entry to local log, respond after entry applied to state machine
3. If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    1. If successful: update nextIndex and matchIndex for
follower (§5.3)
    2. If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
4. If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (`checkCanCommited`)

#### 接收AppendEntries RPC
```
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    //同样，只有在Follower，才重置定时器
	if rf.state == FollowerState {
		rf.resetHeartBeatTimer()
	}
	
	//收到更大的term，改变状态
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.stateCh <- FollowerState
	}
    //Reply false if term < currentTerm 
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	//Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm
	if len(rf.log) - 1 < args.PrevLogIndex {
		//DPrintf("222222222")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm{
		//DPrintf("33333333333")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
    //接收到新leader
	if args.LeaderId != rf.votedFor {
		reply.Term = args.Term
		reply.Success = false
		if rf.votedFor == -1 {
			rf.votedFor = args.LeaderId
		}
		rf.mu.Unlock()
		return
	}

    //1. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it 
    //2. Append any new entries not already in the log
	beginIndex := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		if i + beginIndex < len(rf.log) {
			if rf.log[i + beginIndex] != args.Entries[i]{
				rf.log[i + beginIndex] = args.Entries[i]
				rf.log = rf.log[ : i + beginIndex + 1 : i + beginIndex + 1]
			}

		}else {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

    // If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	reply.Term = rf.currentTerm
	reply.Success = true
	if len(args.Entries) != 0 {
		rf.persist()
	}

	rf.mu.Unlock()
}
```
遵守的条约：
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)
6. If AppendEntries RPC received from new leader: convert to
follower

#### 年轻时犯过的一些错
1. 发送`AppendEntries`时已经不是leader。
解决方案：在发送前判定是否是leader，但是这里没加锁，有可能被其他goroutinue改变状态，不能完全解决问题。
2. `checkCanCommit`除了要日志过半，还要只能提交`log[N].term == currentTerm`的日志
3. 最开始附加日志没有按照规定，采用`append`添加，后来发现日志可能不按照发送的先后顺序来。
4. 分区后重新加入的Follower没有votedFor，主要从新赋值
5. 一定避免耗时逻辑或者锁阻塞网络，否则可能不断引起超时重传

### 相关定时器
#### 心跳定时器
```
	go func(){
		for {
			<-rf.heartBeatTimer.C
			//DPrintf("peerId %d Follwer state send CandidateState\n", rf.me)
			rf.stateCh <- CandidateState
		}
	}()
```
#### 超时选举定时器
```
			go time.AfterFunc(gettimeout() * time.Millisecond, func() {
				if rf.state == CandidateState {
					rf.stateCh <- CandidateState
				}
			})
```
#### apply定时器
```
func (rf *Raft) createCommitTimer() {
	ticks := time.NewTicker(200 * time.Millisecond)
	for _ = range ticks.C {
		rf.mu.Lock()
		cIndex := rf.commitIndex
		for cIndex > rf.lastApplied {
			rf.lastApplied++
			log := rf.log[rf.lastApplied]
			//apply
			rf.raftData = log.L
			rf.persist()

			func(index int){
				rf.applyCh <- ApplyMsg{index, rf.log[index].L, false, nil}
			}(rf.lastApplied)
		}
		rf.mu.Unlock()
	}
}

```

### 持久化
我认为需要持久化这些对象

1. currentTerm
2. votedFor
3. rf.log
4. rf.commitIndex
5. rf.lastApplied
6. rf.raftData

## 不足
为了简单，锁的粒度太大，可以考虑降低锁的粒度。只锁数据，不锁逻辑。

  [1]: http://static.zybuluo.com/biterror/hnzqzepfiiwiq1h0u3236vn4/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-11-11%2018.13.23.png
  [2]: http://static.zybuluo.com/biterror/cuushc818lwqz8kigwkk19si/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-11-12%2010.53.30.png
  [3]: http://static.zybuluo.com/biterror/hns0hv26dvrfbtd23pkek6u9/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-11-12%2011.03.13.png
  [4]: http://static.zybuluo.com/biterror/yrmnaz6usk5miybqxstw4m7b/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-11-12%2011.14.38.png