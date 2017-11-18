# KV-Raft

标签（空格分隔）： MIT6.824

---

这节的作业是基于Raft构建一个类似于zookeeper的强一致性kv系统，主要难点有三个：
1. 如何避免相同日志多次apply
2. 如何宕机重启
3. 如何错误恢复

## 基于Raft构建强一致性的kv系统
client提交的command被Raft库apply多次，这是无法避免的。这节的难点就在于如何在Raft库之上避免重复apply
重复apply原因：client附加日志到leader,由于网络原因超时没返回，client重新附加日志，可能导致两条日志都commit成功

### 整体架构
![kv_model.png-32.3kB][1]
1. client：用户可以创建任意多个，用于提交请求
2. kvStateMachine：用于apply下层raft commit的日志到状态机中，同时与client交互
3. Raft：一致性协议
4. log：用于持久化Raft协议必要信息，和压缩的日志

### Client RPC
为了避免多次apply同一条日志，client提交的日志请求中需要有ClientId和该client的请求号：
```
type GetArgs struct {
	ClientId int64 //每台client唯一
	OpNum int64 //递增
}
```
返回的RPC需要注意是否成功，没有成功重新提交请求

### kvStateMachine结构
```
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kv map[string]string //kv state machine
	//key为clientId,value为当前已经apply的opNum
	//如果有较小的clientId, opNum从raft返回，不再apply
	opCount map[int64]int64 
	
	terms map[int]pack //用于检查附加给Raft的日志是否提交
}
```
比较核心的结构为`kv map[string]string`,`kv map[string]string`

### 核心流程
![statemachine流程.png-38.3kB][2]
client RPC server：
- 不是Leader返回错误
- 超时返回错误
- apply返回成功

logApply goroutine，阻塞在apply channel上

## 宕机重启和日志压缩
### 宕机重启
当重启一台服务器：
1. 首先启动`logApply goroutine`，
2. 重启Raft服务，Raft服务从外部存储读取日志等必要数据
3. 如果有`Snapshot`，读取快找发送给state machine
3. 回放日志，传给apply channel
4. `logApply goroutine`重新apply日志，恢复state machine

### 日志压缩
在任何时候，都可以将本机上已经apply的日志进行压缩。但是仅仅这样是不够的，因为可能某个日志apply了，但是并没有附加到所有服务器。这时需要调用`InstallSnapshot RPC`。

1. 由`state machine`发起压缩日志命令，将某个`index`之前的state machine持久化
2. `raft`库接收到请求，将`index`之前的日志抛弃
3. `Leader`进行正常的`appendEntries RPC`，直到发现机器上没有`Follower`需要的日志
4. `Leader`启动`InstallSnapshot RPC`
5. `Follower`接收`InstallSnapshot RPC`，并把快照发送给上层state machine
6. 修改相关`raft`状态

下图是论文中关于`InstallSnapshot RPC`的描述：
![屏幕快照 2017-11-18 21.22.39.png-144.8kB][3]

### 错误总结
#### 两把锁和一个channel产生deadlock
这个bug困扰我两天，这两天简直魂不守舍，然而在洗澡的时候突然想通，所以再忙也别忘记洗澡啊～

以前写C++没有接触过channel，没想到channel也会产生deadlock。
错误描述：有两把锁，分别是raft.mu和kv.mu，和一个channel叫applyCh。deadlock产生于三个goroutinue
1. goroutinue1持有kv.mu，等待raft.mu
2. goroutinue2持有raft.mu，等待发送applyCh
3. goroutinue3用于接收applyCh，然后正在等待kv.mu，没办法接收applyCh


  [1]: http://static.zybuluo.com/biterror/jy1dz77hn3iylt20xtqg4cwa/kv_model.png
  [2]: http://static.zybuluo.com/biterror/updyp64w53ex0cjwhsq5dgvw/statemachine%E6%B5%81%E7%A8%8B.png
  [3]: http://static.zybuluo.com/biterror/2mg5kzud47gop9vtzddkhg1i/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-11-18%2021.22.39.png