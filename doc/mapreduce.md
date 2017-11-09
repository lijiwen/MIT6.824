# mapreduce

标签（空格分隔）： MIT6.824

---

本文梳理了mapreduce程序中的总体架构。mapreduce中主要有master和work两个模块。

## master
master对外提供RPC接口，用于worker的注册，以及RPC服务的关闭。master的结构如下：
```
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// protected by the mutex
	newCond *sync.Cond // 用于注册worker时加入workers
	workers []string   // worker的地址

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}
```
master提供服务的流程图如下：
![mapreduce_master.png-39.7kB][1]

## worker
worker作为mapreduce的slave节点，其本身作为无状态设计，用于执行具体的任务。
```
type Worker struct {
	sync.Mutex

	name       string
	Map        func(string, string) []KeyValue
	Reduce     func(string, []string) string
	nRPC       int // quit after this many RPCs; protected by mutex
	nTasks     int // total tasks executed; protected by mutex
	concurrent int // number of parallel DoTasks in this worker; mutex
	l          net.Listener
}
```
worker的执行流程图如下：
![mapreduce_worker.png-32.1kB][2]



  [1]: http://static.zybuluo.com/biterror/pirnhhlnm7mroawo765862wp/mapreduce_master.png
  [2]: http://static.zybuluo.com/biterror/ioxcrq3jeg0e3hhs6vq4ut4e/mapreduce_worker.png