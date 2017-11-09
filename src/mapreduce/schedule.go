package mapreduce

import (
	"fmt"
	"log"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	for i := 0; i < ntasks; i++ {
		work := <-registerChan
		ret := false
		switch phase {
		case mapPhase:
			ret = call(work, "Worker.DoTask",
				DoTaskArgs{jobName, mapFiles[i], mapPhase, i, n_other},
				nil)
		case reducePhase:
			ret = call(work, "Worker.DoTask",
				DoTaskArgs{jobName, "", reducePhase, i, n_other},
				nil)
		}
		if ret == true{
			go func(w string){
				registerChan<- w
			}(work)
		}else{
			log.Printf("Task %d error\n", i)
			i--
		}
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
