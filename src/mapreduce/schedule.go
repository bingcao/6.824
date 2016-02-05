package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	successes := 0
	taskChan := make(chan int)
	lock := &sync.Mutex{}

	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()

	for next := range taskChan {
		freeWorker := <-mr.registerChannel
		go func(taskNum int) {
			task := DoTaskArgs{mr.jobName, mr.files[taskNum], phase, taskNum, nios}
			if call(freeWorker, "Worker.DoTask", &task, new(struct{})) {
				lock.Lock()
				successes++
				if successes == ntasks {
					close(taskChan)
				}
				lock.Unlock()
			} else {
				taskChan <- taskNum
			}
			mr.registerChannel <- freeWorker
		}(next)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
