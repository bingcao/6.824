package mapreduce

import "fmt"

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
	done := make(chan int, ntasks+1)
	taskChan := make(chan int)

	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()

	go func() {
		for successes < ntasks {
			successes += <-done
		}
		close(taskChan)
	}()

	for next := range taskChan {
		debug("next: %v\n", next)
		freeWorker := <-mr.registerChannel
		go func(worker string, taskNum int) {
			file := ""
			if phase == mapPhase {
				file = mr.files[taskNum]
			}
			task := DoTaskArgs{mr.jobName, file, phase, taskNum, nios}
			if call(worker, "Worker.DoTask", &task, new(struct{})) {
				done <- 1
				mr.registerChannel <- worker
			} else {
				taskChan <- taskNum
			}
		}(freeWorker, next)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
