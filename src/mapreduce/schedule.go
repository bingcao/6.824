package mapreduce

import (
	"fmt"
	"sync"
)

type SafeArray struct {
	arr  []int
	lock sync.Mutex
}

func (a SafeArray) length() int {
	a.lock.Lock()
	defer a.lock.Unlock()
	return len(a.arr)
}

func (a SafeArray) getNext() int {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.arr[0]
}

func (a *SafeArray) deleteValue(v int) {
	a.lock.Lock()
	for i := 0; i < len(a.arr); i++ {
		if v == a.arr[i] {
			a.arr = append(a.arr[:i], a.arr[i+1:]...)
			break
		}
	}
	a.lock.Unlock()
}

func (a *SafeArray) addValue(v int) {
	a.lock.Lock()
	a.arr = append(a.arr, v)
	a.lock.Unlock()
}

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
	tasks := SafeArray{arr: make([]int, ntasks, 2*ntasks)}
	for i := 0; i < ntasks; i++ {
		tasks.arr[i] = i
	}

	/*taskChan := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()
	successes := 0
	for successes < ntasks {*/
	for tasks.length() > 0 {
		next := tasks.getNext()
		tasks.deleteValue(next)
		/**		next := <-taskChan
		if next == -1 {
			successes++
			continue
		} **/
		freeWorker := <-mr.registerChannel
		taskNum := next
		go func() {
			task := DoTaskArgs{mr.jobName, mr.files[taskNum], phase, taskNum, nios}
			if call(freeWorker, "Worker.DoTask", &task, new(struct{})) {
				//taskChan <- -1
				mr.registerChannel <- freeWorker
			} else {
				//taskChan <- taskNum
				tasks.addValue(taskNum)
			}
		}()
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
