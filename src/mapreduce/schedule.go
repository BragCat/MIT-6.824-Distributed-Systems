package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	taskChan := make(chan DoTaskArgs, ntasks)
	doneChan := make(chan int, ntasks)

	go genTask(jobName, mapFiles, phase, ntasks, n_other, taskChan)
	go dispatch(registerChan, taskChan, doneChan)

	doneTaskCnt := 0
	for {
		doneTaskCnt += <- doneChan
		if (doneTaskCnt == ntasks) {
			break
		}
	}

	close(taskChan)
	close(doneChan)

	fmt.Printf("Schedule: %v done\n", phase)
}

func genTask(jobName string, mapFiles []string, phase jobPhase, ntasks, n_other int, taskChan chan DoTaskArgs) {
	for i := 0; i < ntasks; i++ {
		task := DoTaskArgs {jobName, "", phase, i, n_other}
		if phase == mapPhase {
			task.File = mapFiles[i]
		}
		taskChan <- task
	}
}

func dispatch(registerChan chan string, taskChan chan DoTaskArgs, doneChan chan int) {
	for workerAddr := range registerChan {
		go startWorker(workerAddr, taskChan, doneChan)
	}
}

func startWorker(workerAddr string, taskChan chan DoTaskArgs, doneChan chan int) {
	for task := range taskChan {
		success := call(workerAddr, "Worker.DoTask", task, nil)
		if success {
			doneChan <- 1
		} else {
			taskChan <- task
		}
	}
}
