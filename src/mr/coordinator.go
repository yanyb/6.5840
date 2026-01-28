package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	l           sync.Locker
	nMap        int
	nReduce     int
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task
	mapDone     atomic.Bool
	done        atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.l.Lock()
	defer c.l.Unlock()

	if c.done.Load() {
		reply.Done = true
		return nil
	}

	if c.mapDone.Load() {
		for _, task := range c.reduceTasks {
			if task.FinishTime != 0 {
				continue
			}
			if task.StartTime == 0 || time.Now().Unix()-task.StartTime >= 10 {
				task.StartTime = time.Now().Unix()
				reply.Task = task
				break
			}
		}
	} else {
		for _, task := range c.mapTasks {
			if task.FinishTime != 0 {
				continue
			}
			if task.StartTime == 0 || time.Now().Unix()-task.StartTime >= 10 {
				task.StartTime = time.Now().Unix()
				reply.Task = task
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.l.Lock()
	defer c.l.Unlock()

	if args.Task.TaskType == TaskTypeMap {
		task := c.mapTasks[args.Task.TaskId]
		if task == nil || task.FinishTime > 0 {
			return nil
		}
		task.FinishTime = time.Now().Unix()
		task.OutputFiles = args.Task.OutputFiles

		var outPutFiles []string
		for _, mTask := range c.mapTasks {
			if mTask.FinishTime == 0 {
				return nil
			}
			outPutFiles = append(outPutFiles, mTask.OutputFiles...)
		}
		// map完成
		c.mapDone.CompareAndSwap(false, true)

		for i := 1; i <= c.nReduce; i++ {
			var inputFiles []string
			for j := 1; j <= c.nMap; j++ {
				inputFile := fmt.Sprintf("mr-%d-%d", j, i)
				for _, outPutFile := range outPutFiles {
					if outPutFile == inputFile {
						inputFiles = append(inputFiles, inputFile)
						break
					}
				}
			}

			if len(inputFiles) > 0 {
				c.reduceTasks[i] = &Task{
					TaskType:   TaskTypeReduce,
					TaskId:     i,
					NumReduce:  c.nReduce,
					InputFiles: inputFiles,
				}
			}
		}
	} else {
		task := c.reduceTasks[args.Task.TaskId]
		if task == nil || task.FinishTime > 0 {
			return nil
		}
		task.FinishTime = time.Now().Unix()
		task.OutputFiles = args.Task.OutputFiles

		for _, rTask := range c.reduceTasks {
			if rTask.FinishTime == 0 {
				return nil
			}
		}
		// 全部完成
		c.done.CompareAndSwap(false, true)
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	ret = c.done.Load()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.l = &sync.Mutex{}
	c.nReduce = nReduce
	c.mapTasks = map[int]*Task{}
	c.reduceTasks = map[int]*Task{}
	for i, file := range files {
		c.nMap++
		c.mapTasks[i+1] = &Task{
			TaskType:   TaskTypeMap,
			TaskId:     i + 1,
			NumReduce:  nReduce,
			InputFiles: []string{file},
		}
	}

	c.server()
	return &c
}
