package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
const (
	TaskTypeMap    int = 1
	TaskTypeReduce int = 2
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	TaskType    int
	TaskId      int
	NumReduce   int
	InputFiles  []string
	OutputFiles []string
	StartTime   int64
	FinishTime  int64
}

// Add your RPC definitions here.
type AskForTaskArgs struct {
}

type AskForTaskReply struct {
	Task *Task
	Done bool
}

type FinishTaskArgs struct {
	Task *Task
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
