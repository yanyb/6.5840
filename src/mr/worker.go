package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := AskForTaskArgs{}
		reply := AskForTaskReply{}
		ok := call("Coordinator.AskForTask", &args, &reply)
		if !ok {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if reply.Done {
			break
		}
		if reply.Task == nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		task := reply.Task
		if task.TaskType == TaskTypeMap {
			intermediate := []KeyValue{}
			for _, filename := range task.InputFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := os.ReadFile(filename)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}

			tmpFileMap := map[int]*os.File{}
			tmpEncMap := map[int]*json.Encoder{}
			for _, kv := range intermediate {
				hash := ihash(kv.Key)%task.NumReduce + 1
				file, exist := tmpFileMap[hash]
				if !exist {
					tmpFile, err := os.CreateTemp(".", "mr-")
					if err != nil {
						log.Fatalf("cannot create file %v", err)
					}
					tmpFileMap[hash] = tmpFile
					file = tmpFile
					enc := json.NewEncoder(file)
					tmpEncMap[hash] = enc
				}

				enc := tmpEncMap[hash]
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("encode %v", err)
				}
			}
			var outputFiles []string
			for hash, file := range tmpFileMap {
				file.Close()
				outputFile := fmt.Sprintf("mr-%d-%d", task.TaskId, hash)
				outputFiles = append(outputFiles, outputFile)
				err := os.Rename(file.Name(), outputFile)
				if err != nil {
					log.Fatalf("rename %v", err)
				}
			}

			task.OutputFiles = outputFiles
			fArgs := FinishTaskArgs{
				Task: task,
			}
			fReply := FinishTaskReply{}
			call("Coordinator.FinishTask", &fArgs, &fReply)

		} else {
			intermediate := []KeyValue{}
			for _, filename := range task.InputFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err = dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			file, err := os.CreateTemp(".", "mr-out-")
			if err != nil {
				log.Fatalf("cannot create temp file %v", err)
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
			err = os.Rename(file.Name(), outputFile)
			if err != nil {
				log.Fatalf("rename %v", err)
			}

			task.OutputFiles = []string{outputFile}
			fArgs := FinishTaskArgs{
				Task: task,
			}
			fReply := FinishTaskReply{}
			call("Coordinator.FinishTask", &fArgs, &fReply)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
