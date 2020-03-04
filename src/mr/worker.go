package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import (
	"os"
	"io/ioutil"
	"time"
	"encoding/json"
	"sort"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	taskAck := &TaskAck{Name: "Nothing", Output: make([]string, 0)}
	nextTask := &NextTask{}
	call("Master.NextTask", taskAck, nextTask)

	for {
		fmt.Println("Worker receiving ", nextTask)
		switch nextTask.Type {
		case MAP:
			taskAck = runMapTask(nextTask, mapf)
		case REDUCE:
			taskAck = runReduceTask(nextTask, reducef)
		case EXIT:
			// final call of exit
			taskAck = &TaskAck{
						Name: nextTask.Name, 
						Type: nextTask.Type, 
						Output: make([]string, 0)}
			call("Master.NextTask", taskAck, nextTask)
			return
		default:
			// HEARTBEAT should reset task states
			taskAck = &TaskAck{Name: "Nothing", Output: make([]string, 0)}
			time.Sleep(5*time.Second)
		}
		fmt.Println("Worker finish ", taskAck)
		call("Master.NextTask", taskAck, nextTask)
	}
	
}

func runMapTask(task *NextTask, mapf func(string, string) []KeyValue) *TaskAck {
	
	tempFiles := make([]*os.File, task.NReduce)
	tmpfEncoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i ++ {
		/*
		   We should make temp dir in same folder. Because:
		   GoLang: os.Rename() give error "invalid cross-device link" for Docker container with Volumes.
		   MoveFile(source, destination) will work moving file between folders
		*/
		tempFiles[i],_ = ioutil.TempFile("", "map-out")
		tmpfEncoders[i] = json.NewEncoder(tempFiles[i])
	}

	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			i := ihash(kv.Key) % task.NReduce
			if err := tmpfEncoders[i].Encode(&kv); err != nil {
				return &TaskAck{}
			}
		}
	}

	taskAck := TaskAck{Name:task.Name, Type:task.Type, Output: make([]string, task.NReduce)}
	for i := 0; i < task.NReduce; i ++ {
		tempFiles[i].Close()
		mapOut := fmt.Sprintf("/data/mr-mapout/%v-%02d", task.Name, i)

		fmt.Println("Renaming... ", tempFiles[i].Name(), mapOut)
		os.Rename(tempFiles[i].Name(), mapOut)
		taskAck.Output[i] = mapOut
	}

	return &taskAck
}

func runReduceTask(task *NextTask, reducef func(string, []string) string) *TaskAck {
	intermediate := []KeyValue{}
	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, _ := ioutil.TempFile("./", "reduce-out")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	oName := "mr-out-" + task.Name
	os.Rename(ofile.Name(), oName)

	taskAck := TaskAck{Name: task.Name, Type: task.Type, Output: []string{oName} }
	return &taskAck
}



//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "/data/mr-socket")
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
