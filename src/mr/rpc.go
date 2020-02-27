package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskAck struct {
	Name string
	Type TaskType
	Output []string
}

type NextTask struct {
	Name string
	Type TaskType
	Input []string
	NReduce int
}

// Add your RPC definitions here.

