package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "fmt"
import "sync"
import "time"

type Master struct {
	// Your definitions here.
	nReduce int
	nMap int
	doneMap int
	doneReduce int

	tasks map[string]*Task

	mux sync.RWMutex

	allDone bool
}

type TaskState int
const (
	IDLE TaskState = iota
	INPROGRESS
	DONE
	NOTREADY
)

type TaskType int
const (
	MAP TaskType = iota
	REDUCE
	EXIT
	HEARTBEAT
)

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	name string
	t TaskType
	s TaskState
	files []string
}


func (m *Master) NextTask(req *TaskAck, reply *NextTask) error {
	fmt.Println("Acking... ", req)
	// Mark task DONE according to TaskAck
	if tt,ok := m.tasks[req.Name]; ok && tt.s == INPROGRESS {
		tt.s = DONE

		switch tt.t {
		case MAP:
			m.mux.Lock()
			for i, ofile := range req.Output {
				rtN := fmt.Sprintf("reduce-%02d", i)
				if rt, o := m.tasks[rtN]; o {
					m.tasks[rtN].files = append(rt.files, ofile)
				}else{
					m.tasks[rtN] = &Task{name: rtN, t: REDUCE, s: NOTREADY, files: []string{ofile}}
				}
			}
			m.doneMap++
			if m.doneMap == m.nMap && m.doneReduce == 0 {
				fmt.Println("Map all well done. Start Reducing... ")
				for _,t := range m.tasks {
					if t.t == REDUCE {
						t.s = IDLE
					}
				}
			}
			m.mux.Unlock()
			
		case REDUCE:
			m.doneReduce ++ 
			if m.doneReduce == m.nReduce {
				m.tasks["complete"] = &Task{name: "complete", t: EXIT, s:IDLE}
				m.allDone = true
			}
		}

	}

	// Get Idle task and return it to worker
	t := m.getIdleTask()
	*reply = NextTask{t.name, t.t, t.files, m.nReduce}
	fmt.Println("Scheduling... ", reply)

	if t.t != HEARTBEAT {
		// If returned task is not DONE in 10 seconds then restore it as IDLE
		go func(){
			<-time.After(10 * time.Second)
			if t.s == INPROGRESS {
				fmt.Println(t, " RESTORE TO IDLE ")
				t.s = IDLE
			}
		}()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.allDone
}

func (m *Master) splitMapTask(files []string) {
	m.tasks = make(map[string]*Task)
	for i,f := range files {
		name := fmt.Sprintf("map-%02d", i)
		m.tasks[name] = &Task{name, MAP, IDLE, []string{f}}
	}
	for _,t := range m.tasks {
		fmt.Println("All Map tasks added...  ", t)
	}
	m.nMap = len(files)
}

func (m *Master) getIdleTask() *Task {
	m.mux.RLock()
	defer m.mux.RUnlock()
	for _,task := range m.tasks {
		if task.s == IDLE {
			task.s = INPROGRESS
			return task
		}
	}
	return &Task{t: HEARTBEAT}
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.splitMapTask(files)

	m.server()

	return &m
}
