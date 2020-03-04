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

	nnReduce int
	nMap int
	doneMap int
	doneReduce int

	tasks map[string]*Task

	mux sync.RWMutex

	allDone bool

	waitForExit int
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
	HEARTBEAT TaskType = iota  // HEARTBEAT should be default value
	MAP
	REDUCE
	EXIT
)

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	name string
	t TaskType
	s TaskState
	files []string
}


func (m *Master) dumpTaskState() {
	mapc := make(map[TaskState]int)
	reducec := make(map[TaskState]int)
	for _, t := range m.tasks {
		switch t.t {
			case MAP: mapc[t.s] ++
			case REDUCE: reducec[t.s] ++
		}
	}
	fmt.Println(mapc, reducec)
}

func (m *Master) NextTask(req *TaskAck, reply *NextTask) error {
	fmt.Println("Master Receiving... ", req.Name)
	m.dumpTaskState()
	
	m.mux.Lock()
	// Mark task DONE according to TaskAck
	if tt,ok := m.tasks[req.Name]; ok && tt.s == INPROGRESS {
		fmt.Println("Master Acking... ", req)
		tt.s = DONE

		switch tt.t {
		case MAP:
			
			for i, ofile := range req.Output {
				rtN := fmt.Sprintf("reduce-%02d", i)
				if rt, o := m.tasks[rtN]; o {
					m.tasks[rtN].files = append(rt.files, ofile)
				}else{
					m.tasks[rtN] = &Task{name: rtN, t: REDUCE, s: NOTREADY, files: []string{ofile}}
					m.nnReduce ++ 
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
			
		case REDUCE:
			m.doneReduce ++
		}
	}
	m.mux.Unlock()

	if req.Type == EXIT {
		fmt.Println("A worker exited", m.waitForExit)
		m.waitForExit --
		if m.waitForExit == 0 {
			m.allDone = true
			os.Exit(0)
		}
		return nil
	}

	// all jobs are done, send terminate signal
	if m.doneReduce > 0 && m.doneReduce == m.nnReduce {
		*reply = NextTask{"complete", EXIT, make([]string,0), m.nReduce}
		m.waitForExit ++
		return nil
	}

	// Get Idle task and return it to worker
	t := m.getIdleTask()
	*reply = NextTask{t.name, t.t, t.files, m.nReduce}
	fmt.Println("Scheduling... ", reply.Name)

	if t.t != HEARTBEAT {
		// If returned task is not DONE in 10 seconds then restore it as IDLE
		go func(){
			<-time.After(10 * time.Second)
			m.mux.Lock()
			if t.s == INPROGRESS {
				fmt.Println(t.name, " RESTORE TO IDLE ")
				t.s = IDLE
			}
			m.mux.Unlock()
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
	os.Remove("/data/mr-socket")
	l, e := net.Listen("unix", "/data/mr-socket")
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
	m.mux.Lock()
	defer m.mux.Unlock()
	for _,task := range m.tasks {
		if task.s == IDLE {
			task.s = INPROGRESS
			return task
		}
	}
	// if no more IDLE task, reschedule INPROGRESS task
	for _,task := range m.tasks {
		if task.s == INPROGRESS {
			return task
		}
	}
	return &Task{name: "Heartbeat", t: HEARTBEAT}
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	os.RemoveAll("/data/mr-mapout/")
	os.MkdirAll("/data/mr-mapout/", os.ModeDir)
	// Your code here.
	m.nReduce = nReduce
	m.splitMapTask(files)

	m.server()

	return &m
}
