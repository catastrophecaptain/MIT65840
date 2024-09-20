package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CurrentWorkerID struct {
	WorkerID int
	mu       sync.Mutex
}

func (c *CurrentWorkerID) getWorkerID() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.WorkerID++
	return c.WorkerID
}

func (c *CurrentWorkerID) init() {
	c.WorkerID = 0
}

type ReduceFilesArray struct {
	filesArray [][]string
	mu         []sync.Mutex
}

func (r *ReduceFilesArray) init(nReduce int) {
	r.filesArray = make([][]string, nReduce)
	r.mu = make([]sync.Mutex, nReduce)
}

func (r *ReduceFilesArray) getReduceFiles(reduceID int) []string {
	r.mu[reduceID].Lock()
	defer r.mu[reduceID].Unlock()
	return r.filesArray[reduceID]
}

func (r *ReduceFilesArray) setReduceFiles(reduceID int, file string) {
	r.mu[reduceID].Lock()
	defer r.mu[reduceID].Unlock()
	r.filesArray[reduceID] = append(r.filesArray[reduceID], file)
}

type TasksStatus struct {
	mapTasksStatus    []bool
	reduceTasksStatus []bool
	nReduceLeft       int
	nMapLeft          int
	reduceInit        bool
	exitInit          bool
	mapMachine        []struct {
		start time.Time
		work  bool
	}
	reduceMachine []struct {
		start time.Time
		work  bool
	}
	reduceFilesArray ReduceFilesArray
	// 认定崩溃时间间隔
	crashTime time.Duration
	mu        sync.Mutex
}

func (t *TasksStatus) init(nMap int, nReduce int) {
	t.mapTasksStatus = make([]bool, nMap)
	t.reduceTasksStatus = make([]bool, nReduce)
	t.nReduceLeft = nReduce
	t.nMapLeft = nMap
	t.mapMachine = make([]struct {
		start time.Time
		work  bool
	}, nMap)
	t.reduceMachine = make([]struct {
		start time.Time
		work  bool
	}, nReduce)
	t.reduceFilesArray.init(nReduce)
	t.crashTime = 20 * time.Second

	for i := 0; i < nMap; i++ {
		t.mapTasksStatus[i] = false
	}
	for i := 0; i < nReduce; i++ {
		t.reduceTasksStatus[i] = false
	}
	t.reduceInit = false
	t.exitInit = false
	for i := 0; i < nMap; i++ {
		t.mapMachine[i].work = false
	}
	for i := 0; i < nReduce; i++ {
		t.reduceMachine[i].work = false
	}
}
func (t *TasksStatus) needReduceInit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.nMapLeft == 0 && !t.reduceInit {
		t.reduceInit = true
		return true
	} else {
		return false
	}
}
func (t *TasksStatus) needExitInit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.nReduceLeft == 0 && !t.exitInit {
		t.exitInit = true
		return true
	} else {
		return false
	}
}
func (t *TasksStatus) setMapTasksStatus(mapID int, reduceFiles []string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mapTasksStatus[mapID] {
		return false
	}
	t.mapTasksStatus[mapID] = true
	t.mapMachine[mapID].work = false
	t.nMapLeft--
	for index, file := range reduceFiles {
		t.reduceFilesArray.setReduceFiles(index, file)
	}
	return true
}
func (t *TasksStatus) setReduceTasksStatus(reduceID int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reduceTasksStatus[reduceID] {
		return false
	}
	t.reduceTasksStatus[reduceID] = true
	t.reduceMachine[reduceID].work = false
	t.nReduceLeft--
	return true
}
func (t *TasksStatus) setMapMachine(mapID int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mapMachine[mapID].start = time.Now()
	t.mapMachine[mapID].work = true
}
func (t *TasksStatus) setReduceMachine(reduceID int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reduceMachine[reduceID].start = time.Now()
	t.reduceMachine[reduceID].work = true
}
func (t *TasksStatus) IsReduceTasksDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nReduceLeft == 0
}
func (t *TasksStatus) getCrashedMapTasksID() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	var crashedMapTasksID []int
	for i, infoMachine := range t.mapMachine {
		if infoMachine.start.Add(t.crashTime).Before(time.Now()) && !t.mapTasksStatus[i] {
			crashedMapTasksID = append(crashedMapTasksID, i)
			t.mapMachine[i].work = false
		}
	}
	return crashedMapTasksID

}

func (t *TasksStatus) getCrashedReduceTasksID() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	var crashedReduceTasksID []int
	for i, time_b := range t.reduceMachine {
		if time_b.start.Add(t.crashTime).Before(time.Now()) && !t.reduceTasksStatus[i] {
			crashedReduceTasksID = append(crashedReduceTasksID, i)
			t.reduceMachine[i].work = false
		}
	}
	return crashedReduceTasksID
}

func (t *TasksStatus) getReduceFiles(reduceID int) []string {
	return t.reduceFilesArray.getReduceFiles(reduceID)
}

type Task = AskTaskReply

type Coordinator struct {
	currentWorkerID CurrentWorkerID
	mapTaskChan     chan Task
	reduceTaskChan  chan Task
	exitTaskChan    chan Task
	nReduce         int
	mapFiles        []string
	tasksStatus     TasksStatus

	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskID(args *AskIDArgs, reply *AskIDReply) error {
	reply.WorkerID = c.currentWorkerID.getWorkerID()
	return nil
}
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	reply.NReduce = c.nReduce
	select {
	case task := <-c.mapTaskChan:
		*reply = task
		c.tasksStatus.setMapMachine(reply.MapID)
		return nil
	case task := <-c.reduceTaskChan:
		*reply = task
		c.tasksStatus.setReduceMachine(reply.ReduceID)
		return nil
	case _, ok := <-c.exitTaskChan:
		if !ok {
			*reply = Task{TaskType: "exit"}
		}
		return nil
	}
}
func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	if args.TaskType == "map" {
		succ := c.tasksStatus.setMapTasksStatus(args.MapID, args.ReduceFiles)
		if succ {
			if c.tasksStatus.needReduceInit() {
				go c.initReduceTaskChan()
			}
		}
	} else if args.TaskType == "reduce" {
		succ := c.tasksStatus.setReduceTasksStatus(args.ReduceID)
		if succ {
			if c.tasksStatus.needExitInit() {
				close(c.exitTaskChan)
			}
		}
	}
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
func (c *Coordinator) initReduceTaskChan() {
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskChan <- Task{TaskType: "reduce", ReduceID: i, ReduceFiles: c.tasksStatus.getReduceFiles(i)}
	}
}
func (c *Coordinator) initMapTaskChan() {
	for i, file := range c.mapFiles {
		c.mapTaskChan <- Task{TaskType: "map", MapID: i, MapFile: file, NReduce: c.nReduce}
		// println("nReduce: ", c.nReduce)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.tasksStatus.IsReduceTasksDone()
	if(ret){
		c.cleanIntermediateFiles()
	}
	// Your code here.

	return ret
}

func (c *Coordinator) cleanIntermediateFiles() {
	for i := 0; i < c.nReduce; i++ {
		files := c.tasksStatus.getReduceFiles(i)
		for _, file := range files {
			os.Remove(file)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	duration := 20 * time.Second
	c := Coordinator{}
	c.currentWorkerID.init()
	c.mapTaskChan = make(chan Task, len(files))
	c.reduceTaskChan = make(chan Task, nReduce)
	c.exitTaskChan = make(chan Task)
	c.nReduce = nReduce
	c.mapFiles = files
	c.tasksStatus.init(len(files), nReduce)
	go c.initMapTaskChan()
	c.server()
	println("Coordinator started")
	println("nReduce: ", nReduce)
	println("nMap: ", len(files))
	go c.examine(duration)
	return &c
}
func (c *Coordinator) examine(duration time.Duration) {
	for {
		time.Sleep(duration)
		crashedMapTasksID := c.tasksStatus.getCrashedMapTasksID()
		crashedReduceTasksID := c.tasksStatus.getCrashedReduceTasksID()
		for _, mapID := range crashedMapTasksID {
			c.mapTaskChan <- Task{TaskType: "map", MapID: mapID, MapFile: c.mapFiles[mapID], NReduce: c.nReduce}
		}
		for _, reduceID := range crashedReduceTasksID {
			c.reduceTaskChan <- Task{TaskType: "reduce", ReduceID: reduceID, ReduceFiles: c.tasksStatus.getReduceFiles(reduceID)}
		}
		if c.tasksStatus.IsReduceTasksDone() {
			break
		}
	}
}
