package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type MapTask struct {
	mapID   int
	file    string
	runTime int
}

type ReduceTask struct {
	reduceID int
	files    []string
	runTime  int
}

type Coordinator struct {
	// Your definitions here.
	ch                   chan int
	nReduce              int
	mapTasks             []*MapTask
	reduceTasks          []*ReduceTask
	inProcessMapTasks    map[int]*MapTask
	inProcessReduceTasks map[int]*ReduceTask
}

// coordinator sock
var cSock string = coordinatorSock()

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// set log
	// log.SetOutput(os.Stdout)
	log.SetOutput(io.Discard)
	// outFile, _ := os.Open("../coordinator.log")
	// log.SetOutput(outFile)

	// Init coordinator
	log.SetFlags(log.Lshortfile)
	log.Println("Init coordinator")
	c := Coordinator{}
	c.ch = make(chan int, 1)
	c.nReduce = nReduce
	for i, file := range files {
		mapTask := MapTask{}
		mapTask.mapID = i
		mapTask.file = file
		mapTask.runTime = 0
		c.mapTasks = append(c.mapTasks, &mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{}
		reduceTask.reduceID = i
		reduceTask.runTime = 0
		c.reduceTasks = append(c.reduceTasks, &reduceTask)
	}
	c.inProcessMapTasks = make(map[int]*MapTask)
	c.inProcessReduceTasks = make(map[int]*ReduceTask)
	c.printCoordinator()

	// start rpc server and worker failure checking thread
	c.server()
	go c.checkWorkers()
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	log.Print("Set server")
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":"+coordinatorPort)
	os.Remove(cSock)
	l, e := net.Listen("unix", cSock)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Print("Listen to:", cSock)
	go http.Serve(l, nil)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleRequest(args *byte, reply *Task) error {
	log.Println("Handle request")
	c.ch <- 1
	if len(c.mapTasks) != 0 { // map tasks left
		task := c.mapTasks[0]
		reply.TaskType = 0
		reply.TaskID = task.mapID
		reply.Files = append(reply.Files, task.file)
		reply.NReduce = c.nReduce

		c.mapTasks = c.mapTasks[1:]
		c.inProcessMapTasks[task.mapID] = task

		log.Print("Response, task type: ", reply.TaskType, " task ID: ", reply.TaskID, " task file: ", reply.Files, " NReduce: ", reply.NReduce)
		c.printCoordinator()
	} else if len(c.inProcessMapTasks) != 0 {
		reply.TaskType = 2
	} else if len(c.reduceTasks) != 0 { // reduce tasks left
		task := c.reduceTasks[0]
		reply.TaskType = 1
		reply.TaskID = task.reduceID
		reply.Files = append(reply.Files, task.files...)
		reply.NReduce = c.nReduce

		c.reduceTasks = c.reduceTasks[1:]
		c.inProcessReduceTasks[task.reduceID] = task

		log.Print("Response, task type: ", reply.TaskType, " task ID: ", reply.TaskID, " task file: ", reply.Files, " NReduce: ", reply.NReduce)
		c.printCoordinator()
	} else {
		reply.TaskType = 2
	}
	<-c.ch
	return nil
}

func (c *Coordinator) HandleNotice(args *NoticeArgs, reply *bool) error {
	log.Print("Handle notice")
	c.ch <- 1
	if _, ok := c.inProcessMapTasks[args.TaskID]; args.TaskType == 0 && ok { // map task done
		log.Printf("Map task %v done", args.TaskID)
		delete(c.inProcessMapTasks, args.TaskID)
		for i, file := range args.Files {
			c.reduceTasks[i].files = append(c.reduceTasks[i].files, file)
		}
		c.printCoordinator()
		*reply = true
	} else if _, ok = c.inProcessReduceTasks[args.TaskID]; args.TaskType == 1 && ok { // reduce task done
		log.Printf("Reduce task %v done", args.TaskID)
		delete(c.inProcessReduceTasks, args.TaskID)
		c.printCoordinator()
		*reply = true
	} else if args.TaskType == 0 {
		log.Printf("Map task %v time out", args.TaskID)
		*reply = false
	} else if args.TaskType == 1 {
		log.Printf("Reduce task %v time out", args.TaskID)
		*reply = false
	}
	<-c.ch
	return nil
}

func (c *Coordinator) checkWorkers() {
	for {
		c.ch <- 1
		// check map workers
		for taskID, task := range c.inProcessMapTasks {
			task.runTime++
			c.inProcessMapTasks[taskID] = task
			if task.runTime >= 10 {
				delete(c.inProcessMapTasks, taskID)
				task.runTime = 0
				c.mapTasks = append(c.mapTasks, task)
				log.Printf("Map task %v idle, stop it for rescheduling", taskID)
				c.printCoordinator()
			}
		}

		// check reduce workers
		for taskID, task := range c.inProcessReduceTasks {
			task.runTime++
			c.inProcessReduceTasks[taskID] = task
			if task.runTime >= 10 {
				delete(c.inProcessReduceTasks, taskID)
				task.runTime = 0
				c.reduceTasks = append(c.reduceTasks, task)
				log.Printf("Reduce task %v idle, stop it for rescheduling", taskID)
				c.printCoordinator()
			}
		}
		<-c.ch
		time.Sleep(time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// log.Print("Check done")
	c.ch <- 1
	ret := false

	// Your code here.
	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 && len(c.inProcessMapTasks) == 0 && len(c.inProcessReduceTasks) == 0 {
		ret = true
	}

	<-c.ch
	return ret
}

func (c *Coordinator) printCoordinator() {
	log.Println()
	log.Println("Print coordinator states")
	log.Println("NReduce:", c.nReduce)
	if len(c.mapTasks) > 0 {
		log.Println("Map tasks:")
		for _, mapTask := range c.mapTasks {
			log.Printf("%v ", *mapTask)
		}
		log.Println()
	}
	if len(c.reduceTasks) > 0 {
		log.Println("Reduce tasks:")
		for _, reduceTask := range c.reduceTasks {
			log.Printf("%v ", *reduceTask)
		}
		log.Println()
	}
	if len(c.inProcessMapTasks) > 0 {
		log.Println("In process Map tasks:")
		for _, mapTask := range c.inProcessMapTasks {
			log.Printf("%v ", *mapTask)
		}
		log.Println()
	}
	if len(c.inProcessReduceTasks) > 0 {
		log.Println("In process Reduce tasks:")
		for _, reduceTask := range c.inProcessReduceTasks {
			log.Printf("%v ", *reduceTask)
		}
		log.Println()
	}
	log.Println()
}
