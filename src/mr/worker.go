package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// var directory string = "../main/"

var directory string = ""

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// log.SetOutput(os.Stdout)
	log.SetOutput(ioutil.Discard)
	log.SetFlags(log.Lshortfile)
	log.Print("Worker starts")
	for {
		log.Print()
		log.Print("Request for task")
		task := Task{}
		res := taskCall(&task)
		if res == 0 {
			log.Fatal("Worker terminates")
		} else if res == 2 {
			log.Println("Error in rpc")
			continue
		} else if task.TaskType == 2 {
			log.Println("No task available")
			time.Sleep(5 * time.Second)
			continue
		}

		// do task
		var noticeFiles []string
		if task.TaskType == 0 { // map
			log.Print("Got map task")
			noticeFiles = doMap(mapf, task)
		} else if task.TaskType == 1 { // reduce
			log.Print("Got reduce task")
			doReduce(reducef, task)
		}

		// notice
		log.Print("Task done, notice coordinator")
		noticeArgs := NoticeArgs{}
		noticeArgs.TaskType = task.TaskType
		noticeArgs.TaskID = task.TaskID
		noticeArgs.Files = noticeFiles
		var reply bool
		noticeCall(&noticeArgs, &reply)
		if task.TaskType == 1 && reply == true {
			for _, file := range task.Files {
				os.Remove(directory + file)
			}
		}

		time.Sleep(time.Second)
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func taskCall(reply *Task) int {
	// c, err := rpc.DialHTTP("tcp", coordinatorIP+coordinatorPort)
	c, err := rpc.DialHTTP("unix", cSock)
	if err != nil {
		log.Println("Coordinator terminated, tasks done")
		return 0
	}
	defer c.Close()

	var arg byte = 1
	err = c.Call("Coordinator.HandleRequest", &arg, reply)
	if err == nil {
		return 1
	}
	return 2
}

//
// send an RPC notice to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong or the coordinator response false.
//
func noticeCall(args *NoticeArgs, reply *bool) bool {
	// c, err := rpc.DialHTTP("tcp", coordinatorIP+coordinatorPort)
	c, err := rpc.DialHTTP("unix", cSock)
	if err != nil {
		log.Println("Fail to notice")
		return false
	}
	defer c.Close()

	err = c.Call("Coordinator.HandleNotice", args, reply)
	if err == nil && *reply == true {
		return true
	}
	return false
}

func doMap(mapf func(string, string) []KeyValue, task Task) []string {
	// get map file path
	filename := task.Files[0]
	filepath := directory + filename
	log.Print("Do map on file:", filepath)

	// read map file and do map
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)
	// log.Printf("Map result: %v", intermediate)

	// Create json files
	var interFiles []string
	for i := 0; i < task.NReduce; i++ {
		interFilename := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i) + ".json"
		file, err := os.Create(directory + interFilename)
		if err != nil {
			log.Fatal("Error while creating file:", err)
		}
		file.Close()
		interFiles = append(interFiles, interFilename)
	}
	log.Printf("Create intermedia files: %v", interFiles)

	// Write to json files
	for _, kv := range intermediate {
		key := kv.Key
		reduceID := ihash(key) % task.NReduce
		file, err := os.OpenFile(directory+interFiles[reduceID], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Error while opening file:", err)
		}

		err = json.NewEncoder(file).Encode(&kv)
		if err != nil {
			log.Fatal("Error while encoding KeyValue to file:", err)
		}
		file.Close()
	}
	log.Print("Write KeyValues to intermedia files")

	return interFiles
}

func doReduce(reducef func(string, []string) string, task Task) {
	// read from reduce files
	log.Print("Do reduce on files:", task.Files)
	var intermediate []KeyValue
	for _, filename := range task.Files {
		filepath := directory + filename

		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Error while opening file:", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
		// os.Remove(filepath)
	}

	// sort
	sort.Sort(ByKey(intermediate))
	// log.Print("Read KeyValues:", intermediate)

	// create out file
	outFileName := "mr-out-" + strconv.Itoa(task.TaskID)
	ofile, _ := os.Create(directory + outFileName)
	log.Print("Write to file:", outFileName)

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
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
