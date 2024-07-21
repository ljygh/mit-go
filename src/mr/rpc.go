package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type Task struct {
	TaskType int // 0 for map, 1 for reduce, 2 for no task available
	TaskID   int
	Files    []string
	NReduce  int
}

type NoticeArgs struct {
	TaskType int
	TaskID   int
	Files    []string
}

// type NoticeReply struct {
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
