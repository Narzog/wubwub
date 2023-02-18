package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how 4to declare the arguments
// and reply for an RPC.
//
type RpcMappingDone struct {
	Value int
	Filename string
}

type RpcMappingDoneReply struct {
	Y int
}

type RpcArgs struct {
	X int
}


type RpcReply struct {
	Y string
	FilenameKey int
	Task string
	Nreduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-013"
	s += strconv.Itoa(os.Getuid())
	return s
}
