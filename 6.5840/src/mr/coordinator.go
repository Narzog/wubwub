package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"



type Coordinator struct {
	mu       sync.Mutex
	length    int 
	filenames map[int]string
	queuedfilenames map[QueueValue]int
	finishedfilenames map[int]string
	nReduce int 
	reduce map[int]int 
	queuedreduce map[int]int 
	finishedreduce map[int]int 
	finish string 
}
type QueueValue struct {
	Key     int
	File string
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.


func (c *Coordinator) Example(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock() 
	defer c.mu.Unlock()
	if c.finish == "Map" { 
	

	for key, value := range c.filenames { 
		reply.FilenameKey = key 
		reply.Task = "map"
		reply.Nreduce = c.nReduce //Sending nreduce to the reply back to the worker
		reply.Y = value
		quevalue := QueueValue{key, value} 
		c.queuedfilenames[quevalue] = 0
		delete(c.filenames, key)
		
		break
	}
	} else if c.finish == "Reduce"{ 
		for k, _ := range c.reduce {
			reply.FilenameKey = k // Y task number 
			reply.Nreduce = c.length //This will be used as the x in the x-y thing 
			reply.Task = "reduce"
			reply.Y = "ERRORIFTHISCOMESUP" 
			c.queuedreduce[k] = 0
			delete(c.reduce, k)
			break
//Need to implement logic where queuedreduce is removed inside of mapping done 
//Need to do the incrementing logic too inside organizier 
		}
	} else if c.finish == "Done"{
		reply.Task = "stop"
	}
	return nil
}

//Adds that Key Value ie number and filename into a shared coordinate thingy 
func (c *Coordinator) MappingDone(args *RpcMappingDone, reply *RpcMappingDoneReply)  error{
	reply.Y = 123
	c.mu.Lock() 
	defer c.mu.Unlock()
	if c.finish == "Map" { 
	delete(c.queuedfilenames, QueueValue{args.Value, args.Filename})
	c.finishedfilenames[args.Value] = args.Filename
	if len(c.finishedfilenames) == c.length {
		c.finish = "Reduce"
	}
}   else if c.finish == "Reduce"{ 
		// fmt.Println("wub")
	
		delete(c.queuedreduce, args.Value)
		c.finishedreduce[args.Value] = args.Value
		if len(c.finishedreduce) == c.nReduce {
			c.finish = "Done"
			fmt.Println("OVERALLDONE")
		}
	} 
	return nil

}

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

func (c *Coordinator) Organizer() {
	for true {
		if c.finish == "Map" && len(c.queuedfilenames) > 0 {
			c.mu.Lock() 
			// fmt.Println(c.queuedfilenames)
			for k, _ := range c.queuedfilenames{
				c.queuedfilenames[k] = c.queuedfilenames[k] + 1 
				if c.queuedfilenames[k] > 9 {
					delete(c.queuedfilenames, k)
					c.filenames[k.Key] = k.File //readding it into the filename stuff
				}
			}
			c.mu.Unlock() 
		} else if c.finish == "Reduce" && len(c.queuedreduce) > 0 {
			c.mu.Lock() 
			for x, _ := range c.queuedreduce{
				c.queuedreduce[x] = c.queuedreduce[x] + 1 
				if c.queuedreduce[x] > 9 {
					delete(c.queuedreduce, x)
					c.reduce[x] = x //readding it into the filename stuff
				}
			}
			c.mu.Unlock() 
		}
		time.Sleep(1 * time.Second)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true //This was false earlier 
	if c.finish == "Done" {
		return ret
	} 
	// Your code here.
	time.Sleep(100*time.Second) //remove this later 
	os.Exit(3) //also this i think idk lol
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.reduce = make(map[int]int) 
	for i:=0; i < nReduce; i++ {
		c.reduce[i] = i
 	}
	c.queuedreduce = make(map[int]int) 
	c.finishedreduce =make(map[int]int) 
	
	c.finish = "Map"
	c.nReduce = nReduce
	c.finishedfilenames = make(map[int]string)
	c.filenames = make(map[int]string)
	c.queuedfilenames = make(map[QueueValue]int)
	c.length = len(files)
	for i, v := range files {
		c.filenames[i] = v	
	}
	// c.filenames = append(c.filenames, files...)
	go c.Organizer()
	c.server()
	return &c
}
