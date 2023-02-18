package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "encoding/json"

import "sort"

//Questions: Now that you have all the reduce outputs, then what... do y

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





//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	messages := make(chan string)
	nReducech := make(chan int)
	taskchannel := make(chan string)
	filenamekeych := make(chan int)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	
	for true {
	go CallExample(messages, nReducech, taskchannel, filenamekeych)
	
	task := <- taskchannel
	if task == "stop" {
		fmt.Println("Worker Done")
		break
	}	
	filenamekey := <- filenamekeych
	filename := <- messages
	nReduce := <- nReducech
	if task == "map" {
		intermediate := []KeyValue{}
		mapping := make(map[int][]KeyValue)
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
		intermediate = append(intermediate, kva...)
		
		
		pow := make([]int, nReduce) //For loop to create the ten different reduce files
		for i := range pow {
			mapping[i] = []KeyValue{}
		}
		for _, kv := range intermediate { 
			hashvalue := (ihash(kv.Key) % nReduce)
			mapping[hashvalue] = append(mapping[hashvalue], kv)
		}
		for k, v  := range mapping { 
			name := fmt.Sprintf("%s%d%s%d%s", "mr-", filenamekey, "-", k, "-new") //concatenates string and a number together
			file, err := os.Create(name) 		
			if err != nil {
				panic(err)
			}
			enc := json.NewEncoder(file)
			for _, kv  := range v {
				erring := enc.Encode(&kv)
				if erring != nil {
					fmt.Println("JSON creation doesn't work")	
					panic(erring)
				}			
			}
			done := fmt.Sprintf("%s%d%s%d", "mr-", filenamekey, "-", k) //concatenates  
			os.Rename(name, done) //For crashes 
			file.Close()
		}
		MappingDoneCall(filenamekey, filename) //REMINDER need to change this for reduce 
} else if task == "reduce" {
	fmt.Println("Inside Reduce")
	//Opening all the reduce task files now based on what reduce number we got.
	recieverkeyvaluearray := []KeyValue{}
	
	for k := 0; k < nReduce; k++ { 
		
		reducefile := fmt.Sprintf("%s%d%s%d", "mr-", k, "-", filenamekey) //mr-X-Y, where X is the Map task number, and Y is the reduce task number.
		recievingfile, err := os.Open(reducefile)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
		dec := json.NewDecoder(recievingfile)
		for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		recieverkeyvaluearray = append(recieverkeyvaluearray, kv)
		recievingfile.Close()
		}
	}
		fmt.Println("This is recieverkeyvaluearray")
		fmt.Println(recieverkeyvaluearray)
		sort.Sort(ByKey(recieverkeyvaluearray))

		oname := fmt.Sprintf("%s%d", "mr-out-", filenamekey)  
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(recieverkeyvaluearray) {
			j := i + 1
			for j < len(recieverkeyvaluearray) && recieverkeyvaluearray[j].Key == recieverkeyvaluearray[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, recieverkeyvaluearray[k].Value)
			}
			output := reducef(recieverkeyvaluearray[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", recieverkeyvaluearray[i].Key, output)

			i = j
		}

		ofile.Close()
		MappingDoneCall(filenamekey, "THISISANERROR") //Need to change this call!!!!!!!!!!!!!!!!!
}
	fmt.Println("DONE")
}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample(messagech chan<- string, nReducech chan<- int)(){

func CallExample(messagech chan<- string, nReducech chan<- int, taskchannel chan<- string, filenamekeych chan<- int)(){

	args := RpcArgs{}
	args.X = 1
	reply := RpcReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		taskchannel <-reply.Task 
		filenamekeych <- reply.FilenameKey
		messagech <- reply.Y
		nReducech <- reply.Nreduce
	} else {
		fmt.Printf("call failed!\n")
		taskchannel <- "stop"
	}
}

func MappingDoneCall(value int, filename string)(){

	args := RpcMappingDone{}
	args.Value = value 
	args.Filename = filename
	reply := RpcMappingDoneReply{}
	ok := call("Coordinator.MappingDone", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Printf("err %v\n", err)
	return false
}
