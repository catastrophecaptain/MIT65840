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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	askidargs := AskIDArgs{}
	askidreply := AskIDReply{}
	call("Coordinator.AskID", &askidargs, &askidreply)
	workID := askidreply.WorkerID
	exit := false
	for !exit {
		asktaskargs := AskTaskArgs{WorkerID: workID}
		asktaskreply := AskTaskReply{}
		err := call("Coordinator.AskTask", &asktaskargs, &asktaskreply)
		// println("nReduce: ", asktaskreply.NReduce)
		// print("TaskType: ", asktaskreply.TaskType)
		// println("MapID: ", asktaskreply.MapID)
		// println("mapfile: ", asktaskreply.MapFile)
		// println("Reduce12: ", asktaskreply.mapID12)
		if !err {
			log.Fatalf("cannot ask task")
		}
		if asktaskreply.TaskType == "map" {
			// println("1")
			filename := asktaskreply.MapFile
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
			// os.CreateTemp() implement a temp file
			// write kva to temp file
			// rename temp file to final file
			// the following is the code
			var tempfiles []*os.File
			var enc []*json.Encoder
			for i := 0; i < asktaskreply.NReduce; i++ {
				// tempfiles = append(tempfiles, os.CreateTemp(".", "temp-worker-"))
				tempfile, err := os.CreateTemp(".", "temp-worker-")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				tempfiles = append(tempfiles, tempfile)
				enc = append(enc, json.NewEncoder(tempfiles[i]))
			}
			// use json to encode kva
			// write to temp file
			// the following is the code
			// println("nReduce: ", asktaskreply.NReduce)
			for _, kv := range kva {
				err := enc[ihash(kv.Key)%asktaskreply.NReduce].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode")
				}
			}
			// close temp file
			// rename temp file to final file
			// the following is the code
			for i := 0; i < asktaskreply.NReduce; i++ {
				tempfiles[i].Close()
			}
			for i := 0; i < asktaskreply.NReduce; i++ {
				os.Rename(tempfiles[i].Name(), "mr-"+strconv.Itoa(asktaskreply.MapID)+"-"+strconv.Itoa(i))
			}
			finishargs := FinishArgs{TaskType: "map", MapID: asktaskreply.MapID, ReduceFiles: []string{}}
			finishreply := FinishReply{}
			for i := 0; i < asktaskreply.NReduce; i++ {
				finishargs.ReduceFiles = append(finishargs.ReduceFiles, "mr-"+strconv.Itoa(asktaskreply.MapID)+"-"+strconv.Itoa(i))
			}
			call("Coordinator.Finish", &finishargs, &finishreply)
		} else if asktaskreply.TaskType == "reduce" {
			// println("2")
			var kva []KeyValue
			for _, file := range asktaskreply.ReduceFiles {
				f, err := os.Open(file)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				f.Close()
			}
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(asktaskreply.ReduceID)
			tempfile, _ := os.CreateTemp(".", "temp-worker-")
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			tempfile.Close()
			os.Rename(tempfile.Name(), oname)
			finishargs := FinishArgs{TaskType: "reduce", ReduceID: asktaskreply.ReduceID}
			finishreply := FinishReply{}
			call("Coordinator.Finish", &finishargs, &finishreply)
		} else {
			exit = true
		}
		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
	}
	os.Exit(0);
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
