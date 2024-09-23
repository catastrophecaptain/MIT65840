package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type UniqueTask struct {
	clerkID int64
	taskID  int64
}

type KVServer struct {
	mu           sync.Mutex
	data         map[string]string
	clerk        map[int64]int64
	currentClerk int64
	appendPre    map[UniqueTask]string
	appendmu     sync.Mutex
	clerkmu      sync.Mutex
	// Your definitions here.
}

func (kv *KVServer) GetClerkID(args *GetClerkIDArgs, reply *GetClerkIDReply) {
	// print("clerkID: ", reply.ClerkID, "\n")
	kv.clerkmu.Lock()
	defer kv.clerkmu.Unlock()
	reply.ClerkID = kv.currentClerk
	kv.currentClerk++
}

func (kv *KVServer) AppendClear(args *AppendClearArgs, reply *AppendClearReply) {
	kv.appendmu.Lock()
	defer kv.appendmu.Unlock()
	uniqueTask := UniqueTask{clerkID: args.Clerk, taskID: args.Taskid}
	delete(kv.appendPre, uniqueTask)
}

func (kv *KVServer) checkRepeat(clerkID int64, taskID int64) bool {
	kv.clerkmu.Lock()
	defer kv.clerkmu.Unlock()
	if kv.clerk[clerkID] < taskID {
		kv.clerk[clerkID] = taskID
		return false
	}
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.checkRepeat(args.Clerk, args.Taskid) {
		kv.data[args.Key] = args.Value
	}
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.checkRepeat(args.Clerk, args.Taskid) {
		ret := kv.data[args.Key]
		kv.data[args.Key] = ret + args.Value
		reply.Value = ret
		uniqueTask := UniqueTask{clerkID: args.Clerk, taskID: args.Taskid}
		kv.appendmu.Lock()
		kv.appendPre[uniqueTask] = ret
		kv.appendmu.Unlock()
	} else {
		kv.appendmu.Lock()
		uniqueTask := UniqueTask{clerkID: args.Clerk, taskID: args.Taskid}
		reply.Value = kv.appendPre[uniqueTask]
		kv.appendmu.Unlock()
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.clerk = make(map[int64]int64)
	kv.appendPre = make(map[UniqueTask]string)
	kv.currentClerk = 0
	// You may need initialization code here.

	return kv
}
