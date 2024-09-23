package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"
	"6.5840/labrpc"
)

type Clerk struct {
	server  *labrpc.ClientEnd
	taskid  int64
	clerkid int64
	getid   bool
	// You will have to modify this struct.
	// data map[string]string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.taskid = 0
	ck.getid = false
	// ck.data = make(map[string]string)
	// You'll have to add code here.
	return ck
}
func (ck *Clerk) GetClerkID() int64 {
	args := GetClerkIDArgs{}
	var reply GetClerkIDReply
	for {
		eil := ck.server.Call("KVServer.GetClerkID", &args, &reply)
		// println(reply.ClerkID)
		if eil {
			break
		}
		time.Sleep(1*time.Second)
	}
	ck.clerkid = reply.ClerkID
	return ck.clerkid
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	if !ck.getid {
		ck.GetClerkID()
		ck.getid = true
	}
	args := GetArgs{Key: key}
	ck.taskid++
	args.Taskid = ck.taskid
	args.Clerk = ck.clerkid
	var reply GetReply
	// ck.server.Call("KVServer.Get", &args, &reply)
	for {
		eil := ck.server.Call("KVServer.Get", &args, &reply)
		if eil {
			break
		}
		time.Sleep(1*time.Second)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	if !ck.getid {
		ck.GetClerkID()
		ck.getid = true
	}
	args := PutAppendArgs{Key: key, Value: value}
	ck.taskid++
	args.Taskid = ck.taskid
	args.Clerk = ck.clerkid
	var reply PutAppendReply
	for {
		eil := ck.server.Call("KVServer."+op, &args, &reply)
		if eil {
			break
		}
		time.Sleep(1*time.Second)
	}
	if(op=="Append"){
		args:=AppendClearArgs{Taskid:args.Taskid,Clerk:args.Clerk}
		var reply AppendClearReply
		for {
			eil := ck.server.Call("KVServer.AppendClear", &args, &reply)
			if eil {
				break
			}
			time.Sleep(1*time.Second)
		}
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
