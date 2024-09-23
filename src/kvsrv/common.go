package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Taskid int64
	Clerk  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Taskid int64
	Clerk  int64
	Key    string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

type GetClerkIDArgs struct {
}
type GetClerkIDReply struct {
	ClerkID int64
}

type AppendClearArgs struct {
	Taskid int64
	Clerk  int64
}
type AppendClearReply struct {
}
