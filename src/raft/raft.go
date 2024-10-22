package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Entry struct {
	Term    int
	Command interface{}
}

const (
	follower = iota
	candidate
	leader
)
const (
	called = iota
	notCalled
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent
	currentTerm int
	votedFor    int
	logs        []Entry

	// all servers
	commitIndex int64
	lastApplied int64

	// leader
	nextIndex  []int64
	matchIndex []int64

	// self define volatile
	status    int32
	heartBoot int32
	// heartBootMu sync.Mutex

	// self define persistent
	initIndex int64
	voteNum   int
	leaderId  int
	lastNewIndex int64

	// return to tester
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.status == leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// if len(args.Entries)==0{
	// println(rf.me,"receive boot")
	rf.heartBoot = called
	// }
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = follower
		rf.lastNewIndex = rf.commitIndex
		rf.votedFor = -1
	} else if rf.status == candidate && args.Term == rf.currentTerm {
		rf.status = follower
		rf.lastNewIndex = rf.commitIndex
	}

	if rf.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if(args.PrevLogIndex+int64(len(args.Entries))>rf.lastNewIndex){
		rf.lastNewIndex=args.PrevLogIndex+int64(len(args.Entries))
	}
	if len(rf.logs) < int(args.PrevLogIndex-rf.initIndex+int64(len(args.Entries))+1) {
		//扩容
		// rf.logs=append(rf.logs,make([]Entry,int(args.PrevLogIndex-rf.initIndex+int64(len(args.Entries))+1-int64(len(rf.logs))))...)
		rf.logs = append(rf.logs, args.Entries[rf.initIndex+int64(len(rf.logs))-args.PrevLogIndex-1:]...)
	}
	var cut bool = false
	for i := 0; i < len(args.Entries); i++ {
		if !cut && rf.getEntryTerm(args.PrevLogIndex+1+int64(i)) != args.Entries[i].Term {
			rf.logs = append(make([]Entry, 0), rf.logs[:args.PrevLogIndex-rf.initIndex+int64(len(args.Entries))+1]...)
			cut = true
		}
		rf.logs[args.PrevLogIndex-rf.initIndex+1+int64(i)] = args.Entries[i]
	}
	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex=min(args.LeaderCommit,int64(len(rf.logs))+rf.initIndex-1)
		if args.LeaderCommit < rf.lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastNewIndex
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	lastLogIndex := rf.initIndex + int64(len(rf.logs)) - 1
	lastLogTerm := rf.getEntryTerm(lastLogIndex)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = follower
		rf.lastNewIndex = rf.commitIndex
		rf.votedFor = -1
	}
	newer := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && newer {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.heartBoot = called
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = follower
		rf.lastNewIndex = rf.commitIndex
		rf.votedFor = -1
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// println("append",ok)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = follower
		rf.lastNewIndex = rf.commitIndex
		rf.votedFor = -1
	}
	return ok
}

func (rf *Raft) replicateEntries(server int, starti int64, endi int64, term int) {
	rf.mu.Lock()
	appendEntriesArgs := AppendEntriesArgs{}
	appendEntriesArgs.Term = term
	appendEntriesArgs.LeaderId = rf.me
	appendEntriesArgs.PrevLogIndex = int64(starti) - 1
	appendEntriesArgs.PrevLogTerm = rf.getEntryTerm(appendEntriesArgs.PrevLogIndex)
	appendEntriesArgs.LeaderCommit = rf.commitIndex
	appendEntriesArgs.Entries = append(make([]Entry, 0), rf.logs[starti-rf.initIndex:endi-rf.initIndex+1]...)
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.currentTerm != appendEntriesArgs.Term || rf.status != leader {
			rf.mu.Unlock()
			return
		}
		indexDiff:=starti-rf.matchIndex[server]-1
		rf.mu.Unlock()
		if(indexDiff<0){
			return
		}else if(indexDiff>0){
			time.Sleep(time.Duration(indexDiff)*10*time.Microsecond)
		}
		appendEntriesReply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)
		rf.mu.Lock()
		if rf.currentTerm != appendEntriesArgs.Term || rf.status != leader {
			rf.mu.Unlock()
			return
		}
		if ok && appendEntriesReply.Success {
			if endi > rf.matchIndex[server] {
				println("leader", rf.me, "replicate to follower", server, "from", starti, "to", endi, "at", rf.currentTerm) // debug
				rf.matchIndex[server] = endi
			}
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex(term int) {
	rf.mu.Lock()
	var temp []int64
	for rf.status == leader && rf.currentTerm == term {
		// find mid
		temp = append(make([]int64, 0), rf.matchIndex[:]...)
		sort.Slice(temp, func(i, j int) bool {
			return temp[i] < temp[j]
		})
		// len(temp)>1

		var mid int64
		if len(temp)%2 == 0 {
			mid = temp[len(temp)/2-1]
		} else {
			mid = temp[len(temp)/2]
		}
		if mid > rf.commitIndex && rf.getEntryTerm(mid) == term {
			print(rf.me, "update commitIndex from", rf.commitIndex, "to", mid, "at term", rf.currentTerm) // debug
			print(" matchIndex is ")
			for i := 0; i < len(rf.matchIndex); i++ {
				print(rf.matchIndex[i], " ")
			}
			println()                                                   
			rf.commitIndex = mid
		}
		// println(rf.me,"commitIndex is ",rf.commitIndex," mid is ",mid) // debug
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) replicateToServer(server int, term int) {
	rf.mu.Lock()
	appendEntriesArgs := AppendEntriesArgs{}
	appendEntriesArgs.Term = term
	appendEntriesArgs.LeaderId = rf.me
	appendEntriesArgs.PrevLogIndex = rf.nextIndex[server] - 1
	appendEntriesArgs.PrevLogTerm = rf.getEntryTerm(appendEntriesArgs.PrevLogIndex)
	appendEntriesArgs.LeaderCommit = rf.commitIndex
	appendEntriesArgs.Entries = make([]Entry, 0)
	for appendEntriesArgs.Term == rf.currentTerm && rf.status == leader {
		rf.mu.Unlock()
		appendEntriesReply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)
		rf.mu.Lock()
		// println("nextIndex is ",rf.nextIndex[server]," : ",appendEntriesReply.Success) // debug
		if rf.currentTerm != appendEntriesArgs.Term || rf.status != leader {
			rf.mu.Unlock()
			return
		}
		if ok && appendEntriesReply.Success {
			rf.matchIndex[server] = appendEntriesArgs.PrevLogIndex
			break
		}
		if ok && !appendEntriesReply.Success {
			rf.nextIndex[server]-- // dangerous
			appendEntriesArgs.PrevLogIndex--
			appendEntriesArgs.PrevLogTerm = rf.getEntryTerm(appendEntriesArgs.PrevLogIndex)
		}
	}
	rf.mu.Unlock()
	//

	rf.mu.Lock()
	for appendEntriesArgs.Term == rf.currentTerm && rf.status == leader {
		currentLogIndex := rf.initIndex + int64(len(rf.logs)) - 1
		// rf.mu.Unlock()
		if rf.nextIndex[server] <= currentLogIndex {
				go rf.replicateEntries(server, rf.nextIndex[server], currentLogIndex, term)
			// rf.mu.Lock()
			rf.nextIndex[server] = currentLogIndex + 1
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	for !rf.killed() {
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.logs[rf.lastApplied-rf.initIndex].Command
			applyMsg.CommandIndex = int(rf.lastApplied)
			rf.applyCh <- applyMsg
			println(rf.me, "apply", rf.lastApplied, "at term", rf.currentTerm) // debug
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == leader
	if isLeader {
		index = len(rf.logs) + int(rf.initIndex)
		term = rf.currentTerm
		rf.matchIndex[rf.me] = int64(len(rf.logs)) + rf.initIndex
		rf.logs = append(rf.logs, Entry{Term: rf.currentTerm, Command: command})
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) getEntryTerm(index int64) int {
	term := -2
	if index >= rf.initIndex && index < rf.initIndex+int64(len(rf.logs)) {
		term = rf.logs[index-rf.initIndex].Term
	} else if index >= rf.initIndex+int64(len(rf.logs)) {
		term = -1
	}
	return term
}

func (rf *Raft) sendHeartBoots(term int) {
	rf.mu.Lock()
	for rf.currentTerm == term && rf.status == leader {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			appendEntriesArgs := AppendEntriesArgs{}
			appendEntriesArgs.Term = rf.currentTerm
			appendEntriesArgs.LeaderId = rf.me
			appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
			appendEntriesArgs.PrevLogTerm = rf.getEntryTerm(appendEntriesArgs.PrevLogIndex)
			appendEntriesArgs.LeaderCommit = rf.commitIndex
			appendEntriesArgs.Entries = make([]Entry, 0)
			appendEntriesReply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
		}
		rf.mu.Unlock()
		time.Sleep(110 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) getVotes(term int) {
	cond := sync.NewCond(&rf.mu)
	wg := sync.WaitGroup{}
	rf.mu.Lock()
	voteSum := len(rf.peers)
	wg.Add(voteSum - 1)
	for i := 0; i < voteSum; i++ {
		if i == rf.me {
			continue
		}
		go rf.getVote(term, i, cond, &wg)
	}
	for {
		cond.Wait()
		if rf.voteNum*2 > len(rf.peers) && rf.currentTerm == term && rf.status == candidate {
			rf.status = leader
			println(rf.me, "become leader", "at term", rf.currentTerm) // debug
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = int64(len(rf.logs)) + rf.initIndex
			}
			for i := 0; i < len(rf.peers); i++ {
				rf.matchIndex[i] = 0
			}
			go rf.sendHeartBoots(term)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.replicateToServer(i, term)
			}
			go rf.updateCommitIndex(term)
			break
		}
		if rf.currentTerm != term || rf.status != candidate {
			break
		}
	}
	rf.mu.Unlock()
	wg.Wait()
}
func (rf *Raft) getVote(term int, voter int, signalCond *sync.Cond, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := RequestVoteArgs{}
		args.Term = term
		rf.mu.Lock()
		if term != rf.currentTerm || rf.voteNum*2 > len(rf.peers) {
			signalCond.Signal()
			rf.mu.Unlock()
			return
		}
		args.CandidateId = rf.me
		args.LastLogIndex = int64(len(rf.logs)) + rf.initIndex - 1
		// args.lastLogTerm = (len(rf.logs)-1>0)?rf.logs[len(rf.logs)-1].Term:0
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		} else {
			args.LastLogTerm = 0
		}
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		succ := rf.sendRequestVote(voter, &args, &reply)
		if !succ {
			continue
		} else {
			rf.mu.Lock()
			if reply.VoteGranted && rf.currentTerm == term {
				rf.voteNum++
			}
			signalCond.Signal()
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		if rf.status != leader && rf.heartBoot != called {
			rf.status = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.lastNewIndex = rf.commitIndex
			println(rf.me, "start vote", rf.currentTerm) // debug
			// println("leader:", rf.status==leader)
			go rf.getVotes(rf.currentTerm)
		}
		rf.heartBoot = notCalled
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		var ms int64 = int64(1500 + (rand.Int63() % 1500))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// println("me:", me)
	// println("len(peers):", len(peers))
	rf.nextIndex = make([]int64, len(peers))
	rf.matchIndex = make([]int64, len(peers))
	rf.lastNewIndex = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.initIndex = 1
	rf.heartBoot = notCalled
	rf.currentTerm = 0

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply log entries goroutine
	go rf.applyLogEntries()

	return rf
}
