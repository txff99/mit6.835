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
	"fmt"
	"math/rand"

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

const NULL = -1
const ELECTION_TIMEOUT = 400 // millisecond
const HEARTBEAT_INTERVAL = 100

type Log struct {
	Entry interface{}
	Term  int
}

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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	shouldStartElection bool
	serverNum           int
	isLeader            bool
	countVote           int

	log         []Log
	currentTerm int
	votedFor    int

	commitIdx   int
	lastApplied int

	nextIdx    []int
	matchIndex []int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	LeaderCommit int
	IsHeartBeat  bool

	PrevLogTerm int
	Entries     []Log
}

type AppendEntriesResult struct {
	Term    int
	Success bool
}

func (rf *Raft) UpdateTerm(term int) {
	//fmt.Printf("server %d term %d update to %d\n", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.votedFor = NULL
	rf.isLeader = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, result *AppendEntriesResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//fmt.Printf("outdated leader %d term %d\n", args.LeaderId, args.Term)
		result.Term = rf.currentTerm
		result.Success = false
		return
	}
	// if term > currentTerm, convert to follower
	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
	}
	// hear from leader, set startElection to false
	rf.shouldStartElection = false
	if args.IsHeartBeat {
		//fmt.Printf("heart beat sent from %d to %d \n", args.LeaderId, rf.me)
		return
	}
	if GetLastLogIdx(rf.log) < args.PrevLogIdx || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		result.Success = false
		return
	}
	rf.log = rf.log[:args.PrevLogIdx]
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIdx {
		lastIdx := GetLastLogIdx(rf.log)
		if args.LeaderCommit < lastIdx {
			rf.commitIdx = args.LeaderCommit
		} else {
			rf.commitIdx = lastIdx
		}
	}
	result.Success = true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("state of server %d: term %d, is leader: %t\n", rf.me, rf.currentTerm, rf.isLeader)
	return rf.currentTerm, rf.isLeader
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

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
	}
	if rf.votedFor == NULL {
		if GetLastLogIdx(rf.log) >= args.LastLogIndex && rf.log[args.LastLogIndex].Term == args.LastLogTerm {
			//fmt.Printf("%d vote for %d, term %d\n", rf.me, args.CandidateId, rf.currentTerm)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) startElection() {
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%d election start, term %d\n", rf.me, rf.currentTerm)
	rf.countVote = 0
	if rf.votedFor == NULL {
		//fmt.Printf("%d vote for itself\n", rf.me)
		rf.votedFor = rf.me
		rf.countVote++
	}
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i,
			RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
				LastLogIndex: GetLastLogIdx(rf.log),
				LastLogTerm:  rf.log[GetLastLogIdx(rf.log)].Term},
			RequestVoteReply{Term: 0, VoteGranted: false})
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.UpdateTerm(reply.Term)
	}
	if args.Term != rf.currentTerm {
		// avoid vote from the past
		return
	}
	if reply.VoteGranted {
		rf.countVote++
	}
	if rf.countVote >= rf.serverNum/2+1 {
		rf.countVote = 0
		//fmt.Printf("%d won, term %d\n", rf.me, rf.currentTerm)
		rf.isLeader = true
		go rf.sendHeartBeat()
	}
}

func (rf *Raft) sendHeartBeat() {
	for {
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				return
			}
			go rf.sendHeartBeatRPC(i,
				AppendEntriesArgs{Term: rf.currentTerm, IsHeartBeat: true, LeaderId: rf.me},
				AppendEntriesResult{Term: 0})
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeatRPC(server int, args AppendEntriesArgs, reply AppendEntriesResult) {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.UpdateTerm(reply.Term)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		time.Sleep(time.Duration(ELECTION_TIMEOUT) * time.Millisecond)

		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.shouldStartElection && !rf.isLeader {
			rf.currentTerm++
			rf.votedFor = NULL
			go rf.startElection()
		} else {
			rf.shouldStartElection = true
		}
		rf.mu.Unlock()

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
	// Your initialization code here (3A, 3B, 3C).

	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 1)
	rf.log[0] = Log{Entry: nil, Term: 0}

	rf.commitIdx = 0
	rf.lastApplied = 0

	rf.nextIdx = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.shouldStartElection = true
	rf.isLeader = false
	rf.serverNum = len(rf.peers)
	rf.countVote = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
