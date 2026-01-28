package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"6.5840/labgob"
	"bytes"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	Leader    int32 = 1
	Candidate int32 = 2
	Follower  int32 = 3
)

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role int32

	voteCond        *sync.Cond
	electionCond    *sync.Cond
	electionTimer   time.Time
	heartbeatPeriod time.Duration // Millisecond

	applyCond *sync.Cond

	// persistent state
	currentTerm       int
	votedFor          int
	lastIncludedTerm  int
	lastIncludedIndex int
	logs              []Log
	snapshot          []byte

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	applyCh    chan raftapi.ApplyMsg
	applyExist chan struct{}
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
	isleader = rf.role == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedTerm int
	var lastIncludedIndex int
	var logs []Log
	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&lastIncludedTerm)
	d.Decode(&lastIncludedIndex)
	d.Decode(&logs)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.logs = logs
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	term := rf.getTermOfIndex(index)
	rf.retainAfterIndex(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
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

	// term less than currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// term equal currentTerm
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// check log up-to-date
	var isUpToDate bool
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	if args.LastLogTerm > lastLogTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		isUpToDate = args.LastLogIndex >= lastLogIndex
	}

	if args.Term > rf.currentTerm || isUpToDate {
		rf.role = Follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		if isUpToDate {
			rf.votedFor = args.CandidateId
			// only when granted reset election time out
			// randomization electionTimeout
			rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
		}
		rf.persist()
		rf.electionCond.Broadcast()
		// grant vote
		if isUpToDate {
			reply.VoteGranted = true
		}
	}
	return
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
	return ok
}

func (rf *Raft) sendVote() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	term := rf.currentTerm
	lastLogTerm, lastLogIndex := rf.getLastLogTerm(), rf.getLastLogIndex()

	// send vote and collection grant
	grant := 1 // self
	grantMap := make(map[int]interface{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			ok := rf.sendRequestVote(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.currentTerm != term || rf.role != Candidate {
					return
				}
				if reply.VoteGranted {
					if _, exist := grantMap[peer]; !exist {
						grant++
					}
					if grant > len(rf.peers)/2 {
						rf.role = Leader
						// init nextIndex & matchIndex
						for j := range rf.nextIndex {
							rf.nextIndex[j] = rf.getLastLogIndex() + 1
							rf.matchIndex[j] = 0
						}
						// send heartbeat immediately
						rf.sendAppendEntries()
						go rf.sendHeartbeat(rf.currentTerm)
					}
				} else {
					if _, exist := grantMap[peer]; exist {
						grant--
					}
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						// randomization electionTimeout
						rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
						// notify
						rf.electionCond.Broadcast()
					}
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for accelerated log backtracking optimization
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.role = Follower
	// randomization electionTimeout
	rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
	rf.electionCond.Broadcast()

	var needPersist bool
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
	}

	l := rf.getLastLogIndex() + 1
	if l <= args.PrevLogIndex {
		reply.ConflictIndex = l
		return
	}

	//  when this happen
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictTerm = -1
		return
	}

	if rf.getTermOfIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.getTermOfIndex(args.PrevLogIndex)
		// the first index whose entry has term equal to conflictTerm
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.getTermOfIndex(i) != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex = i
		}
	} else {
		reply.Success = true
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		for i := range args.Entries {
			if l-1 < args.PrevLogIndex+i+1 {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				needPersist = true
				break
			}
			// if an existing entry conflicts with a new one
			if args.Entries[i].Term != rf.getTermOfIndex(args.PrevLogIndex+i+1) {
				rf.retainBeforeIndex(args.PrevLogIndex + i + 1)
				rf.logs = append(rf.logs, args.Entries[i:]...)
				needPersist = true
				break
			}
		}
		// update commit index
		commitIndex := min(lastNewIndex, args.LeaderCommit)
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.applyCond.Broadcast()
		}
	}
	return
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.role = Follower
	rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
	rf.electionCond.Broadcast()

	var needPersist bool
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
	}

	if args.LastIncludedIndex > rf.lastIncludedIndex {
		needPersist = true
		if rf.getLastLogIndex() < args.LastIncludedIndex {
			rf.logs = []Log{}
		} else {
			if rf.getTermOfIndex(args.LastIncludedIndex) != args.LastIncludedTerm {
				rf.logs = []Log{}
			} else {
				rf.retainAfterIndex(args.LastIncludedIndex)
			}
		}

		rf.snapshot = args.Data
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex

		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		term := rf.currentTerm
		nextIndex := rf.nextIndex[i]

		if nextIndex > rf.lastIncludedIndex {
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.getTermOfIndex(prevLogIndex)
			logs := rf.getLogsFromIndex(nextIndex)
			cpLogs := make([]Log, len(logs))
			copy(cpLogs, logs)
			leaderCommit := rf.commitIndex

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      cpLogs,
				LeaderCommit: leaderCommit,
			}

			go func(peer int) {
				reply := &AppendEntriesReply{}
				// call rpc
				ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// an rough implementation without dealing the  rcp fail case,just for learning purpose
				if ok {
					if rf.currentTerm != term || rf.role != Leader {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						// randomization electionTimeout
						rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
						rf.persist()
						rf.electionCond.Broadcast()
						return
					}

					if reply.Success {
						matchIndex := args.PrevLogIndex + len(args.Entries)
						if matchIndex > rf.matchIndex[peer] {
							rf.matchIndex[peer] = matchIndex
							rf.nextIndex[peer] = matchIndex + 1
							rf.updateLeaderCommitIndex()
						}
						return
					}

					if reply.ConflictTerm < 0 {
						return
					}

					if reply.ConflictTerm == 0 {
						rf.nextIndex[peer] = reply.ConflictIndex
						return
					}

					// accelerated log backtracking
					var conflictTermFound bool
					var conflictIndex int
					for j := args.PrevLogIndex; j >= rf.lastIncludedIndex; j-- {
						if rf.getTermOfIndex(j) == reply.ConflictTerm {
							conflictTermFound = true
							conflictIndex = j + 1
							break
						}
					}
					if conflictTermFound {
						rf.nextIndex[peer] = conflictIndex
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				}
			}(i)
		} else {
			snapshot := rf.snapshot
			lastIncludedIndex := rf.lastIncludedIndex
			lastIncludedTerm := rf.lastIncludedTerm

			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              snapshot,
			}

			go func(peer int) {
				reply := &InstallSnapshotReply{}
				// call rpc
				ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if rf.currentTerm != term || rf.role != Leader {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						// randomization electionTimeout
						rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
						rf.persist()
						rf.electionCond.Broadcast()
						return
					}

					matchIndex := args.LastIncludedIndex
					if matchIndex > rf.matchIndex[peer] {
						rf.matchIndex[peer] = matchIndex
						rf.nextIndex[peer] = matchIndex + 1
						rf.updateLeaderCommitIndex()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	quorum := len(rf.peers) / 2
	commitIndex := matchIndex[quorum]
	if commitIndex > rf.commitIndex && rf.getTermOfIndex(commitIndex) == rf.currentTerm {
		rf.commitIndex = commitIndex
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendHeartbeat(term int) {
	for rf.killed() == false {
		select {
		case <-time.After(rf.heartbeatPeriod):
			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.sendAppendEntries()
			rf.mu.Unlock()
		}
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
	isLeader = rf.role == Leader
	if isLeader {
		term = rf.currentTerm
		rf.logs = append(rf.logs, Log{
			Term:    term,
			Command: command,
		})
		index = rf.getLastLogIndex()
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.sendAppendEntries()
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
	rf.mu.Lock()
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
	rf.voteCond.Broadcast()
	rf.electionCond.Broadcast()
	rf.mu.Unlock()
	// Your code here, if desired.
	select {
	case <-rf.applyExist:
	}
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(200)+300) * time.Millisecond
}

func (rf *Raft) getLastLogTerm() int {
	l := len(rf.logs)
	if l > 0 {
		return rf.logs[l-1].Term
	}
	return rf.lastIncludedTerm
}

func (rf *Raft) getLastLogIndex() int {
	if rf.lastIncludedIndex > 0 {
		return rf.lastIncludedIndex + len(rf.logs)
	}
	return len(rf.logs) - 1
}

func (rf *Raft) getTermOfIndex(index int) int {
	// index must >= rf.lastIncludedIndex
	if rf.lastIncludedIndex == 0 {
		return rf.logs[index].Term
	}
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.logs[index-rf.lastIncludedIndex-1].Term
}

func (rf *Raft) retainAfterIndex(index int) {
	if rf.lastIncludedIndex > 0 {
		rf.logs = rf.logs[index-rf.lastIncludedIndex:]
	} else {
		rf.logs = rf.logs[index+1:]
	}
	return
}

func (rf *Raft) retainBeforeIndex(index int) {
	// index must > rf.lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		rf.logs = rf.logs[:index-rf.lastIncludedIndex-1]
	} else {
		rf.logs = rf.logs[:index]
	}
}

func (rf *Raft) getLogOfIndex(index int) Log {
	// index must > rf.lastIncludedIndex
	if rf.lastIncludedIndex == 0 {
		return rf.logs[index]
	}
	return rf.logs[index-rf.lastIncludedIndex-1]
}

func (rf *Raft) getLogsFromIndex(index int) []Log {
	// index must > rf.lastIncludedIndex
	if rf.lastIncludedIndex == 0 {
		return rf.logs[index:]
	}
	return rf.logs[index-rf.lastIncludedIndex-1:]
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		for rf.role != Follower && !rf.killed() {
			rf.electionCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		d := rf.electionTimer.Sub(time.Now())
		if d <= 0 {
			rf.role = Candidate
			rf.voteCond.Broadcast()
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		select {
		case <-time.After(d):
		}
	}
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.role != Candidate && !rf.killed() {
			rf.voteCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.sendVote()
		rf.mu.Unlock()
		time.Sleep(rf.getElectionTimeout())
	}
}

func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		var lastApplied int
		if rf.lastIncludedIndex > 0 && rf.lastApplied < rf.lastIncludedIndex {
			lastApplied = rf.lastIncludedIndex
			snapshot := rf.snapshot
			snapshotTerm := rf.lastIncludedTerm
			SnapShotIndex := rf.lastIncludedIndex
			rf.mu.Unlock()
			applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  snapshotTerm,
				SnapshotIndex: SnapShotIndex,
			}
		} else {
			lastApplied = rf.lastApplied + 1
			log := rf.getLogOfIndex(lastApplied)
			rf.mu.Unlock()
			applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: lastApplied,
			}
		}
		rf.mu.Lock()
		if lastApplied > rf.lastApplied {
			rf.lastApplied = lastApplied
		}
		rf.mu.Unlock()
	}
	rf.applyExist <- struct{}{}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = Follower
	rf.votedFor = -1
	// randomization electionTimeout
	rf.electionTimer = time.Now().Add(rf.getElectionTimeout())
	rf.heartbeatPeriod = time.Millisecond * 100
	rf.voteCond = sync.NewCond(&rf.mu)
	rf.electionCond = sync.NewCond(&rf.mu)
	rf.logs = []Log{{}} // index start at sequence 1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyExist = make(chan struct{})
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.commitIndex = 0
	rf.lastApplied = 0

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.startElection()
	go rf.applier(applyCh)

	return rf
}
