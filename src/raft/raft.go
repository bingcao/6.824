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
	"labrpc"
	"math/rand"
	"sync"
	"time"
	// "bytes
	// "encoding/gob"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // mutex for entire structure
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           []Log
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	matchLocks    []sync.Mutex
	electionTimer *time.Timer
	state         State
	applyCh       chan ApplyMsg
	goApply       chan bool
	quitElection  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

func (rf *Raft) resetTimer() {
	if rf.state == Leader {
		debug("leader %d shouldn't be here\n", rf.me)
	}
	newTime := rand.Intn(400) + 100
	rf.electionTimer.Reset(time.Millisecond * time.Duration(newTime))
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.state == Leader && rf.currentTerm >= args.Term {
		return
	}

	newTerm := rf.currentTerm

	if args.Term > rf.currentTerm {
		debug("updating server %d's term from %d to %d\n", rf.me, rf.currentTerm, args.Term)
		newTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	//debug("server %d in term %d who's voted for %d received vote request:\n %+v\n", rf.me, rf.currentTerm, rf.votedFor, args)
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		//debug("server %d rejected server %d, and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
	} else {
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)-1) {
			rf.resetTimer()
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			//debug("server %d accepted server %d and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
		} else {
			reply.VoteGranted = false
			//debug("server %d rejected server %d and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
		}
	}

	rf.currentTerm = newTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
}

func (rf *Raft) applyMessages() {
	for rf.commitIndex > rf.lastApplied {
		// TODO: apply log[lastApplied] to state machine
		debug("server %d applying log entry %d\n", rf.me, rf.lastApplied+1)
		rf.lastApplied++
		applyMsg := ApplyMsg{Index: rf.lastApplied + 1, Command: rf.log[rf.lastApplied].Command}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if rf.state == Leader && rf.currentTerm > args.Term {
		debug("INTERESTING: server %d with term %d tried to append %+v to leader %d with term %d?\n", args.LeaderId, args.Term, args.Entries, rf.me, rf.currentTerm)
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		debug("from append entries: ")
		rf.updateTerm(args.Term)
	}
	if len(args.Entries) > 0 {
		debug("REQUEST: server %d with log %+v, term %d, and commit index %d received the following request:\n%+v\n", rf.me, rf.log, rf.currentTerm, rf.commitIndex, args)
	}
	if rf.me != args.LeaderId {
		rf.resetTimer()
	}
	if rf.state == Candidate && rf.currentTerm <= args.Term {
		// TODO: stop waiting for votes
		go func() {
			rf.quitElection <- true
		}()
		rf.state = Follower
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex != -1 && args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
	} else {
		reply.Success = true
		startIndex := 0
		for _, entry := range args.Entries {
			if entry.Index >= len(rf.log) {
				break
			} else if rf.log[entry.Index].Term != entry.Term {
				debug("erasing entries of server %d starting from index %d\n", rf.me, entry.Index)
				rf.log = rf.log[:entry.Index]
				debug("log is now %+v\n", rf.log)
				break
			} else {
				startIndex++
			}
		}
		if startIndex != 0 && rf.me != args.LeaderId {
			debug("INTERESTING CASE!!!! start index is %d\n", startIndex)
			// TODO: is this supposed to ever occur?
		}
		for _, entry := range args.Entries[startIndex:] {
			rf.log = append(rf.log, entry)
		}
		if len(args.Entries) > 0 {
			debug("UPDATE: server %d's new log: %+v\n", rf.me, rf.log)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			rf.goApply <- true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		debug("append to server %d from leader %d failed\n", server, args.LeaderId)
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != Leader {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, Log{term, index, command})
	debug("appended new log entry to leader!\n")
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.matchLocks = make([]sync.Mutex, len(peers))
	rf.state = Follower
	rf.applyCh = applyCh
	rf.goApply = make(chan bool)
	rf.quitElection = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().Unix())
	rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(400)+100))

	go func() {
		// election timer loop
		for _ = range rf.electionTimer.C {
			debug("server %d election timer timed out!\n", rf.me)
			go rf.newElection()
		}
	}()

	go func() {
		for _ = range rf.goApply {
			rf.applyMessages()
		}
	}()

	return rf
}

func (rf *Raft) updateTerm(newTerm int) {
	debug("updating server %d's term from %d to %d\n", rf.me, rf.currentTerm, newTerm)
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) handleMessage(m ApplyMsg) {
	rf.resetTimer()
}

func (rf *Raft) newElection() {
	//debug("server %d starting an election!\n", rf.me)

	rf.mu.Lock()
	rf.resetTimer()
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.state = Candidate
	voteArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1}

	if len(rf.log) == 0 {
		voteArgs.LastLogTerm = 0
	} else {
		voteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	votes := make(chan bool)
	voteCount := 0
	yeses := 0
	electionDone := false
	var electionMu sync.Mutex

	var wg sync.WaitGroup

	for i := range rf.peers {
		wg.Add(1)
		if i == rf.me {
			continue
		}
		go func(follower int) {
			defer wg.Done()
			if rf.sendRequestVote(follower, voteArgs, &reply) {
				//debug("server %d got reply from server %d that's: %+v\n", rf.me, follower, reply)
				electionMu.Lock()
				if electionDone {
					return
				}
				electionMu.Unlock()
				if reply.VoteGranted {
					//debug("server %d accepting a YES vote from server %d\n", rf.me, follower)
					votes <- true
				} else {
					//debug("server %d getting a NO vote from server %d\n", rf.me, follower)
					if reply.Term > currentTerm {
						rf.mu.Lock()
						rf.updateTerm(reply.Term)
						rf.mu.Unlock()
						rf.quitElection <- true
					}
					votes <- false
				}
			}
		}(i)
	}

	go func() {
		// handles closing of votes channel
		wg.Wait()
		close(votes)
	}()

	for {
		if electionDone {
			break
		}
		select {
		case <-rf.quitElection:
			debug("server %d exiting election because term is outdated\n", rf.me)
			electionMu.Lock()
			electionDone = true
			electionMu.Unlock()

		case vote := <-votes:
			//debug("server %d got vote %d\n", rf.me, vote)
			voteCount++
			if vote {
				yeses++
			}
			rf.mu.Lock()
			if yeses == (len(rf.peers)-1)/2 && rf.votedFor == rf.me { // TODO: i feel like the second check is kinda hacky
				debug("server %d becoming leader with %d votes\n", rf.me, yeses)
				rf.state = Leader
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = -1
				}
				rf.electionTimer.Stop()
				go rf.leader()
				electionMu.Lock()
				electionDone = true
				electionMu.Unlock()
			} else if voteCount == len(rf.peers)-1 || rf.votedFor != rf.me {
				debug("server %d exiting election as follower...\n", rf.me)
				electionMu.Lock()
				electionDone = true
				electionMu.Unlock()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) leader() {
	debug("leader %d process spawned\n", rf.me)
	for i := range rf.peers {
		go func(i int) {
			for rf.state == Leader {
				<-time.After(50 * time.Millisecond)

				index := rf.nextIndex[i]
				rf.mu.Lock()
				newEntries := AppendEntriesArgs{rf.currentTerm, rf.me, index - 1, 0, make([]Log, 0), rf.commitIndex}
				if len(rf.log) >= index {
					newEntries.Entries = rf.log[index:]
				}
				if index-1 >= 0 {
					newEntries.PrevLogTerm = rf.log[index-1].Term
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				if !rf.sendAppendEntries(i, newEntries, &reply) {
					debug("send append entries failed for server %d from leader %d\n", i, rf.me)
					// TODO: I think you shouldn't do anything in this case, need to check
				} else {
					//debug("reply: %+v\n", reply)
					if reply.Term > rf.currentTerm {
						if len(newEntries.Entries) == 0 {
							debug("from heartbeat: ")
						} else {
							debug("from append entries reply: ")
						}
						rf.updateTerm(reply.Term)
						rf.resetTimer()
						// TODO: this part seems sketchy. may need to halt more of the leader processes
					} else if reply.Success {
						rf.nextIndex[i] = rf.nextIndex[i] + len(newEntries.Entries)
						rf.matchLocks[i].Lock()
						rf.matchIndex[i] = rf.matchIndex[i] + len(newEntries.Entries)
						rf.matchLocks[i].Unlock()
					} else {
						if index > 0 {
							rf.nextIndex[i]--
						}
					}
				}
			}
		}(i)
	}
	go func() {
		// goroutine to handle updating commits
		for rf.state == Leader {
			rf.mu.Lock()
			log_length := len(rf.log)
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			newCommit := rf.commitIndex
			for i := rf.commitIndex + 1; i < log_length; i++ {
				count := 0
				if rf.log[i].Term < currentTerm {
					continue
				} else if rf.log[i].Term > currentTerm {
					debug("BAD: leader %d at term %d has weird log: %+v\n", rf.me, rf.currentTerm, rf.log)
					break
				}
				for j := range rf.peers {
					rf.matchLocks[j].Lock()
					if rf.matchIndex[j] >= i {
						count++
					}
					rf.matchLocks[j].Unlock()
				}
				if count > (len(rf.peers))/2 {
					debug("incremented leader %d's commit index from %d to %d, had %d/%d agrees\n", rf.me, newCommit, i, count, len(rf.peers))
					newCommit = i
				} else {
					break
				}
			}
			if newCommit != rf.commitIndex {
				rf.commitIndex = newCommit
				rf.goApply <- true
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()
}

func (rf *Raft) heartbeat(follower int) {
	//debug("leader %d sent heartbeat", rf.me)
	heartbeatArgs := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[follower] - 1, 1, make([]Log, 0), 0}
	if rf.nextIndex[follower]-1 > 0 {
		heartbeatArgs.PrevLogTerm = rf.log[rf.nextIndex[follower]-1].Term
		heartbeatArgs.LeaderCommit = min(rf.commitIndex, rf.nextIndex[follower]-1)
	}
	var heartbeatReply AppendEntriesReply
	if rf.sendAppendEntries(follower, heartbeatArgs, &heartbeatReply) {
		if heartbeatReply.Term > rf.currentTerm {
			debug("leader %d resetting to follower from heartbeat\n", rf.me)
			rf.mu.Lock()
			rf.updateTerm(heartbeatReply.Term)
			rf.mu.Unlock()
			rf.resetTimer()
		}
	}
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

// Debugging enabled?
const debugEnabled = true

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}
