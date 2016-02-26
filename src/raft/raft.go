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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
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
	CurrentTerm   int
	VotedFor      int
	Log           []Log
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	electionTimer *time.Timer
	state         State
	applyCh       chan ApplyMsg
	goApply       chan bool
	quitElection  chan bool
	killCh        chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(Raft{CurrentTerm: rf.CurrentTerm, VotedFor: rf.VotedFor, Log: rf.Log})
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf)
	if rf.VotedFor == 0 && len(rf.Log) == 0 && rf.CurrentTerm == 0 {
		// kinda hacky way to make sure I initalize voted for to -1
		rf.VotedFor = -1
	}
}

func (rf *Raft) resetTimer() {
	if rf.state == Leader {
		debug("WHATWHATWHAT: leader %d shouldn't be here\n", rf.me)
	}
	newTime := rand.Intn(500) + 100
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

	reply.Term = rf.CurrentTerm

	if rf.state == Leader && rf.CurrentTerm >= args.Term {
		return
	}

	newTerm := rf.CurrentTerm
	// just so we don't reset twice
	reset := false

	if args.Term > rf.CurrentTerm {
		debug("updating server %d's term from %d to %d\n", rf.me, rf.CurrentTerm, args.Term)
		newTerm = args.Term
		rf.state = Follower
		rf.VotedFor = -1
		rf.resetTimer()
		reset = true
	}

	debug("server %d in term %d with status %d who's voted for %d received vote request:\n %+v\n", rf.me, rf.CurrentTerm, rf.state, rf.VotedFor, args)
	if args.Term < rf.CurrentTerm || (rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) {
		reply.VoteGranted = false
		//debug("server %d rejected server %d for term outdated or votedfor, and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
	} else {
		lastLogTerm := 0
		if len(rf.Log) > 0 {
			lastLogTerm = rf.Log[len(rf.Log)-1].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.Log)-1) {
			if !reset {
				if rf.state == Leader {
					debug("WARNING: I'm resetting my timer as leader from Request Vote RPC from %d with term %d", args.CandidateId, args.Term)
				}
				rf.resetTimer()
			}
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			//debug("server %d accepted server %d and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
		} else {
			reply.VoteGranted = false
			//debug("server %d rejected server %d for out of date log and vote granted set to %d\n", rf.me, args.CandidateId, reply.VoteGranted)
		}
	}

	rf.CurrentTerm = newTerm
	rf.persist()
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
	Term               int
	Success            bool
	ConflictTerm       int
	FirstConflictIndex int
}

func (rf *Raft) applyMessages() {
	go func(offset int, end int, log []Log) {
		for i := 0; offset+i <= end; i++ {
			applyMsg := ApplyMsg{Index: offset + i + 1, Command: log[offset+i].Command}
			debug("server %d applying log entry %d\n", rf.me, offset+i)
			rf.applyCh <- applyMsg
		}
	}(rf.lastApplied+1, rf.commitIndex, rf.Log)
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) findConflictIndex(conflictTerm int) int {
	firstConflictIndex := len(rf.Log) - 1
	for i := len(rf.Log) - 2; i >= 0; i-- {
		if rf.Log[i].Term < conflictTerm {
			break
		}
		firstConflictIndex--
	}
	// make sure it's at least 0
	return max(firstConflictIndex, 0)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if rf.state != Leader {
		rf.resetTimer()
	}
	if rf.state == Leader && rf.CurrentTerm > args.Term {
		//debug("INTERESTING: server %d with term %d tried to append %+v to leader %d with term %d?\n", args.LeaderId, args.Term, args.Entries, rf.me, rf.CurrentTerm)
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		debug("from append entries: ")
		rf.updateTerm(args.Term)
	}

	if len(args.Entries) > 0 {
		debug("REQUEST: server %d with log of length %d, term %d, and commit index %d received append request from leader %d with Term %d, CommitIndex %d, PrevLogIndex %d, and PrevLogTerm %d\n", rf.me, len(rf.Log), rf.CurrentTerm, rf.commitIndex, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm)
		//debug("REQUEST: server %d with log %+v, term %d, and commit index %d received the following request:\n%+v\n", rf.me, rf.Log, rf.CurrentTerm, rf.commitIndex, args)
	}

	if rf.state != Leader {
		rf.resetTimer()
	}

	if rf.state == Candidate && rf.CurrentTerm <= args.Term {
		// TODO: stop waiting for votes
		go func() {
			rf.quitElection <- true
		}()
		rf.state = Follower
	}

	if args.Term < rf.CurrentTerm {
		reply.Success = false
	} else if args.PrevLogIndex >= len(rf.Log) || (args.PrevLogIndex != -1 && args.PrevLogIndex < len(rf.Log) && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		if len(rf.Log) == 0 {
			reply.ConflictTerm = 0
		} else if args.PrevLogIndex >= len(rf.Log) {
			reply.ConflictTerm = min(args.PrevLogTerm, rf.Log[len(rf.Log)-1].Term+1)
		} else {
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		}
		reply.FirstConflictIndex = rf.findConflictIndex(reply.ConflictTerm)
		reply.Success = false
	} else {
		reply.Success = true
		startIndex := 0
		for _, entry := range args.Entries {
			if entry.Index >= len(rf.Log) {
				break
			} else if rf.Log[entry.Index].Term != entry.Term {
				debug("erasing entries of server %d starting from index %d\n", rf.me, entry.Index)
				rf.Log = rf.Log[:entry.Index]
				//debug("log is now %+v\n", rf.Log)
				break
			} else {
				startIndex++
			}
		}
		if startIndex != 0 && rf.me != args.LeaderId {
			//debug("INTERESTING CASE!!!! start index is %d\n", startIndex)
		}
		for _, entry := range args.Entries[startIndex:] {
			rf.Log = append(rf.Log, entry)
		}
		rf.persist()
		if len(args.Entries) > 0 {
			//debug("UPDATE: server %d's new log: %+v\n", rf.me, rf.Log)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
			rf.applyMessages()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := len(rf.Log)
	term := rf.CurrentTerm
	rf.Log = append(rf.Log, Log{term, index, command})
	rf.persist()
	//debug("NOTICE: appended new log entry to leader!\n")
	return index + 1, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killCh <- true
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
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.applyCh = applyCh
	rf.goApply = make(chan bool)
	rf.quitElection = make(chan bool)
	rf.killCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().Unix())
	rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(500)+100))

	go func() {
		// election timer loop
		for {
			select {
			case <-rf.electionTimer.C:
				debug("server %d election timer timed out!\n", rf.me)
				go rf.newElection()
			case <-rf.killCh:
				rf.killCh <- true
				return
			}
		}
	}()
	return rf
}

func (rf *Raft) updateTerm(newTerm int) {
	debug("updating server %d's term from %d to %d\n", rf.me, rf.CurrentTerm, newTerm)
	rf.CurrentTerm = newTerm
	rf.state = Follower
	rf.VotedFor = -1
	rf.persist()
}

func (rf *Raft) handleMessage(m ApplyMsg) {
	rf.resetTimer()
}

func (rf *Raft) newElection() {

	rf.mu.Lock()
	rf.resetTimer()
	rf.CurrentTerm += 1
	debug("NEW ELECTION: server %d starting an election in term %d!\n", rf.me, rf.CurrentTerm)
	currentTerm := rf.CurrentTerm
	rf.VotedFor = rf.me
	rf.persist()
	rf.state = Candidate
	voteArgs := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me, LastLogIndex: len(rf.Log) - 1}

	if len(rf.Log) == 0 {
		voteArgs.LastLogTerm = 0
	} else {
		voteArgs.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	rf.mu.Unlock()

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
			var reply RequestVoteReply
			if rf.sendRequestVote(follower, voteArgs, &reply) {
				debug("server %d got reply from server %d that's: %+v\n", rf.me, follower, reply)
				electionMu.Lock()
				if electionDone {
					return
				}
				electionMu.Unlock()
				if reply.VoteGranted {
					debug("server %d accepting a YES vote from server %d\n", rf.me, follower)
					votes <- true
				} else {
					debug("server %d getting a NO vote from server %d\n", rf.me, follower)
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
			if yeses == (len(rf.peers)-1)/2 && rf.VotedFor == rf.me { // TODO: i feel like the second check is kinda hacky
				debug("server %d becoming leader with %d votes\n", rf.me, yeses)
				rf.state = Leader
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.Log)
					rf.matchIndex[i] = -1
				}
				rf.electionTimer.Stop()
				go rf.leader()
				electionMu.Lock()
				electionDone = true
				electionMu.Unlock()
			} else if voteCount == len(rf.peers)-1 || rf.VotedFor != rf.me {
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
	debug("LEADER %d process spawned\n", rf.me)
	for i := range rf.peers {
		go func(i int) {
			for {

				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()

				if state == Leader {

					rf.mu.Lock()
					index := rf.nextIndex[i]
					newEntries := AppendEntriesArgs{rf.CurrentTerm, rf.me, index - 1, 0, make([]Log, 0), rf.commitIndex}
					if index < 0 {
						debug("WEIRDWEIRDWIRD: leader %d of term %d has next index %d of server %d, log is %+v\n", rf.me, rf.CurrentTerm, rf.nextIndex[i], i, rf.Log)
					}
					if len(rf.Log) >= index {
						newEntries.Entries = rf.Log[index:]
					}
					if index-1 >= 0 {
						if index-1 >= len(rf.Log) {
							debug("FAIL: next index %d of server %d weird, my log is %+v\n", index, i, rf.Log)
						}
						newEntries.PrevLogTerm = rf.Log[index-1].Term
					}
					rf.mu.Unlock()

					var reply AppendEntriesReply
					if !rf.sendAppendEntries(i, newEntries, &reply) {
						//debug("send append entries failed for server %d from leader %d\n", i, rf.me)
						// TODO: I think you shouldn't do anything in this case, need to check
					} else {
						/*if len(newEntries.Entries) > 0 {
							debug("REPLY from server %d: %+v\n", i, reply)
						}*/

						rf.mu.Lock()
						if reply.Term > rf.CurrentTerm {
							if len(newEntries.Entries) == 0 {
								debug("from heartbeat: ")
							} else {
								debug("from append entries reply: ")
							}
							rf.updateTerm(reply.Term)
							rf.resetTimer()
							// TODO: this part seems sketchy. may need to halt more of the leader processes
						} else if reply.Success {
							rf.nextIndex[i] = min(rf.nextIndex[i]+len(newEntries.Entries), len(rf.Log))
							rf.matchIndex[i] = rf.nextIndex[i] - 1
							if len(newEntries.Entries) > 0 {
								debug("old nextIndex for server %d was %d, now %d\n", i, rf.nextIndex[i]-len(newEntries.Entries), rf.nextIndex[i])
								debug("old matchIndex for server %d was %d, now %d\n", i, rf.matchIndex[i]-len(newEntries.Entries), rf.matchIndex[i])
							}
						} else {
							if index > 0 {
								newIndex := max(rf.nextIndex[i]-1, 0)
								for ; newIndex > 0; newIndex-- {
									if rf.Log[newIndex].Term == reply.ConflictTerm {
										break
									}
								}
								if reply.FirstConflictIndex < 0 && newIndex < 0 {
									debug("WHATWHATWHAT: leader %d got double -1s for server %d, previously %d\n", rf.me, i, rf.nextIndex[i])
								}
								rf.nextIndex[i] = min(reply.FirstConflictIndex, newIndex)
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							}
						}
						rf.mu.Unlock()

					}
				} else {
					break
				}
				<-time.After(75 * time.Millisecond)
			}
		}(i)
	}
	go func() {
		// goroutine to handle updating commits
		for {
			rf.mu.Lock()
			if rf.state == Leader {
				log_length := len(rf.Log)
				currentTerm := rf.CurrentTerm

				newCommit := rf.commitIndex
				//debug("leader %d at term %d trying to update commit index which is %d\n", rf.me, rf.CurrentTerm, rf.commitIndex)
				for i := rf.commitIndex + 1; i < log_length; i++ {
					count := 0
					if rf.Log[i].Term < currentTerm {
						continue
					} else if rf.Log[i].Term > currentTerm {
						debug("BAD: leader %d at term %d has weird log: %+v\n", rf.me, rf.CurrentTerm, rf.Log)
						break
					}
					for j := range rf.peers {
						if rf.matchIndex[j] >= i {
							count++
						}
					}
					//debug("count for above log was %d\n", count)
					if count > (len(rf.peers))/2 {
						debug("incremented leader %d's commit index from %d to %d, had %d/%d agrees\n", rf.me, newCommit, i, count, len(rf.peers))
						newCommit = i
					} else {
						break
					}
				}
				if newCommit != rf.commitIndex {
					rf.commitIndex = newCommit
					rf.applyMessages()
				}
			} else {
				break
			}

			rf.mu.Unlock()
			<-time.After(100 * time.Millisecond)
		}
		rf.mu.Unlock()
	}()
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}
