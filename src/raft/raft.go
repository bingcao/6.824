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
	// "fmt"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// asdfasdf
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Term        int
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
	CurrentTerm int
	VotedFor    int
	Log         []Log
	// index of highest entry that was committed, is not reset upon snapshots
	commitIndex int
	// index of highest entry that was applied, is not reset upon snapshots
	lastApplied int
	// index of next log entry to send to that server, initially leader last log index + 1
	nextIndex []int
	// index of highest log entry known to match, initially 0
	matchIndex     []int
	electionTimer  *time.Timer
	state          State
	applyCh        chan ApplyMsg
	goApply        chan bool
	quitElectionCh chan bool
	killCh         chan bool
	SnapshotIndex  int
	SnapshotTerm   int
}

func (rf *Raft) toLocal(index int) int {
	return index - rf.SnapshotIndex - 1
}

func (rf *Raft) toGlobal(index int) int {
	return index + rf.SnapshotIndex + 1
}

func (rf *Raft) getFullLogLength() int {
	return len(rf.Log) + rf.SnapshotIndex + 1
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
	e.Encode(Raft{CurrentTerm: rf.CurrentTerm, VotedFor: rf.VotedFor, Log: rf.Log, SnapshotIndex: rf.SnapshotIndex, SnapshotTerm: rf.SnapshotTerm})
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
	if rf.VotedFor == 0 && len(rf.Log) == 0 && rf.CurrentTerm == 0 && rf.SnapshotIndex == 0 {
		// kinda hacky way to make sure I initalize voted for to -1
		rf.VotedFor = -1
	}
}

func (rf *Raft) resetTimer() {
	if rf.state == Leader {
		debug("WHATWHATWHAT: leader %d shouldn't be here\n", rf.me)
	}
	newTime := rand.Intn(250) + 250
	rf.electionTimer.Reset(time.Millisecond * time.Duration(newTime))
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

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	debug("server %d entering install snapshot with log of length %d and snapshot index %d, last included index is %d\n", rf.me, len(rf.Log), rf.SnapshotIndex, args.LastIncludedIndex)
	reply.Term = rf.CurrentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sendSnap := false
	if args.Term < rf.CurrentTerm {
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.updateTerm(args.Term)
	}
	rf.resetTimer()
	if rf.toLocal(args.LastIncludedIndex) < len(rf.Log) && rf.toLocal(args.LastIncludedIndex) >= 0 && rf.Log[rf.toLocal(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		toSave := rf.Log[rf.toLocal(args.LastIncludedIndex+1):]
		rf.Log = make([]Log, len(rf.Log)-rf.toLocal(args.LastIncludedIndex+1))
		for i := 0; i < len(toSave); i++ {
			rf.Log[i] = toSave[i]
		}
	} else {
		rf.Log = make([]Log, 0)
		sendSnap = true
	}
	rf.SnapshotIndex = args.LastIncludedTerm
	rf.SnapshotIndex = args.LastIncludedIndex

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
		sendSnap = true
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if sendSnap {
		rf.persister.SaveSnapshot(args.Data)
		var empty struct{}
		rf.applyCh <- ApplyMsg{0, 0, empty, true, args.Data}
	}
	debug("Server %d returning from install snapshot\n", rf.me)
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

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
		if rf.state == Candidate {
			go rf.quitElection(rf.CurrentTerm)
		}
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
		lastLogTerm := rf.SnapshotTerm
		if len(rf.Log) > 0 {
			lastLogTerm = rf.Log[len(rf.Log)-1].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.getFullLogLength()) {
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) applyMessages() {
	go func(offset int, end int, log []Log) {
		for i := 0; offset+i <= end; i++ {
			applyMsg := ApplyMsg{Index: log[offset+i].Index + 1, Term: log[offset+i].Term, Command: log[offset+i].Command}
			debug("server %d applying log entry %d\n", rf.me, log[offset+i].Index)
			rf.applyCh <- applyMsg
		}
	}(rf.toLocal(rf.lastApplied+1), rf.toLocal(rf.commitIndex), rf.Log)
	rf.lastApplied = rf.commitIndex
}

// finds first index in the log that has term equal to conflictTerm, which is defined to be the last term we know conflicts between a leader and follower
func (rf *Raft) findConflictIndex(conflictTerm int) int {
	conflictIndex := len(rf.Log) - 1
	for i := len(rf.Log) - 2; i >= 0; i-- {
		if rf.Log[i].Term < conflictTerm {
			break
		}
		conflictIndex--
	}
	// make sure it's at least 0
	return rf.toGlobal(max(conflictIndex, 0))
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if len(args.Entries) > 0 {
		debug("REQUEST: server %d with state %d, log of length %d, term %d, commit index %d, and snapshot index %d received append request from leader %d of %d entries with Term %d, CommitIndex %d, PrevLogIndex %d, and PrevLogTerm %d; first index in log is %d, last is %d\n", rf.me, rf.state, len(rf.Log), rf.CurrentTerm, rf.commitIndex, rf.SnapshotIndex, args.LeaderId, len(args.Entries), args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index)
	}
	if rf.state != Leader {
		rf.resetTimer()
	}
	if rf.state == Leader && rf.CurrentTerm > args.Term {
		debug("INTERESTING: server %d with term %d tried to append %+v to leader %d with term %d?\n", args.LeaderId, args.Term, args.Entries, rf.me, rf.CurrentTerm)
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		debug("from append entries: ")
		rf.updateTerm(args.Term)
	}

	if rf.state != Leader {
		rf.resetTimer()
	}

	if rf.state == Candidate && rf.CurrentTerm <= args.Term {
		go rf.quitElection(rf.CurrentTerm)
		rf.state = Follower
	}

	if args.Term < rf.CurrentTerm {
		debug("SETTING REPLY TO FALSE DUE TO TERMS\n")
		reply.Success = false
	} else if rf.toLocal(args.PrevLogIndex) >= len(rf.Log) || (rf.toLocal(args.PrevLogIndex) >= 0 && rf.Log[rf.toLocal(args.PrevLogIndex)].Term != args.PrevLogTerm) {
		debug("server %d trying to find conflict index with log of length %d, snapshot index %d, and prev log index %d\n", rf.me, len(rf.Log), rf.SnapshotIndex, args.PrevLogIndex)
		if rf.toLocal(args.PrevLogIndex) >= len(rf.Log) {
			reply.ConflictIndex = rf.getFullLogLength()
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.Log[rf.toLocal(args.PrevLogIndex)].Term
			reply.ConflictIndex = rf.findConflictIndex(reply.ConflictTerm)
		}
		debug("SETTING REPLY TO FALSE DUE TO LOGS, conflict index %d and conflict term %d\n", reply.ConflictIndex, reply.ConflictTerm)
		reply.Success = false
	} else {
		reply.Success = true
		startIndex := 0
		for _, entry := range args.Entries {
			if rf.toLocal(entry.Index) < 0 {
				continue
			} else if rf.toLocal(entry.Index) < 0 {
				debug("FAILFAILFAIL: server %d has snapshot index %d, considering an entry with index %d from leader %d\n", rf.me, rf.SnapshotIndex, entry.Index, args.LeaderId)
			}
			if rf.toLocal(entry.Index) >= len(rf.Log) {
				break
			} else if rf.Log[rf.toLocal(entry.Index)].Term != entry.Term {
				debug("erasing entries of server %d starting from index %d\n", rf.me, rf.toLocal(entry.Index))
				rf.Log = rf.Log[:rf.toLocal(entry.Index)]
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
			rf.commitIndex = min(args.LeaderCommit, rf.getFullLogLength()-1)
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

	index := rf.getFullLogLength()
	term := rf.CurrentTerm
	rf.Log = append(rf.Log, Log{term, index, command})
	rf.persist()
	debug("NOTICE: appended new log entry to leader %d with index %d: %+v!\n", rf.me, index, command)
	<-time.After(10 * time.Millisecond)
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
	rf.electionTimer.Stop()
	rf.killCh <- true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
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
	rf.quitElectionCh = make(chan bool)
	rf.killCh = make(chan bool, 2)
	rf.SnapshotTerm = 0
	rf.SnapshotIndex = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().Unix())
	rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(250)+250))

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

func (rf *Raft) Snapshot(snapshotIndex int, snapshotTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex -= 1
	debug("snapshotting server %d with index %d and term %d, server has length %d and snapshot index %d, consider last index %d\n", rf.me, snapshotIndex, snapshotTerm, len(rf.Log), rf.SnapshotIndex, rf.toLocal(snapshotIndex))
	lastIndex := rf.toLocal(snapshotIndex)
	if snapshotIndex >= 0 && snapshotTerm >= 0 {
		if lastIndex+1 < len(rf.Log) && lastIndex >= 0 {
			rf.SnapshotIndex = rf.Log[lastIndex].Index
			rf.SnapshotTerm = rf.Log[lastIndex].Term
			toSave := rf.Log[lastIndex+1:]
			rf.Log = make([]Log, len(rf.Log)-lastIndex-1)
			for i := 0; i < len(toSave); i++ {
				rf.Log[i] = toSave[i]
			}
		} else if lastIndex >= 0 {
			rf.Log = make([]Log, 0)
			rf.SnapshotIndex = snapshotIndex
			rf.SnapshotTerm = snapshotTerm
		}
		rf.persist()
	}
	if rf.lastApplied < rf.SnapshotIndex {
		rf.lastApplied = rf.SnapshotIndex
	}
	if rf.commitIndex < rf.SnapshotIndex {
		rf.commitIndex = rf.SnapshotIndex
	}
	if rf.state == Leader {
		for i := 0; i < len(rf.nextIndex); i++ {
			if rf.nextIndex[i] > rf.getFullLogLength() {
				rf.nextIndex[i] = rf.SnapshotIndex + 1
			}
		}
		/*if rf.nextIndex[rf.me] <= rf.SnapshotIndex {
			debug("leader %d next index forwarded from %d to %d\n", rf.me, rf.nextIndex[rf.me], rf.SnapshotIndex+1)
			rf.nextIndex[rf.me] = rf.SnapshotIndex + 1
		}*/
	}
	debug("snapshotting server %d returned with new log length %d, snapshot index of %d, snapshot term of %d\n", rf.me, len(rf.Log), rf.SnapshotIndex, rf.SnapshotTerm)
}

func (rf *Raft) updateTerm(newTerm int) {
	debug("updating server %d's term from %d to %d\n", rf.me, rf.CurrentTerm, newTerm)
	rf.CurrentTerm = newTerm
	rf.state = Follower
	rf.VotedFor = -1
	rf.persist()
}

func (rf *Raft) quitElection(curTerm int) {
	for {
		select {
		case rf.quitElectionCh <- true:
			return
		default:
			if rf.CurrentTerm > curTerm || rf.state != Candidate {
				return
			}
		}
	}
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
	voteArgs := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me, LastLogIndex: rf.getFullLogLength()}

	if len(rf.Log) == 0 {
		voteArgs.LastLogTerm = rf.SnapshotTerm
	} else {
		voteArgs.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	rf.mu.Unlock()

	votes := make(chan bool, len(rf.peers)-1)
	voteCount := 0
	yeses := 0
	electionDone := false
	var electionMu sync.Mutex

	var wg sync.WaitGroup

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(follower int) {
			defer wg.Done()
			var reply RequestVoteReply
			debug("server %d sending request vote to server %d\n", rf.me, follower)
			if rf.sendRequestVote(follower, voteArgs, &reply) {
				debug("server %d in term %d got reply from server %d that's: %+v\n", rf.me, currentTerm, follower, reply)
				electionMu.Lock()
				if electionDone {
					return
				}
				electionMu.Unlock()
				if reply.VoteGranted {
					debug("server %d in term %d accepting a YES vote from server %d\n", rf.me, currentTerm, follower)
					select {
					case votes <- true:
						return
					case <-rf.killCh:
						rf.killCh <- true
						return
					}
					votes <- true
				} else {
					debug("server %d in term %d getting a NO vote from server %d\n", rf.me, currentTerm, follower)
					if reply.Term > currentTerm {
						rf.mu.Lock()
						rf.updateTerm(reply.Term)
						rf.mu.Unlock()
						go rf.quitElection(currentTerm)
						electionMu.Lock()
						electionDone = true
						electionMu.Unlock()
					}
					select {
					case votes <- false:
						return
					case <-rf.killCh:
						rf.killCh <- true
						return
					}
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
		case <-rf.quitElectionCh:
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
				debug("SERVER %d SETTING NEXT INDEX TO %d\n", rf.me, rf.getFullLogLength())
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.getFullLogLength()
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

func (rf *Raft) doInstallSnapshot(server int, curTerm int, snapIndex int, snapTerm int) bool {
	snapshotArgs := InstallSnapshotArgs{curTerm, rf.me, snapIndex, snapTerm, rf.persister.ReadSnapshot()}
	var reply InstallSnapshotReply
	snapReplyCh := make(chan bool, 1)
	snapTimedOut := make(chan bool, 1)
	go func(replyChan chan bool, timeout chan bool) {
		ok := rf.sendInstallSnapshot(server, snapshotArgs, &reply)
		select {
		case <-timeout:
			return
		case replyChan <- ok:
			return
		case <-rf.killCh:
			rf.killCh <- true
			return
		}
	}(snapReplyCh, snapTimedOut)

	select {
	case ok := <-snapReplyCh:
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				rf.updateTerm(reply.Term)
				rf.resetTimer()
			}
			rf.mu.Unlock()
			return true
		}
	case <-time.After(50 * time.Millisecond):
		snapTimedOut <- true
	case <-rf.killCh:
		rf.killCh <- true
	}
	return false
}

func (rf *Raft) leader() {
	for i := range rf.peers {
		go func(follower int) {
			for {
				go func() {
					rf.mu.Lock()
					if rf.state == Leader {
						index := rf.nextIndex[follower]
						newEntries := AppendEntriesArgs{rf.CurrentTerm, rf.me, index - 1, rf.SnapshotTerm, make([]Log, 0), rf.commitIndex}
						if index <= rf.SnapshotIndex && rf.SnapshotIndex >= 0 && follower != rf.me {
							debug("leader %d calling Install Snapshot on server %d with snapshot index %d, local index is %d, o.g. is %d\n", rf.me, follower, rf.SnapshotIndex, rf.toLocal(index), index)
							snapIndex := rf.SnapshotIndex
							snapTerm := rf.SnapshotTerm
							curTerm := rf.CurrentTerm
							rf.mu.Unlock()
							snapshotInstalled := rf.doInstallSnapshot(follower, curTerm, snapIndex, snapTerm)
							rf.mu.Lock()
							debug("snapshot install returned from leader %d to follower %d with response %t\n", rf.me, follower, snapshotInstalled)
							if snapshotInstalled {
								newEntries.Entries = rf.Log
								newEntries.PrevLogIndex = -1
								newEntries.PrevLogTerm = -1
								rf.nextIndex[follower] = max(rf.nextIndex[follower], snapIndex+1)
								rf.matchIndex[follower] = snapIndex
							} else {
								rf.mu.Unlock()
								return
							}
						} else if rf.toLocal(index) < 0 && follower == rf.me {
							debug("MAYBE WARNING: leader %d next index is %d but snapshot index is %d with log of length %d\n", rf.me, index, rf.SnapshotIndex, len(rf.Log))
							newEntries.PrevLogIndex = rf.SnapshotIndex
							newEntries.PrevLogTerm = rf.SnapshotTerm
							newEntries.Entries = rf.Log
						} else if rf.toLocal(index) < len(rf.Log) {
							debug("server %d sending to server %d has local index: %d with snapshot index %d and log length %d\n", rf.me, follower, rf.toLocal(index), rf.SnapshotIndex, len(rf.Log))
							newEntries.Entries = rf.Log[rf.toLocal(index):]
						}
						if rf.toLocal(index-1) >= 0 {
							if rf.toLocal(index-1) >= len(rf.Log) {
								debug("FAIL: for leader %d, next index %d (converted to %d) of server %d weird, my log is %+v\n", rf.me, index, rf.toLocal(index), follower, rf.Log)
							}
							newEntries.PrevLogTerm = rf.Log[rf.toLocal(index-1)].Term
						}
						rf.mu.Unlock()

						var reply AppendEntriesReply
						replyCh := make(chan bool)
						timedOut := make(chan bool)
						go func(replyChan chan bool, timeout chan bool) {
							ok := rf.sendAppendEntries(follower, newEntries, &reply)
							select {
							case <-timeout:
								return
							case replyChan <- ok:
								return
							case <-rf.killCh:
								rf.killCh <- true
								return
							}
						}(replyCh, timedOut)

						timeout := 15 * time.Millisecond

						select {
						case ok := <-replyCh:
							if ok {
								if len(newEntries.Entries) > 0 {
									debug("REPLY from server %d for leader %d: %+v\n", follower, rf.me, reply)
								}

								rf.mu.Lock()
								if rf.state != Leader {
									rf.mu.Unlock()
									return
								}
								if reply.Term > rf.CurrentTerm {
									if len(newEntries.Entries) == 0 {
										debug("from heartbeat: ")
									} else {
										debug("from append entries reply: ")
									}
									rf.updateTerm(reply.Term)
									rf.resetTimer()
								} else if reply.Success {
									rf.nextIndex[follower] = min(rf.nextIndex[follower]+len(newEntries.Entries), rf.getFullLogLength())
									rf.matchIndex[follower] = rf.nextIndex[follower] - 1
									if len(newEntries.Entries) > 0 {
										debug("old nextIndex for server %d was %d, now %d\n", follower, rf.nextIndex[follower]-len(newEntries.Entries), rf.nextIndex[follower])
										debug("old matchIndex for server %d was %d, now %d\n", follower, rf.matchIndex[follower]-len(newEntries.Entries), rf.matchIndex[follower])
									}
								} else {
									newNext := len(rf.Log)
									foundTerm := false
									for ; newNext > 0; newNext-- {
										if rf.Log[newNext-1].Term == reply.ConflictTerm {
											foundTerm = true
											break
										}
									}
									if foundTerm {
										debug("old nextIndex for server %d was %d, now %d\n", follower, rf.nextIndex[follower], rf.toGlobal(newNext))
										rf.nextIndex[follower] = rf.toGlobal(newNext)
									} else {
										debug("old nextIndex for server %d was %d, now %d\n", follower, rf.nextIndex[follower], reply.ConflictIndex)
										rf.nextIndex[follower] = reply.ConflictIndex
									}
									debug("old matchIndex for server %d was %d, now %d\n", follower, rf.matchIndex[follower], rf.nextIndex[follower]-1)
									rf.matchIndex[follower] = rf.nextIndex[follower] - 1
								}
								rf.mu.Unlock()
							} else {
								if len(newEntries.Entries) > 0 {
									debug("RPC from server %d for leader %d failed\n", follower, rf.me)
								}
							}
						case <-time.After(timeout):
							// Timeout; disregard reply
							if len(newEntries.Entries) > 0 {
								debug("RPC from server %d for leader %d timed out\n", follower, rf.me)
							}
							timedOut <- true
						case <-rf.killCh:
							rf.killCh <- true
							return
						}
					} else {
						rf.mu.Unlock()
					}
				}()
				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()
				if state != Leader {
					break
				}
				select {
				case <-rf.killCh:
					rf.killCh <- true
					return
				case <-time.After(100 * time.Millisecond):
				}
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
				for i := rf.toLocal(rf.commitIndex + 1); i < log_length; i++ {
					count := 0
					if rf.Log[i].Term < currentTerm {
						continue
					} else if rf.Log[i].Term > currentTerm {
						debug("BAD: leader %d at term %d has weird log: %+v\n", rf.me, rf.CurrentTerm, rf.Log)
						break
					}
					for j := range rf.peers {
						if rf.toLocal(rf.matchIndex[j]) >= i {
							count++
						}
					}
					//debug("count for above log was %d\n", count)
					if count > (len(rf.peers))/2 {
						debug("incremented leader %d's commit index from %d to %d, match index is %+v\n", rf.me, newCommit, rf.toGlobal(i), rf.matchIndex)
						newCommit = i
					} else {
						break
					}
				}
				if newCommit != rf.commitIndex {
					rf.commitIndex = rf.toGlobal(newCommit)
					rf.applyMessages()
				}
			} else {
				rf.mu.Unlock()
				break
			}

			rf.mu.Unlock()
			<-time.After(100 * time.Millisecond)
		}
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
		log.Printf(format, a...)
	}
	return
}
