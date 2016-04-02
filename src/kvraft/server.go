package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	//	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation Type
	Key       string // the key to look for or to add
	Value     string // only for put or append; in Get it is ""
	Id        int64
}

type ConfirmMsg struct {
	Id    int64
	Value string
	Error Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	snapmu       sync.Mutex

	// Your definitions here.
	commChans map[int][]chan ConfirmMsg
	state     State
	idMap     map[int64][]chan bool
}

type State struct {
	Ids               map[int64]bool
	Kvs               map[string]string
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (kv *RaftKV) setIdMap(id int64) chan bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if chanList, ok := kv.idMap[id]; ok {
		kv.idMap[id] = append(chanList, make(chan bool, 1))
		return kv.idMap[id][len(kv.idMap[id])-1]
	} else {
		kv.idMap[id] = make([]chan bool, 1)
		kv.idMap[id][0] = make(chan bool, 1)
		return kv.idMap[id][0]
	}
}

func (kv *RaftKV) getCommChan(index int) chan ConfirmMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.commChans[index]; exists {
		kv.commChans[index] = append(kv.commChans[index], make(chan ConfirmMsg, 1))
		return kv.commChans[index][len(kv.commChans[index])-1]
	} else {
		kv.commChans[index] = make([]chan ConfirmMsg, 1)
		kv.commChans[index][0] = make(chan ConfirmMsg, 1)
		return kv.commChans[index][0]
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{GET, args.Key, "", args.Id}
	setChan := kv.setIdMap(op.Id)
	index, _, ok := kv.rf.Start(op)
	commChan := kv.getCommChan(index)
	setChan <- true

	if ok {
		reply.WrongLeader = false
		response := <-commChan
		if response.Id == op.Id {
			reply.Err = response.Error
			reply.Value = response.Value
		} else {
			reply.Err = ErrLostAction
		}
		return
	} else {
		reply.WrongLeader = true
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{args.Operation, args.Key, args.Value, args.Id}
	setChan := kv.setIdMap(op.Id)
	index, _, ok := kv.rf.Start(op)
	commChan := kv.getCommChan(index)
	setChan <- true

	if ok {
		reply.WrongLeader = false
		response := <-commChan
		if response.Id == op.Id {
			reply.Err = OK
		} else {
			reply.Err = ErrLostAction
		}
		return
	} else {
		reply.WrongLeader = true
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) ReadState(persister *raft.Persister) {
	var newState State
	kv.snapmu.Lock()
	r := bytes.NewBuffer(persister.ReadSnapshot())
	kv.snapmu.Unlock()
	if r.Len() == 0 {
		newState.Ids = make(map[int64]bool)
		newState.Kvs = make(map[string]string)
		newState.LastIncludedTerm = 0
		newState.LastIncludedIndex = 0
	} else {
		d := gob.NewDecoder(r)
		d.Decode(&newState)
	}
	kv.mu.Lock()
	kv.state = newState
	kv.mu.Unlock()
}

func (kv *RaftKV) SaveState(persister *raft.Persister) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.state)
	data := w.Bytes()
	kv.snapmu.Lock()
	persister.SaveSnapshot(data)
	kv.snapmu.Unlock()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.idMap = make(map[int64][]chan bool)
	kv.commChans = make(map[int][]chan ConfirmMsg)
	kv.ReadState(persister)
	DPrintf("STARTING KV SERVER %d\n", me)
	kv.rf.Snapshot(kv.state.LastIncludedIndex, kv.state.LastIncludedTerm)

	go func() {
		for m := range kv.applyCh {
			if m.UseSnapshot {
				kv.ReadState(persister)
			} else {
				index := m.Index
				DPrintf("KVRAFT SERVER %d GOT OP %+v\n", kv.me, m.Command)
				sendValue := ""
				var err Err = OK
				kv.mu.Lock()
				if m.Command.(Op).Operation == GET {
					DPrintf("SERVER %d KV IS %+v\n", kv.me, kv.state.Kvs)
					if value, found := kv.state.Kvs[m.Command.(Op).Key]; found {
						sendValue = value
					} else {
						err = ErrNoKey
					}
				} else if ok, _ := kv.state.Ids[m.Command.(Op).Id]; !ok {
					kv.state.Ids[m.Command.(Op).Id] = true
					if m.Command.(Op).Operation == PUT {
						kv.state.Kvs[m.Command.(Op).Key] = m.Command.(Op).Value
					} else if m.Command.(Op).Operation == APPEND {
						kv.state.Ids[m.Command.(Op).Id] = true
						kv.state.Kvs[m.Command.(Op).Key] += m.Command.(Op).Value
					}
				}
				go kv.sendConfirms(index, m.Command.(Op).Id, sendValue, err)
				kv.state.LastIncludedIndex = m.Index
				kv.state.LastIncludedTerm = m.Term
				kv.SaveState(persister)
				if kv.maxraftstate > 0 {
					go kv.snapshot(persister)
				}
				kv.mu.Unlock()
			}
		}
	}()

	/*go func() {
		for {
			kv.mu.Lock()
			if kv.maxraftstate > 0 && persister.RaftStateSize() >= kv.maxraftstate {
				kv.SaveState(persister)
				go kv.rf.Snapshot(kv.state.LastIncludedIndex, kv.state.LastIncludedTerm)
			}
			kv.mu.Unlock()
			<-time.After(500 * time.Millisecond)
		}
	}()*/

	return kv
}

func (kv *RaftKV) snapshot(persister *raft.Persister) {
	kv.snapmu.Lock()
	if persister.RaftStateSize() >= kv.maxraftstate {
		kv.rf.Snapshot(kv.state.LastIncludedIndex, kv.state.LastIncludedTerm)
	}
	kv.snapmu.Unlock()
}

func (kv *RaftKV) sendConfirms(index int, id int64, value string, err Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for j := 0; j < len(kv.idMap[id]); j++ {
		<-kv.idMap[id][j]
		close(kv.idMap[id][j])
	}
	delete(kv.idMap, id)
	for i := 0; i < len(kv.commChans[index]); i++ {
		kv.commChans[index][i] <- ConfirmMsg{id, value, err}
		close(kv.commChans[index][i])
	}
	delete(kv.commChans, index)
}
