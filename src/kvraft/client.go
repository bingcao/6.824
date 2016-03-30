package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", args, &reply)
//
func (ck *Clerk) Get(key string) string {
	DPrintf("CLIENT starting get of %s\n", key)
	args := GetArgs{key, nrand()}
	i := ck.leader
	for {
		var reply GetReply
		DPrintf("CLIENT %d TRYING TO CALL GET on server %d for key %s\n", ck.id, i, key)
		done := make(chan bool)
		go func() {
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			if ok {
				done <- true
			}
		}()
		select {
		case <-done:
			DPrintf("CLIENT %d GOT GET REPLY from server %d to get %s: %+v\n", ck.id, i, key, reply)
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
			} else if reply.Err == ErrNoKey {
				return ""
			} else if reply.Err == ErrLostAction {
				// retry this server because its the leader
			} else if reply.Err == OK {
				ck.leader = i
				DPrintf("CLIENT %d SET LEADER TO %d\n", ck.id, i)
				return reply.Value
			}
		case <-time.After(500 * time.Millisecond):
			DPrintf("CLIENT %d TIMED OUT ON GET REQUEST for server %d and key %s", ck.id, i, key)
			i = (i + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", args, &reply)
//
func (ck *Clerk) PutAppend(key string, value string, op Type) {
	args := PutAppendArgs{key, value, op, nrand()}
	i := ck.leader
	for {
		var reply PutAppendReply
		DPrintf("CLIENT %d TRYING TO CALL PUT/APPEND on server %d for key %s and value %s\n", ck.id, i, key, value)
		done := make(chan bool)
		go func() {
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
			if ok {
				done <- true
			}
		}()
		select {
		case <-done:
			DPrintf("CLIENT %d GOT PUT/APPEND KVRAFT REPLY from server %d for key %s and value %s: %+v\n", ck.id, i, key, value, reply)
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
			} else if reply.Err == ErrLostAction {
				// retry this server because its the leader
			} else if reply.Err == OK {
				ck.leader = i
				DPrintf("CLIENT %d SET LEADER TO %d\n", ck.id, i)
				return
			}
		case <-time.After(500 * time.Millisecond):
			DPrintf("CLIENT %d TIMED OUT ON PUT/APPEND KVRAFT REQUEST for server %d and key %s and value %s", ck.id, i, key, value)
			i = (i + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
