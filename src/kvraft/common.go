package raftkv

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrLostAction = "ErrLostAction"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	CHECK  = "Check"
)

type Err string

type Type string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Operation Type // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
