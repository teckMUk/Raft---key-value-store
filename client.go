package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers       []*labrpc.ClientEnd
	clinetId      int64
	requestNumber int
	leaderId      int
	// You will have to modify this struct.
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
	ck.clinetId = nrand()
	ck.requestNumber = 0
	ck.leaderId = int(nrand()) % len(servers)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// args := GetArgs{SenderID: ck.clinetId,RequestNumber: ck.requestNumber+1,Key: key}
	// reply := GetReply{}
	//DPrintf("Get Request called for key %s",key)
	var reply GetReply
	// var Value string
	i := ck.leaderId
	for {
		DPrintf("sending request to server id %d at get by client id %d", i, ck.clinetId)
		args := GetArgs{SenderID: ck.clinetId, RequestNumber: ck.requestNumber + 1, Key: key}
		reply = GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				DPrintf("wrong leader")
			} else {
				ck.leaderId = i
				ck.requestNumber += 1
				//DPrintf("Get Request back for key %s",key)
				break
			}
		} else {
			i = (i + 1) % len(ck.servers)
			DPrintf("failed to sent request to client at get")
		}
	}
	if reply.Err == ErrNoKey {
		return ""
	} else {
		return reply.Value
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//DPrintf("PutAppend Request called for key %s and value %s",key,value)
	// var Value string

	i := ck.leaderId
	for {
		DPrintf("sending request at putappend to server id %d by client id %d", i, ck.clinetId)
		args := PutAppendArgs{SenderID: ck.clinetId, RequestNumber: ck.requestNumber + 1, Key: key, Value: value, Op: op}
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			//DPrintf("Clinet got back replt %+v,reply.WrongLeader %t",reply,reply.WrongLeader)
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				DPrintf("wrong leader")
			} else {
				ck.leaderId = i
				ck.requestNumber += 1
				//DPrintf("PutAppend Request back for key %s and value %s",key,value)
				//DPrintf("client ")
				return
			}
		} else {
			i = (i + 1) % len(ck.servers)
			DPrintf("failed to sent request to client at putappend")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
