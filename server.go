package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key           string
	Value         string
	SenderID      int64
	RequstNumber  int
	OperationType string //put get append
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore              map[string]string //database
	clientLSR            map[int64]int     //LSR = last sent request which heard by server and was sucessful, to save
	clientResultChan     map[int64]chan Result
	clientCurrentRequest map[int]Op
	serialRequest        chan Op
	clientExpectingIndex map[int64]int
	//memory space I will only store the higest last Request knowing all Request lower then this number have
	//to succesful
}
type Result struct {
	OpType      string
	Value       string
	Errmsg      Err
	WrongLeader bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("Get called with args %+v and server id %d",args,kv.me)
	// kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "WRONGLEADER"
		// kv.mu.Unlock()
		return
	}
	//DPrintf("Leader found %+v",args)
	makeReplyChan := make(chan Result)
	kv.mu.Lock()
	kv.clientResultChan[args.SenderID] = makeReplyChan // we declare a new channel
	op := Op{OperationType: "Get", Key: args.Key, SenderID: args.SenderID, RequstNumber: args.RequestNumber}
	kv.mu.Unlock()
	kv.serialRequest <- op

	// index, _, _ := kv.rf.Start(op)
	// kv.clientCurrentRequest[index] = op
	// kv.mu.Unlock()
	//DPrintf("client %d is waith for get request",args.SenderID)

	select {
	case result := <-makeReplyChan:
		reply.Err = result.Errmsg
		reply.Value = result.Value
		reply.WrongLeader = result.WrongLeader
		//DPrintf("client %d got back reply at get request result is %+v",args.SenderID,result)

	}
	// kv.mu.Lock()
	// delete(kv.clientResultChan,args.SenderID)
	// kv.mu.Unlock()

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("putappend called with args %+v, server id %d",args,kv.me)
	// kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "WRONGLEADER"
		// kv.mu.Unlock()
		return
	}
	//DPrintf("Leader found %+v",args)
	kv.mu.Lock()
	makeReplyChan := make(chan Result)
	kv.clientResultChan[args.SenderID] = makeReplyChan // we declare a new channel
	kv.mu.Unlock()
	op := Op{OperationType: args.Op, Key: args.Key, Value: args.Value, SenderID: args.SenderID, RequstNumber: args.RequestNumber}
	kv.serialRequest <- op

	// //DPrintf("called start")
	// index, _, _ := kv.rf.Start(op)
	// //DPrintf("got index %d", index)
	// kv.clientCurrentRequest[index] = op
	// kv.mu.Unlock()
	//DPrintf("client %d is waith for putappend request",args.SenderID)
	select {
	case result := <-makeReplyChan:
		////DPrintf("got reply back %+v", result)
		//DPrintf("client %d got back reply at putappend and result %+v",args.SenderID,result)
		reply.Err = result.Errmsg
		////DPrintf("result at client end wrong leader %t", result.WrongLeader)
		reply.WrongLeader = result.WrongLeader

	}

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	//DPrintf("I am dead")
	// Debug = 0
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) ReadRequst() {
	for {
		select {
		case response := <-kv.serialRequest:
			index, _, _ := kv.rf.Start(response)
			kv.mu.Lock()
			//DPrintf("server %d is expecting command %+v at index %d, isleader %t",kv.me,response,index,isLeader)
			kv.clientCurrentRequest[index] = response
			kv.clientExpectingIndex[response.SenderID] = index
			//DPrintf("hello server %d is here ",kv.me)
			kv.mu.Unlock()
		}
		//DPrintf("hello server %d is here ",kv.me)
	}
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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
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
	kv.clientLSR = make(map[int64]int)
	kv.kvstore = make(map[string]string)
	kv.clientResultChan = make(map[int64]chan Result)
	kv.clientCurrentRequest = make(map[int]Op)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.serialRequest = make(chan Op)
	kv.clientExpectingIndex = make(map[int64]int)
	go kv.ReadRequst()
	go kv.readApplyChan()

	return kv
}

// func (kv *RaftKV) readApplyChan(){
// 	readApplyChan := kv.applyCh
// 	for{
// 		select{
// 		case applyToDB := <-readApplyChan:
// 			index := applyToDB.Index
// 			op :=  applyToDB.Command.(Op)
// 			replyChan := kv.clientResultChan[op.SenderID]
// 			var reply Result
// 			kv.mu.Lock()
// 			if op.OperationType == "Get"{
// 				v, Ok := kv.kvstore[op.Key]
// 				if Ok{
// 					reply := Result{Value: v,Key: op.Key,OpType: "Get"}
// 					//compare karna hai if this a valid request
// 					//error headling

// 					kv.mu.Unlock()
// 					replyChan <-reply
// 				}else{
// 					_,mystate:= kv.rf.GetState()
// 					if mystate{
// 						reply := Result{Errmsg: ErrNoKey, WrongLeader: false}
// 						kv.mu.Unlock()
// 						replyChan <-reply
// 					}else{
// 						reply := Result{Errmsg: ErrNoKey, WrongLeader: true}
// 						kv.mu.Unlock()
// 						replyChan <-reply
// 					}
// 				}
// 			}else if op.OperationType == "Put"{
// 				kv.kvstore[op.Key] = op.Value
// 				reply := Result{OpType: "Put"}
// 				replyChan := kv.clientResultChan[op.SenderID]
// 				kv.mu.Unlock()
// 				replyChan <-reply

// 			}else{
// 				v, Ok := kv.kvstore[op.Key]
// 				if Ok{
// 					kv.kvstore[op.Key]=v+op.Value
// 				}else{
// 					kv.kvstore[op.Key] = op.Value
// 				}
// 				reply := Result{OpType: "Put"}
// 				replyChan := kv.clientResultChan[op.SenderID]
// 				kv.mu.Unlock()
// 				replyChan <-reply
// 			}
// 			// kv.mu.Unlock()
// 			// continue
// 		}
// 	}
// }
func (kv *RaftKV) readApplyChan() {
	// readApplyChan := kv.applyCh
	for {
		select {
		case applyResult := <-kv.applyCh:
			//DPrintf("apply chan result %+v at server id %d", applyResult,kv.me)
			var Value string
			op := applyResult.Command.(Op)
			v, ok := kv.clientLSR[op.SenderID]
			if ok {
				if op.RequstNumber <= v {
					if op.OperationType == "Get" {
						v, ok := kv.kvstore[op.Key]
						if ok {
							Value = v
						} else {
							Value = ErrNoKey
						}

					} else {
						Value = OK
					}
					//DPrintf("Replying to client with result of duplicate request %+v, and lSR %d",op,v)
					kv.replytoClient(applyResult, op, Value)
					continue
				}
			}
			if op.OperationType == "Get" {
				v, ok := kv.kvstore[op.Key]
				if ok {
					Value = v
				} else {
					Value = ErrNoKey
				}
			} else if op.OperationType == "Put" {
				kv.kvstore[op.Key] = op.Value
				Value = OK
			} else {
				// in case of append
				v, Ok := kv.kvstore[op.Key]
				if Ok {
					kv.kvstore[op.Key] = v + op.Value
				} else {
					kv.kvstore[op.Key] = op.Value
				}
				Value = OK
			}
			kv.clientLSR[op.SenderID] = op.RequstNumber
			//DPrintf("Replying to client with result of its request %+v",op)
			kv.replytoClient(applyResult, op, Value)
		case <-time.After(2000 * time.Millisecond):
			//DPrintf("I have time out for server id %d before lock",kv.me)
			kv.mu.Lock()
			// allchan:=kv.clientResultChan
			// kv.mu.Unlock()
			//DPrintf("I have time out for server id %d",kv.me)
			// allchan := kv.clientResultChan

			for key, replyChan := range kv.clientResultChan {
				//DPrintf("I am sending reply back to clients server id %d",kv.me)
				result := Result{WrongLeader: true}
				kv.mu.Unlock()
				replyChan <- result
				kv.mu.Lock()
				//kv.mu.Lock()
				delete(kv.clientResultChan, key)
				//kv.mu.Unlock()
			}
			kv.mu.Unlock()

		}
		//DPrintf("I am here server id %d",kv.me)
	}
}

// func (kv *RaftKV) replytoClient(applyResult raft.ApplyMsg,op Op,Value string){
// 	kv.mu.Lock()
// 	// //DPrintf("reply to client function")
// 	replyChan, ok := kv.clientResultChan[op.SenderID]
// 	//DPrintf("reply to client function with value of ok %t",ok)
// 	kv.mu.Unlock()

// 	// //DPrintf("calling getstate")
// 	// _,isLeader := kv.rf.GetState()
// 	// //DPrintf("ok %t value at server number %d", ok, kv.me)
// 	// // kv.mu.Lock()
// 	// //DPrintf("I am the leader %t",isLeader)
// 	if ok {
// 		kv.mu.Lock()
// 		expectedCommand, ok := kv.clientCurrentRequest[applyResult.Index]
// 		//DPrintf("ExpectedCommand %+v at index %d while the commad I got %+v with values of ok %t",expectedCommand,applyResult.Index,op,ok)
// 		kv.mu.Unlock()
// 		if (expectedCommand.SenderID==op.SenderID) && (expectedCommand.RequstNumber==op.RequstNumber){
// 			result := Result{WrongLeader: false, Errmsg: Err(Value), Value: Value, OpType: op.OperationType}
// 			//DPrintf("Replying to client with correct leader %d with result %+v",expectedCommand.SenderID,result)
// 			replyChan <- result
// 		}else{
// 			result := Result{WrongLeader: true, Errmsg: Err(Value), Value: Value, OpType: op.OperationType}
// 			//DPrintf("Replying to client with wrong leader %d with result %+v",expectedCommand.SenderID,result)
// 			replyChan <- result

// 		}

// 		// replyChan := kv.clientResultChan[op.SenderID]

// 		//kv.mu.Unlock()

// 	}
// 	//DPrintf("I am here")
// }

func (kv *RaftKV) replytoClient(applyResult raft.ApplyMsg, op Op, Value string) {
	//DPrintf("Called here ")
	kv.mu.Lock()
	expectedCommand, ok := kv.clientCurrentRequest[applyResult.Index]
	expectedIndex, ok3 := kv.clientExpectingIndex[op.SenderID]
	//DPrintf("replycalled at server %d to client function with value of ok %t and expectedCommand %+v with op %+v",kv.me,ok,expectedCommand,op)
	//kv.mu.Unlock()
	if ok {
		//ethier I am a new leader or an old leader waiting for a command
		////DPrintf("op %+v",op)
		if expectedCommand.SenderID == op.SenderID && expectedCommand.RequstNumber == op.RequstNumber {
			// current leader  send result to new op.senderID
			if ok3 && expectedIndex == applyResult.Index {
				//kv.mu.Lock()
				replyChan, ok2 := kv.clientResultChan[op.SenderID]
				////DPrintf("got result can %t",ok)
				//kv.mu.Unlock()
				if ok2 {
					////DPrintf("Sending reply to client for correct leader")
					result := Result{WrongLeader: false, Errmsg: Err(Value), Value: Value, OpType: op.OperationType}
					//DPrintf(" Before1 chan sending reply to client server id %d and client %d",kv.me,op.SenderID)
					delete(kv.clientResultChan, op.SenderID)
					kv.mu.Unlock()
					replyChan <- result
					//DPrintf(" After1 chan sending reply to client server id %d",kv.me)
					//kv.mu.Lock()

				} else {
					kv.mu.Unlock()
				}
			} else {
				kv.mu.Unlock()
			}
		} else {
			//kv.mu.Lock()
			replyChan, ok2 := kv.clientResultChan[expectedCommand.SenderID]
			//kv.mu.Unlock()
			if ok2 {
				////DPrintf("Sending reply to client of wrong leader")
				result := Result{WrongLeader: true, Errmsg: Err(Value), Value: Value, OpType: op.OperationType}
				//DPrintf(" Before2 chan sending reply to client server id %d and client id %d",kv.me,expectedCommand.SenderID)
				delete(kv.clientResultChan, expectedCommand.SenderID)
				kv.mu.Unlock()
				replyChan <- result
				//DPrintf(" After2 chan sending reply to client server id %d",kv.me)
				//kv.mu.Lock()

				//kv.mu.Unlock()
			} else {
				kv.mu.Unlock()
			}
			//DPrintf("server id %d",kv.me)
			// I am the old leader
		}
	} else {
		kv.mu.Unlock()
	}
}
