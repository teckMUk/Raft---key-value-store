package raft

// 23100313
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
	//"fmt"
	"bytes"
	"encoding/gob"
	"labrpc"
	"math"
	"math/rand"

	// "strconv"
	"sync"
	"time"
	// "fmt"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Raft struct {
	mu           sync.Mutex
	peers        []*labrpc.ClientEnd
	persister    *Persister
	me           int // index into peers[]
	currentTerm  int
	votedFor     int
	myState      string // f- follower c- candanaite l - leader
	leaderHBChan chan int
	votedChan    chan int
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	applyChan    chan ApplyMsg
	killed       int

	// currentVoteCount int
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	////DPrintf("In raft called getstate %d",rf.me)
	var term int
	var isleader bool
	isleader = false
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.myState == "l" {
		isleader = true
	}
	////DPrintf("node %d in term %d mystate is %s\n",rf.me,rf.currentTerm,rf.myState)
	rf.mu.Unlock()

	// Your code here.
	return term, isleader
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
	rf.mu.Lock()
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.mu.Unlock()
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
	rf.mu.Lock()
	d.Decode(&rf.log)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

// AppendEntiresArgs
type AppendEntiresArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntiresArgs
type AppendEntiresReply struct {
	Term        int
	Success     bool
	ServerIndex int
}

func (rf *Raft) AppendEntires(args AppendEntiresArgs, reply *AppendEntiresReply) {

	rf.mu.Lock()
	//open
	////DPrintf("I got heart beat node %d mykill %d",rf.me,rf.killed)
	if rf.killed == 1 {
		rf.mu.Unlock()
		return
	}
	////DPrintfff("I  %d got HB from leader %d in term %d",rf.me,args.LeaderID,args.Term)
	// fmt.Print(args.Entries )
	// fmt.Print("\n")

	myTerm := rf.currentTerm
	reply.ServerIndex = rf.me
	//rf.mu.Unlock()
	if args.Term < myTerm {
		////////DPrintfff("Term failed")
		reply.Success = false
		reply.Term = myTerm
		rf.mu.Unlock()
	} else {
		////DPrintfff("I got a valid heart beat")
		//rf.leaderHBChan <- 1
		//rf.mu.Lock()
		if args.Term > myTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.myState = "f"
		rf.mu.Unlock()
		rf.persist()
		////////DPrintfff("HeartBeat recived")
		rf.leaderHBChan <- 1
		rf.mu.Lock()
		reply.Term = args.Term
		mylog := rf.log

		//myCommitIndex := rf.commitIndex
		//rf.mu.Unlock()

		if args.PrevLogIndex == -1 {
			//rf.mu.Lock()
			rf.log = args.Entries
			//rf.mu.Unlock()
			reply.Success = true
		} else if args.PrevLogIndex >= len(mylog) {
			//DPrintff("Deny log")
			reply.Success = false
		} else if mylog[args.PrevLogIndex].Term != args.PrevLogTerm {
			//DPrintff("Deny log2")
			reply.Success = false
		} else {
			//append entries to mylog
			if len(args.Entries) != 0 { // check incase to prevent garabage value to
				//rf.mu.Lock()
				mylog = rf.log[:args.PrevLogIndex+1]
				rf.log = append(mylog, args.Entries...)
				//rf.mu.Unlock()
			}
			reply.Success = true
		}
		rf.mu.Unlock()
		rf.persist()
		rf.mu.Lock()
		// fmt.Printf("Printing follwer log after a append heart beat %d",rf.me)
		// fmt.Print(rf.log)
		// fmt.Print('\n')

		if reply.Success && (args.PrevLogIndex != -1 && len(args.Entries) == 0) {
			//rf.mu.Lock()
			// fmt.Printf("My log after success of id %d\n",rf.me)
			// fmt.Print(rf.log)
			// fmt.Print("\n")
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
				if rf.commitIndex > rf.lastApplied {
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						entry := rf.log[i]
						applyEntry := ApplyMsg{Command: entry.Command, Index: i + 1}
						rf.mu.Unlock()
						//DPrintf("Replying to channel before2 node id %d",rf.me)
						rf.applyChan <- applyEntry
						//DPrintf("Replying to channel After2 node id %d",rf.me)
						rf.mu.Lock()
						// //////DPrintfff("Node %d is replicating entry %d",reply.ServerIndex,entry.Command)
						// //////DPrintfff("The args I got %d",args)
						rf.lastApplied = i
					}
				}
			}

			//rf.mu.Unlock()
			//commit here to state machine
		}
		rf.mu.Unlock()
		//open
		//////DPrintf("Follower Node %d recived args prevIndex %d Enteries %d leadercommitIndex %d mycommitIndex %d and mylog %d ,prvelogTerm %d",rf.me,args.PrevLogIndex,args.Entries,args.LeaderCommit,rf.commitIndex,rf.log,args.PrevLogTerm)
		// //////DPrintfff("\nMy log on Appendires after insetrion (follower id %d) %d \n",rf.me,rf.log)
		// //////DPrintfff("\n Appendires prev index recived %d , term %d , entries %d \n",args.PrevLogIndex,args.Term,args.Entries)
		// //////DPrintfff("\n Leader commit index %d \n",args.LeaderCommit)
		// we will check log replication

	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	//open
	//////DPrintf("Node %d is requesting for vote in term %d to node %d in term %d with mylog %d, cad had last index %d and last term %d",args.CandidateID,args.Term,rf.me,rf.currentTerm,rf.log,args.LastLogIndex,args.LastLogTerm)
	if rf.killed == 1 {
		rf.mu.Unlock()
		return
	}
	if args.Term < rf.currentTerm {
		////////DPrintfff("Didn'tVotedFor" + strconv.Itoa(args.CandidateID) + strconv.Itoa(args.Term) + strconv.Itoa(rf.currentTerm))
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		if rf.votedFor == -1 || args.Term > rf.currentTerm {
			myLastLogIndex := -1
			myLastlogTerm := -1
			if len(rf.log) != 0 {
				myLastLogIndex = len(rf.log) - 1
				myLastlogTerm = rf.log[myLastLogIndex].Term
			}
			if myLastlogTerm > args.LastLogTerm || (myLastlogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex) {
				////DPrintfff("mylast log Term %d mylastIndex %d cad last term %d and cad lastIndex %d",myLastlogTerm,myLastLogIndex,args.LastLogTerm,args.LastLogIndex)
				reply.VoteGranted = false
				reply.Term = args.Term
				if args.Term > rf.currentTerm {
					rf.currentTerm = args.Term
					rf.myState = "f"
				}
				rf.mu.Unlock()
				rf.persist()
			} else {
				rf.myState = "f"
				////////DPrintfff("VotedFor" + strconv.Itoa(args.CandidateID))
				reply.VoteGranted = true
				reply.Term = args.Term
				rf.currentTerm = reply.Term
				rf.votedFor = args.CandidateID
				rf.mu.Unlock()
				rf.persist()
				rf.votedChan <- 1
			}

		} else {
			////////DPrintfff("Didn't VotedFor" + strconv.Itoa(args.CandidateID) + strconv.Itoa(rf.votedFor))
			reply.VoteGranted = false
			reply.Term = args.Term
			rf.mu.Unlock()
		}
	}
	//open me
	//////DPrintf("my id %d my term %d mylog %d cad id %d cad term %d cad lastTerm %d cad lastnIdex %d voteStatus %t VotedFor %d",rf.me,rf.currentTerm,rf.log,args.CandidateID,args.Term,args.LastLogTerm,args.LastLogIndex,reply.VoteGranted,rf.votedFor)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, replyChan chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	////////DPrintfff("HELLO UR ANSSS %t", reply.VoteGranted)
	if ok == true {
		replyChan <- reply
	}
	return ok
}
func (rf *Raft) sendAppendEntires(server int, args AppendEntiresArgs, reply *AppendEntiresReply, replyChan chan *AppendEntiresReply) bool {
	////////DPrintfff("Heart sendAppendWas called")
	ok := rf.peers[server].Call("Raft.AppendEntires", args, reply)
	if ok == true {
		replyChan <- reply
	}
	return ok
}

func (rf *Raft) swpanElection() {
	////DPrintf("New election started by %d ",rf.me)
	electionResultchan := make(chan bool)
	go rf.broadCastVotes(electionResultchan)
	select {
	case result := <-electionResultchan:
		////////DPrintfff("Election result recived")
		if result == true {
			////DPrintf("New elected Leader for id %d in term %d\n",rf.me,rf.currentTerm)
			rf.mu.Lock()
			rf.myState = "l"
			////DPrintfff("I am the leader my id %d",rf.me)
			peerLen := len(rf.peers)
			rf.matchIndex = []int{} ////re inalizte the array on election of new leader
			rf.nextIndex = []int{}
			for i := 0; i < peerLen; i++ {
				//re inalizte the array.
				rf.matchIndex = append(rf.matchIndex, -1)        //sent new value
				rf.nextIndex = append(rf.nextIndex, len(rf.log)) //next index will len as start from 0
			}
			//open
			//DPrintf("New leader elected node %d in term %d",rf.me,rf.currentTerm)
			// fmt.Print("My log after becoming leader",rf.log,)
			// fmt.Print("My log after becoming leader",rf.nextIndex,)
			// rf.votedFor = -1
			rf.mu.Unlock()
			rf.votedChan <- 1
			go rf.swpanHeartBeat()
		}
	}

}
func (rf *Raft) swpanHeartBeat() {
	for {
		rf.mu.Lock()
		myState := rf.myState
		if rf.killed == 1 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if myState == "l" {
			////////DPrintfff("I have broadcasted heartbeat")
			go rf.broadCastHeartBeat()
		} else {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

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
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	myState := rf.myState
	//rf.mu.Unlock()
	if myState == "l" {
		//rf.mu.Lock()
		index = len(rf.log)
		newEntry := LogEntry{index, rf.currentTerm, command}
		rf.log = append(rf.log, newEntry)
		term = rf.currentTerm
		index = index + 1
		//open
		//////DPrintf("Leaders %d log at start %d",rf.me,rf.log)

		// fmt.Print("\n my log after adding a entry from start\n")
		// fmt.Print(rf.log)
		// fmt.Print('\n')
		////DPrintf("leader log after %d",rf.log)
		rf.mu.Unlock()

	} else {
		isLeader = false
		rf.mu.Unlock()
	}
	rf.persist()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.killed = 1
	rf.mu.Unlock()
	// Your code here, if desired.
}

// func timer(timer chan int)(chan int){

// 	go timeout(20,timer);
// 	fmt.Print("time out started\n")
// 	return timer
// }

// func timeout(timer time.Duration, timeChannel chan int ){
// 	time.Sleep(timer* time.Second)
// 	fmt.Print("time out\n")
// 	timeChannel <- 1;

// }

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderHBChan = make(chan int)
	rf.myState = "f"
	rf.votedChan = make(chan int)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyChan = applyCh
	rf.killed = 0
	// rf.currentVoteCount = 0
	// Your initialization code here.
	go rf.handler()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//open
	//////DPrintf("After coming back node %d up votedFor %d log %d currentTerm %d",rf.me,rf.votedFor,rf.log,rf.currentTerm)

	return rf
}

func (rf *Raft) handler() {
	var timeOutValue time.Duration
	for {
		rf.mu.Lock()
		if rf.killed == 1 {
			rf.mu.Unlock()
			return
		}
		if rf.myState == "l" {
			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()
		timeOutValue = rf.selectTimeout()
		////DPrintfff("Node %d is starting timeout in term %d for %d",rf.me,rf.currentTerm,timeOutValue)

		////////DPrintfff("time out value for node %d for dutation %d",rf.me,timeOutValue)
		//timerChannel := make(chan int)
		//creating a new channel for timer everytime to reset as sleep go can't be waked up in the midle
		// so creating a new channel will ensure the timeout dosen't time out due to older timer
		//but dose according to the new timmer.
		select {
		case <-rf.votedChan:
			////DPrintfff("votedchan reseted for %d",rf.me)
		case <-rf.leaderHBChan:
			////DPrintfff("Node %d Got heart beat",rf.me)
			////////DPrintfff("Heart beat recividedfor id %d in term %d\n",rf.me,rf.currentTerm)
		case <-time.After(timeOutValue * time.Millisecond):

			rf.mu.Lock()
			//DPrintf("I have time out for id %d in term %d\n",rf.me,rf.currentTerm)
			////////DPrintfff("Election started by " + strconv.Itoa(rf.me))
			rf.currentTerm += 1
			rf.myState = "c"
			rf.votedFor = rf.me
			rf.mu.Unlock()
			rf.persist()
			go rf.swpanElection()

		}
	}
}
func timer(timeOutValue time.Duration) chan int {
	timerChannel := make(chan int)
	go timeout(timeOutValue, timerChannel)
	////DPrintfff("New timeout started for %d\n",timeOutValue)
	return timerChannel
}

func timeout(timeOutValue time.Duration, timeChannel chan int) {
	time.Sleep(timeOutValue * time.Millisecond)
	// ////////DPrintfff("timed out\n")
	timeChannel <- 1

}

func (rf *Raft) broadCastVotes(electionResult chan bool) {
	rf.mu.Lock()
	currentNodes := len(rf.peers)
	myindex := rf.me
	myterm := rf.currentTerm
	args := RequestVoteArgs{}
	if len(rf.log) != 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	rf.mu.Unlock()
	replyChan := make(chan *RequestVoteReply)
	args.CandidateID = myindex
	args.Term = myterm
	// args.LastLogIndex = myLastLogIndex
	// args.LastLogTerm = myLastlogTerm
	//fill args here
	for i := 0; i < currentNodes; i++ {
		if i == myindex {

		} else {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, args, &reply, replyChan)
		}
	}
	voteCount := 1
	majorityNeed := int(math.Round(float64(currentNodes) / 2))
	for {
		select {
		case voteReply := <-replyChan:
			////////DPrintfff("voteReply %d %d %t", rf.me, voteReply.Term, voteReply.VoteGranted)
			if voteReply.VoteGranted == true {
				voteCount += 1
				if voteCount >= majorityNeed {
					electionResult <- true
					return
				}

			} else {

				rf.mu.Lock()
				if voteReply.Term > rf.currentTerm {
					rf.currentTerm = voteReply.Term
					rf.myState = "f"
					rf.votedFor = -1
					rf.mu.Unlock()
					rf.persist()
					electionResult <- false
					return
				}
				rf.mu.Unlock()

			}
		}
	}

}

func (rf *Raft) broadCastHeartBeat() {
	rf.mu.Lock()
	//DPrintf("broadcasting heart beat node %d",rf.me)
	currentNodes := len(rf.peers)
	myindex := rf.me
	myterm := rf.currentTerm
	rf.mu.Unlock()
	args := AppendEntiresArgs{}
	args.Term = myterm
	args.LeaderID = myindex
	replyChan := make(chan *AppendEntiresReply)
	myArgsList := []AppendEntiresArgs{}
	for i := 0; i < currentNodes; i++ {

		if i == myindex {
			myArgsList = append(myArgsList, args)

		} else {
			rf.mu.Lock()
			////////DPrintfff("Next index %d",rf.nextIndex[i])
			lastLogIndex := len(rf.log)
			if lastLogIndex >= rf.nextIndex[i] {
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				////////DPrintfff("index %d",args.PrevLogIndex)
				if args.PrevLogIndex != -1 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				myLog := make([]LogEntry, len(rf.log))
				copy(myLog, rf.log)
				//did this to remove race read on overstack that
				//silce are are passed by reference over all array is same
				//to stop this I made a shallow copy
				//this is done to the best of my understanding on slice
				// this did stop race from what I tested
				////////DPrintfff("my next index %d and my prev index %d",rf.nextIndex[i],args.PrevLogIndex)
				args.Entries = myLog[args.PrevLogIndex+1:]

			} else {
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.LeaderCommit = rf.commitIndex
			}
			//open
			//////DPrintf("Leader %d is sending Heart beat with prevIndex %d nextIndex %d matchIndex %d and log %d and commitIndex %d,Enteries %d to node %d",rf.me,args.PrevLogIndex,rf.nextIndex,rf.matchIndex,rf.log,rf.commitIndex,args.Entries,i)
			myArgsList = append(myArgsList, args)
			rf.mu.Unlock()
			reply := AppendEntiresReply{}
			// fmt.Printf("node %d has been sent args\n", i)
			// fmt.Print(args)
			go rf.sendAppendEntires(i, args, &reply, replyChan)
		}
	}
	for {
		select {
		case reply := <-replyChan:
			if reply.Success == false {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.myState = "f"
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.mu.Unlock()
					rf.persist()
				} else {
					////////DPrintfff("Reducing next index %d",rf.nextIndex[reply.ServerIndex])
					rf.nextIndex[reply.ServerIndex] = rf.nextIndex[reply.ServerIndex] - 1
					if rf.nextIndex[reply.ServerIndex] == -1 {
						rf.nextIndex[reply.ServerIndex] = 0
					}
					rf.mu.Unlock()
				}
				// rf.mu.Unlock()

			} else {
				args = myArgsList[reply.ServerIndex]
				rf.mu.Lock()
				////////DPrintfff("My next index in sus %d and previndex %d ",rf.nextIndex[reply.ServerIndex],args.PrevLogIndex)
				rf.nextIndex[reply.ServerIndex] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[reply.ServerIndex] = args.PrevLogIndex + len(args.Entries)
				////////DPrintfff("My next index in su %d ",rf.nextIndex[reply.ServerIndex])
				//check and update commit index
				//max will be the size of my log replicated
				sizeOfLog := len(rf.log)
				tempCommitIndex := []int{}
				myMatchIndex := rf.matchIndex
				votesNeedToCommit := int(math.Round(float64(currentNodes) / 2))
				// to find the new commitindex to commit the last
				// index in the array will be the higest index to commit size of array zero is no new match index found
				for i := rf.commitIndex + 1; i < sizeOfLog; i++ {
					//now we will check if i has majority in all logs
					count := 1 // a leader will all these log so will vote for the log to commit.
					for _, value := range myMatchIndex {
						if i <= value {
							count = count + 1
						}
					}
					if count >= votesNeedToCommit {
						tempCommitIndex = append(tempCommitIndex, i)
					}
				}
				////DPrintf("My temp commit Index %d",tempCommitIndex)
				if len(tempCommitIndex) != 0 {
					x := tempCommitIndex[len(tempCommitIndex)-1]
					////DPrintf("My x value %d, logterm %d, currentTerm %d",x,rf.log[x].Term,rf.currentTerm)
					if rf.log[x].Term == rf.currentTerm {
						rf.commitIndex = x
					}

					//fmt.Printf("Leaders commit index after changing %d",rf.commitIndex)
				}
				////DPrintf("leader %d matchIndex %d NewcommitIndex %d last apply %d",rf.me,rf.matchIndex,rf.commitIndex,rf.lastApplied)

				if rf.commitIndex > rf.lastApplied {
					// fmt.Print(rf.commitIndex)
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {

						entry := rf.log[i]
						applyEntry := ApplyMsg{Command: entry.Command, Index: i + 1}
						////DPrintf("Node %d applied %+v, lastApplied %d, i %d",rf.me,applyEntry,rf.lastApplied,i)
						rf.mu.Unlock()
						//DPrintf("Replying to channel before node id %d",rf.me)
						rf.applyChan <- applyEntry
						//DPrintf("Replying to channel after node id %d",rf.me)
						rf.mu.Lock()

						rf.lastApplied = i
					}
				}
				rf.mu.Unlock()
				////DPrintf("locked left afer applying")

			}
		}
	}

}

func (rf *Raft) selectTimeout() time.Duration {
	rf.mu.Lock()
	mystate := rf.myState
	rf.mu.Unlock()
	var timeOut time.Duration
	timeOut = 200
	if mystate == "c" {
		rand.Seed(time.Now().UnixNano())
		min := 200
		max := 400
		timeOut = time.Duration(rand.Intn(max-min+1) + min)
	}
	return timeOut
}
