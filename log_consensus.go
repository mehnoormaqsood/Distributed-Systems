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
	"labrpc"
	"sync"
	"time"
    "math/rand"
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

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	role 		int
	votedFor	int
	numOfVotes 	int
	log			[]LogEntry

	commitIndex int //highest log entry committed
	lastAppliedToSM int //index of highest log entry applied to state machine

	//Reinitialised after each election
	nextIndex 	[]int //index of next log entries to send to followers -- reinitialized to last log index + 1
	matchIndex	[]int //index of highest log entry known to be replicated on each server -- reinitialized to 0 

	applyCh			chan ApplyMsg
	chanVote 			chan bool
	chanHeartBeat 		chan bool
	chanElectedLeader	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	if (rf.role == 2){
		isleader = true
	}else{
		isleader = false
	}

	rf.mu.Unlock()

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

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term 	int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextTryIndex int
}

//
// example RequestVote RPC handler.
//

//candidate is up-to-date if its log ends in a greater term or ends in the same term but greater or same index (log length)
func (rf *Raft) checkUpToDate(candidateLastTerm int, candidateLastIndex int) bool {
	return  (candidateLastTerm > rf.lastLogTerm()) || (candidateLastTerm == rf.lastLogTerm() && candidateLastIndex >= rf.lastLogIndex()) //returns true if candidate is uptodate
	//lastLogTerm and lastLogIndex would be 0 at the start of RAFT when the very first leader is to be elected
}
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		return

	} else if args.Term > rf.currentTerm{ //higher term
		rf.currentTerm = args.Term
		rf.role = 0
		rf.votedFor = -1
		rf.numOfVotes = 0
	}
	reply.Term = rf.currentTerm
	//higher term or same term (if rf.votedFor!=-1), may receive votes from multiple candidates in the same term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkUpToDate(args.LastLogTerm, args.LastLogIndex){ //has voted 
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.chanVote <- true
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.lastLogIndex() + 1
		return
	} else if args.Term > rf.currentTerm{ //valid leader now check log
		rf.currentTerm = args.Term
		rf.role = 0
		rf.votedFor = -1
		rf.numOfVotes = 0
	}
	rf.chanHeartBeat <- true //leader is alive

	reply.Term = rf.currentTerm
	//making log consistent with leader

	//case 2.1 : no entry at previous log index, shorter log at follower
	if args.PrevLogIndex > rf.lastLogIndex(){
		reply.NextTryIndex = rf.lastLogIndex()+1 //
		return
	}

	//case 2.2 : prev log entry terms do not match, find the first index for that term 
	confIndex := -1
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm{ 
		for i, t := range rf.log{
			if t.Term == rf.log[args.PrevLogIndex].Term{
				confIndex = i 
				break
			}
		}
		reply.NextTryIndex = confIndex
		return
	}
	
	// case 3 : prev log entry matches
	// 3.1: no entry at current index: append the entries of leader
	// 3.2: delete the log from conflicting entries onwards. If conflicting entry is X, retain peer log till X-1 and append args.Entries[X:]

	entryIndex := 0
	for i:= entryIndex ; i<len(args.Entries) ; i++ {
		currIndexToCheck := args.PrevLogIndex + 1 + i
		//no entry at currIndex 
		if currIndexToCheck > rf.lastLogIndex() {
			entryIndex = i
			break
		} else if rf.log[currIndexToCheck].Term != args.Entries[i].Term { //different term at same curr index
			rf.log = rf.log[:currIndexToCheck]
			entryIndex = i
			break
		}
	}

	//step 4 : append the entries to log
	// rf.log = rf.log[:args.PrevLogIndex+1]
	// rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) > 0{
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
	}
	reply.NextTryIndex = args.PrevLogIndex + len(args.Entries) + 1
	reply.Success = true

	//step 5 :  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		go rf.commitEntries()
	}
	return
}

func (rf *Raft) commitEntries() {
	for i := rf.lastAppliedToSM+1 ; i<= rf.commitIndex; i++{
		msg := ApplyMsg{
			Index: i,
			Command: rf.log[i].Command,
		}
		rf.applyCh <- msg
	}
	rf.lastAppliedToSM = rf.commitIndex
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

func (rf *Raft) lastLogIndex() (int){
	index := rf.log[len(rf.log)-1].Index //at start when there is no log entry, index is 0
	return index
}

func (rf *Raft) lastLogTerm() (int){
	return rf.log[len(rf.log)-1].Term //at start when there is no log entry, term is 0
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := (rf.role == 2)
	if (isLeader){
		index = rf.lastLogIndex() + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command}) //add the log entry and start agreement 
	}

	return index, term, isLeader
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
	
	// Your initialization code here.
	rf.role = 0 //all servers start as followers
	rf.currentTerm = 0
	rf.numOfVotes = 0
	rf.commitIndex = 0
	rf.lastAppliedToSM = 0
	rf.log = append(rf.log, LogEntry{Term:0}) //null log entry at all servers, the log with commands would start at index 1 
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.chanElectedLeader = make(chan bool, 100)
	rf.chanHeartBeat = make(chan bool, 100)
	rf.chanVote = make(chan bool, 100)

	go rf.startFunc()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startFunc(){
	for{
		switch rf.role {
		case 0: //Follower
			select{
			case <-rf.chanHeartBeat: //Leader exists, reset timeout
			case <- rf.chanVote: //Voted for someone, reset timeout
			case <- time.After((time.Duration(300+rand.Intn(100)))*time.Millisecond):// election timeout, become a candidate, switch role
				rf.mu.Lock()
				rf.role = 1
				rf.mu.Unlock()
			}
		case 1: //Candidate
			rf.startElection()
			select {
			case <- rf.chanHeartBeat: //found a leader, revert to follower state
				rf.mu.Lock()
				rf.role = 0
				rf.votedFor = -1
				rf.numOfVotes = 0
				rf.mu.Unlock()
			case <- rf.chanElectedLeader: //became a leader, switch role
			case <- time.After((time.Duration(300+rand.Intn(100)))*time.Millisecond): //no leader elected, start another election
			}
		case 2: //Leader
			rf.sendHeartBeats() //send heartbeats after every 100 milliseconds
		}
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.numOfVotes = 1
	rf.mu.Unlock()
	go rf.requestForVotes() //broadcast vote requests to all peers
}

func (rf *Raft) requestForVotes(){
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm: rf.lastLogTerm(),
	}
	rf.mu.Unlock()
	for peer, _ := range rf.peers {
		if peer!= rf.me && rf.role == 1 { //peers that are not me and I am a candidate
			go func(peer int, args RequestVoteArgs){
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply){ // a reply has been received
					rf.mu.Lock()
					if reply.Term > rf.currentTerm{ //stale term, change to follower and update term
						rf.currentTerm = reply.Term
						rf.role = 0
						rf.votedFor = -1
						rf.numOfVotes = 0
					} else if reply.VoteGranted == true{
						rf.numOfVotes ++
						if rf.numOfVotes > (len(rf.peers))/2{ //won by majority
							rf.role = 2
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							nextIndex := rf.lastLogIndex() + 1
							for i := range rf.nextIndex {
								rf.nextIndex[i] = nextIndex
								rf.matchIndex[i] = 0
							}
							rf.chanElectedLeader <- true //switch role by notifying through channel
						}
					}
					rf.mu.Unlock()
				}
			}(peer, args)
		}
	}
}



func (rf *Raft) sendHeartBeats() {
	for peer, _ := range rf.peers{
		if peer != rf.me && rf.role == 2{
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				entries := rf.log[rf.nextIndex[peer]:]
	
				args:= AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: prevLogIndex ,
					PrevLogTerm: prevLogTerm,
					Entries: entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
	
				go func(peer int, args AppendEntriesArgs){
					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(peer, args, reply){
						rf.mu.Lock()
						if reply.Success == true { //log has been replicated
							if len(args.Entries) > 0 {
								rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
								rf.matchIndex[peer] = rf.nextIndex[peer] - 1
							}
						} else if reply.Success == false{
							if reply.Term > rf.currentTerm{
								rf.role = 0 //switch to follower
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.numOfVotes = 0
							} else {
								rf.nextIndex[peer] = min(reply.NextTryIndex, rf.lastLogIndex())
							}
						}
						rf.mu.Unlock()
	
						for n := rf.lastLogIndex(); n > rf.commitIndex && rf.log[n].Term == rf.currentTerm; n-- {
							count := 1
							for i := range rf.peers {
								if i != rf.me && rf.matchIndex[i] >= n {
									count++
								}
							}
							if count > len(rf.peers)/2{ //found one such n for which condition is true, commit entires upto this n and apply to state machine
								rf.commitIndex = n 
								go rf.commitEntries()
								break
							}
						}

					}
				}(peer, args)
		}
	}
	time.Sleep(100 * time.Millisecond)
}

func min(x, y int) int {
    if x < y {
        return x
    }
    return y
}