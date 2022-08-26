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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
    Value string
    Command  string

    ClientId  int64
    RequestId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	database	map[string]string
	notify		map[int]chan RequestInfo
	lastApplied	map[int64]int


	// Your definitions here.
}

type RequestInfo struct {
	Index		int
	ClientId	int64
	RequestId	int
}

//PutAppend and Get have to add operations in Raft log using Start()
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op:= Op{
		Key: args.Key,
		Command: "Get",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	//check if leader, if yes add to log and wait until op is applied
	leader := kv.wait(op)
	if leader == true {
		kv.mu.Lock()
		if val, ok := kv.database[args.Key]; ok{
			reply.Value = val
			reply.Err = OK
			reply.WrongLeader = false
			return
		} else{
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}else{
		reply.WrongLeader = true
	}

}

func (kv *RaftKV) wait(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if (isLeader == false){
		return false
	}
	//is leader and entry added to log, wait for it to be applied or removed from log in case of leader changes

	//opening a channel for the index at which the command is to be applied
	kv.mu.Lock()
	_, ok := kv.notify[index]
	if !ok{
		kv.notify[index] = make(chan RequestInfo)
	}
	notifChann := kv.notify[index]
	kv.mu.Unlock()

	for{

		select{
		case reqApplied := <-notifChann: //check if same request is applied at index
			kv.mu.Lock()
			delete(kv.notify, index)
			kv.mu.Unlock()

			if index == reqApplied.Index && reqApplied.ClientId == op.ClientId && reqApplied.RequestId == op.RequestId{
				return true
			} else {
				return false
			}
		case <- time.After(100 * time.Millisecond): //check for leader changes
			kv.mu.Lock()
			_, leaderNow := kv.rf.GetState()
			if !leaderNow{
				delete(kv.notify, index)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()
		}
	}

}
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op:= Op{
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	if args.Op == "Put"{
		op.Command = "Put"
	} else{
		op.Command = "Append"
	}

	leader := kv.wait(op)
	if leader == true{
		reply.Err = OK
		reply.WrongLeader = false
	} else{
		reply.WrongLeader = true
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
	kv.database = make(map[string]string)
	kv.notify = make(map[int]chan RequestInfo)
	kv.lastApplied = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.start()
	return kv
}

func (kv *RaftKV) start(){
	for{
		select{
		case req := <- kv.applyCh: //msgs returned on applyCh are recieved here
			op:= req.Command.(Op) //command committed by raft leader 
			//at-most-once, check for duplicity of request by client
			kv.mu.Lock()
			flag := kv.checkDuplicate(op)
			if flag==true{ //duplicate req
				kv.mu.Unlock()
				continue 
			}
			//unique request, process it and notify the request handlers 
			if op.Command == "Put"{
				kv.database[op.Key] = op.Value
			} else if op.Command == "Append"{
				if _, ok := kv.database[op.Key]; ok {
					kv.database[op.Key] += op.Value
				}else{
					kv.database[op.Key] = op.Value
				}
			}

			kv.lastApplied[op.ClientId] = op.RequestId
			//send info of the request applied on the index channel
			if notifChann, ok := kv.notify[req.Index]; ok{ 
				notifChann <- RequestInfo{
					Index:	req.Index,
					ClientId: op.ClientId,
					RequestId: op.RequestId,
				}
			}
			
			kv.mu.Unlock()

		}
	}

}
func (kv *RaftKV) checkDuplicate(op Op) bool{
	lastReqId, ok := kv.lastApplied[op.ClientId]
	if ok == true && op.RequestId <= lastReqId{
		return true //requests sent by client sequentially, a request with ID greater than current req has already been applied, so previous ID should have already been processed
	}else{
		return false
	}
}