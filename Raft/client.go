package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID	int64
	requestID	int
	leaderID	int
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
	// You'll have to add code here.
	ck.clientID = nrand()
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
	reqID := ck.requestID + 1
	args := GetArgs{
		Key: key,
		ClientId: ck.clientID,
		RequestId: reqID,
	} 
	reply := GetReply{}
	i := ck.leaderID
	for reply.Err != OK {
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply) //RPC call to leader
		if !ok || reply.WrongLeader == true {
			i = (i+1)%(len(ck.servers))
			ck.leaderID = i //try another server
			continue
		} else if reply.Err == ErrNoKey{
			return "" //key not found
		}
	}
	//request sent and key found, update last request id for this client and return value
	ck.requestID = reqID
	return reply.Value
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
	reqID := ck.requestID + 1
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientID,
		RequestId: reqID,
	}

	reply := PutAppendReply{}
	i := ck.leaderID
	for reply.Err != OK{
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply) //RPC call to leader
		if !ok || reply.WrongLeader == true {
			i = (i+1)%(len(ck.servers))
			ck.leaderID = i //try another server to find leader
			continue
		}
	}
	ck.requestID = reqID
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
