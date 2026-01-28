package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"sync"
	"sync/atomic"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu            sync.Mutex
	lastLeadIndex int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := &rpc.GetArgs{
		Key: key,
	}

	for {
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.Get", args, reply)
		if !ok {
			lastLeadIndex = (lastLeadIndex + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			lastLeadIndex = (lastLeadIndex + 1) % len(ck.servers)
			continue
		}

		ck.mu.Lock()
		ck.lastLeadIndex = lastLeadIndex
		ck.mu.Unlock()

		return reply.Value, reply.Version, reply.Err
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	var resend atomic.Bool
	var err rpc.Err
	for {
		reply := &rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.Put", args, reply)
		if !ok {
			lastLeadIndex = (lastLeadIndex + 1) % len(ck.servers)
			resend.CompareAndSwap(false, true)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			lastLeadIndex = (lastLeadIndex + 1) % len(ck.servers)
			resend.CompareAndSwap(false, true)
			continue
		}

		err = reply.Err
		if reply.Err == rpc.ErrVersion {
			if resend.Load() {
				err = rpc.ErrMaybe
			}
		}

		break
	}

	ck.mu.Lock()
	ck.lastLeadIndex = lastLeadIndex
	ck.mu.Unlock()

	return err
}
