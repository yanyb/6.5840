package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := rpc.GetArgs{Key: key}
	var failNum int
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.Get", &args, &reply)
		if !ok {
			failNum++
			if failNum > len(ck.servers) {
				return "", 0, rpc.ErrWrongGroup
			}
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

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	var resend atomic.Bool
	var failNum int
	var err rpc.Err
	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.Put", &args, &reply)
		if !ok {
			failNum++
			lastLeadIndex = (lastLeadIndex + 1) % len(ck.servers)
			resend.CompareAndSwap(false, true)
			if failNum > len(ck.servers) {
				err = rpc.ErrWrongGroup
				break
			}
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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}

	for {
		reply := shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.FreezeShard", &args, &reply)
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
		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}

	for {
		reply := shardrpc.InstallShardReply{}

		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.InstallShard", &args, &reply)
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

		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	ck.mu.Lock()
	lastLeadIndex := ck.lastLeadIndex
	ck.mu.Unlock()
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}

	for {
		reply := shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[lastLeadIndex], "KVServer.DeleteShard", &args, &reply)
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

		return reply.Err
	}
}
