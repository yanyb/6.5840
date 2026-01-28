package shardgrp

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu        sync.Mutex
	reqMaxNum shardcfg.Tnum
	maxNum    shardcfg.Tnum
	ksm       map[shardcfg.Tshid]*Shard
}

type Shard struct {
	Kvm    map[string]*V
	Frozen bool
}

type V struct {
	Value   string
	Version rpc.Tversion
}

type Snapshot struct {
	MaxNum shardcfg.Tnum
	Ksm    map[shardcfg.Tshid]*Shard
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch r := req.(type) {
	case rpc.GetArgs:
		shid := shardcfg.Key2Shard(r.Key)
		shard, exist := kv.ksm[shid]
		if !exist {
			return &rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		}

		if shard.Frozen {
			return rpc.GetReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		v, exist := shard.Kvm[r.Key]
		if !exist {
			return rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		}
		return rpc.GetReply{
			Value:   v.Value,
			Version: v.Version,
			Err:     rpc.OK,
		}
	case rpc.PutArgs:
		shid := shardcfg.Key2Shard(r.Key)
		shard, exist := kv.ksm[shid]
		if !exist {
			if r.Version == 0 {
				shard = &Shard{
					Kvm: make(map[string]*V),
				}
				kv.ksm[shid] = shard
			} else {
				return rpc.PutReply{
					Err: rpc.ErrNoKey,
				}
			}
		}

		if shard.Frozen {
			return rpc.PutReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		v, exist := shard.Kvm[r.Key]
		if !exist {
			if r.Version == 0 {
				shard.Kvm[r.Key] = &V{
					Value:   r.Value,
					Version: 1,
				}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			}
			return rpc.PutReply{
				Err: rpc.ErrNoKey,
			}
		}

		if v.Version != r.Version {
			return rpc.PutReply{
				Err: rpc.ErrVersion,
			}
		}

		v.Value = r.Value
		v.Version++

		return rpc.PutReply{
			Err: rpc.OK,
		}
	case shardrpc.FreezeShardArgs:
		shard, exist := kv.ksm[r.Shard]
		if !exist {
			shard = &Shard{
				Kvm: make(map[string]*V),
			}
			kv.ksm[r.Shard] = shard
		}
		shard.Frozen = true

		// copy kv
		kvm := make(map[string]*V)
		if shard != nil {
			for k, v := range shard.Kvm {
				kvm[k] = v
			}
		}

		// tricky marshal must before set frozen
		b, err := json.Marshal(kvm)
		if err != nil {
			log.Fatalf("Marshal err %v", err)
		}

		if r.Num > kv.maxNum {
			kv.maxNum = r.Num
		}

		return shardrpc.FreezeShardReply{
			State: b,
			Num:   r.Num,
			Err:   rpc.OK,
		}
	case shardrpc.InstallShardArgs:
		shard, exist := kv.ksm[r.Shard]
		if !exist {
			shard = &Shard{
				Kvm: make(map[string]*V),
			}
			kv.ksm[r.Shard] = shard
		}

		err := json.Unmarshal(r.State, &shard.Kvm)
		if err != nil {
			log.Fatalf("Unmarshal err %v", err)
		}

		shard.Frozen = false

		if r.Num > kv.maxNum {
			kv.maxNum = r.Num
		}

		return shardrpc.InstallShardReply{
			Err: rpc.OK,
		}
	case shardrpc.DeleteShardArgs:
		shard, exist := kv.ksm[r.Shard]
		if exist {
			shard.Kvm = nil
		}

		if r.Num > kv.maxNum {
			kv.maxNum = r.Num
		}

		return shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := Snapshot{
		MaxNum: kv.maxNum,
		Ksm:    make(map[shardcfg.Tshid]*Shard),
	}

	for shid, shard := range kv.ksm {
		snapshot.Ksm[shid] = &Shard{
			Frozen: shard.Frozen,
			Kvm:    make(map[string]*V),
		}
		for k, v := range shard.Kvm {
			snapshot.Ksm[shid].Kvm[k] = v
		}
	}

	b, err := json.Marshal(snapshot)
	if err != nil {
		log.Fatalf("Marshal err %v", err)
	}

	return b
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var snapshot Snapshot
	err := json.Unmarshal(data, &snapshot)
	if err != nil {
		log.Fatalf("Unmarshal err %v", err)
	}

	kv.ksm = snapshot.Ksm
	kv.maxNum = snapshot.MaxNum
	if kv.maxNum > kv.reqMaxNum {
		kv.reqMaxNum = kv.maxNum
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case rpc.GetReply:
		reply.Value = r.Value
		reply.Version = r.Version
		reply.Err = r.Err
	default:
		reply.Err = err
	}
	return
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case rpc.PutReply:
		reply.Err = r.Err
	default:
		reply.Err = err
	}
	return
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	kv.mu.Lock()
	if kv.reqMaxNum > args.Num {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.reqMaxNum = args.Num
	kv.mu.Unlock()

	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case shardrpc.FreezeShardReply:
		reply.State = r.State
		reply.Num = r.Num
		reply.Err = r.Err
	default:
		reply.Err = err
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	kv.mu.Lock()
	if kv.reqMaxNum > args.Num {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.reqMaxNum = args.Num
	kv.mu.Unlock()

	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case shardrpc.InstallShardReply:
		reply.Err = r.Err
	default:
		reply.Err = err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	kv.mu.Lock()
	if kv.reqMaxNum > args.Num {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.reqMaxNum = args.Num
	kv.mu.Unlock()

	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case shardrpc.DeleteShardReply:
		reply.Err = r.Err
	default:
		reply.Err = err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.ksm = make(map[shardcfg.Tshid]*Shard)

	return []tester.IService{kv, kv.rsm.Raft()}
}
