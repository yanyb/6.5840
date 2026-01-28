package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu  sync.Mutex
	kvm map[string]*V
}

type V struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		v, exist := kv.kvm[r.Key]
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
		v, exist := kv.kvm[r.Key]
		if !exist {
			if r.Version == 0 {
				kv.kvm[r.Key] = &V{
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
	default:
		log.Fatalf("DoOp should execute only Get and Put not %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	for k, v := range kv.kvm {
		e.Encode(k)
		e.Encode(v.Value)
		e.Encode(v.Version)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	for {
		var k string
		var value string
		var version rpc.Tversion
		if d.Decode(&k) != nil || d.Decode(&value) != nil || d.Decode(&version) != nil {
			break
		} else {
			kv.kvm[k] = &V{
				Value:   value,
				Version: version,
			}
		}
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	switch r := rep.(type) {
	case rpc.PutReply:
		reply.Err = r.Err
	default:
		reply.Err = err
	}
	return
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.kvm = map[string]*V{}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
