package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
	"sync"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu             sync.Mutex
	key            string
	shardgrpClerkM map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.key = "config"
	sck.shardgrpClerkM = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	value := cfg.String()
	sck.IKVClerk.Put(sck.key, value, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	value, version, _ := sck.IKVClerk.Get(sck.key)
	old := shardcfg.FromString(value)
	for sid, ogid := range old.Shards {
		shid := shardcfg.Tshid(sid)
		ngid := new.Shards[shid]
		if ngid != ogid {
			_, servers1, _ := old.GidServers(shid)
			oClerk := sck.GetShardgrpClerk(ogid, servers1)

			_, servers2, _ := new.GidServers(shid)
			nClerk := sck.GetShardgrpClerk(ngid, servers2)

			// freeze
			b, err := oClerk.FreezeShard(shid, old.Num)
			if err != rpc.OK {
				return
			}

			// install
			err = nClerk.InstallShard(shid, b, new.Num)
			if err != rpc.OK {
				return
			}

			// delete
			err = oClerk.DeleteShard(shid, old.Num)
			if err != rpc.OK {
				return
			}
		}
	}

	// save
	value = new.String()
	sck.IKVClerk.Put(sck.key, value, version)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, _ := sck.IKVClerk.Get(sck.key)
	return shardcfg.FromString(value)
}

func (sck *ShardCtrler) GetShardgrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	sck.mu.Lock()
	defer sck.mu.Unlock()
	clerk, exist := sck.shardgrpClerkM[gid]
	if !exist {
		clerk = shardgrp.MakeClerk(sck.clnt, servers)
		sck.shardgrpClerkM[gid] = clerk
	}
	return clerk
}
