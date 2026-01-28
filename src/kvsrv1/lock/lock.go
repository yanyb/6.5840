package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	tester "6.5840/tester1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	k  string
	v  string
	vr rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.k = l
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		v, vr, err := lk.ck.Get(lk.k)
		if err == rpc.OK && v != "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		lk.v = tester.Randstring(6)
		lk.vr = vr

		err = lk.ck.Put(lk.k, lk.v, lk.vr)
		var needRecheck bool
		switch err {
		case rpc.OK:
			// get the key success
			return
		case rpc.ErrMaybe:
			// need to recheck if you get the key or not,may you get the key success
			needRecheck = true
			break
		case rpc.ErrVersion:
			// not get the key
			break
		case rpc.ErrNoKey:
			// impossible
		}

		if needRecheck {
			v, vr, err = lk.ck.Get(lk.k)
			if err == rpc.OK && lk.v == v && lk.vr+1 == vr {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		var needRecheck bool
		err := lk.ck.Put(lk.k, "", lk.vr+1)
		switch err {
		case rpc.OK:
			// release ok
			return
		case rpc.ErrMaybe:
			needRecheck = true
		case rpc.ErrVersion:
			// maybe someone use the lock incorrectly
			break
		case rpc.ErrNoKey:
			// impossible
		}

		if needRecheck {
			_, vr, err := lk.ck.Get(lk.k)
			if err == rpc.OK && vr >= lk.vr+1 {
				return
			}
		}
	}
}
