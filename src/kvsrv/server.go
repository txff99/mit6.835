package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	KeyValue         map[string]string
	TransactionTrack map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.KeyValue[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.TransactionTrack[args.Transaction]; ok {
		if args.Ack {
			delete(kv.TransactionTrack, args.Transaction)
		}
		return
	}
	kv.KeyValue[args.Key] = args.Value
	kv.TransactionTrack[args.Transaction] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.TransactionTrack[args.Transaction]; ok {
		if args.Ack {
			delete(kv.TransactionTrack, args.Transaction)
		}
		reply.Value = value
		return
	}
	if value, ok := kv.KeyValue[args.Key]; ok {
		reply.Value = value
		kv.KeyValue[args.Key] += args.Value
	} else {
		kv.KeyValue[args.Key] = ""
	}
	kv.TransactionTrack[args.Transaction] = reply.Value
}

func StartKVServer() *KVServer {
	kv := &KVServer{KeyValue: make(map[string]string),
		TransactionTrack: make(map[int64]string)}
	//kv := new(KVServer)
	//You may need initialization code here.

	return kv
}
