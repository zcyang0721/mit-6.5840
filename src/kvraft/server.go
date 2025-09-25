package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.RWMutex
	dead    int32		// 标记服务器是否已关闭
	rf      *raft.Raft	// 指向底层 Raft 共识算法实例的指针
	applyCh chan raft.ApplyMsg	// 接收 Raft 层已提交的日志条目

	maxRaftState int 	// 触发快照的 Raft 日志大小阈值
	lastApplied  int 	// 记录最后一个已应用到状态机的日志索引

	stateMachine   KVStateMachine                // 键值存储状态机接口，封装了实际的键值存储操作（内存中的map）
	lastOperations map[int64]OperationContext    // 记录每个客户端的最后一次操作上下文(clientId, OperationContext)
	notifyChans    map[int]chan *CommandResponse // 按日志索引映射的通知通道
}

// 处理客户端命令请求的 RPC 方法
func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), request, response)
	// 重复请求检查，如果是重复请求，直接返回之前存储的响应结果
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	
	// 提交命令到 Raft 层
	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	// 获取通知通道并等待结果
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	
	// 异步清理通知通道
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// 用于检测重复请求
func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId	// 重复请求
}

func (kv *KVServer) Kill() {
	DPrintf("{Node %v} has been killed", kv.rf.Me())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// 专门负责监听 applyCh 通道，接收已提交的日志条目，将其应用到状态机
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), message)
			if message.CommandValid {
				kv.mu.Lock()
				// 检查日志索引是否已过时
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored",
							kv.rf.Me(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command := message.Command.(Command)
				// 重复请求，使用之前存储的响应结果；非重复请求，将命令应用到状态机并存储响应结果
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v",
							kv.rf.Me(), message, kv.lastOperations[command.ClientId], command.ClientId)
					response = kv.lastOperations[command.ClientId].LastResponse
				} else {
					response = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// 领导者通过命令通知通道响应客户端请求
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

// 用于判断是否需要创建快照
func (kv *KVServer) needSnapshot() bool {
	return kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState
}

// 在指定索引处创建系统状态的快照
func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)			// 创建一个字节缓冲区，用于存储编码后的快照数据
	e := labgob.NewEncoder(w)		// 创建一个 labgob 编码器，用于将数据结构序列化为字节流
	e.Encode(kv.stateMachine)		// 编码当前键值存储状态机的状态
	e.Encode(kv.lastOperations)		// 编码客户端最后操作记录，用于重复请求检测
	kv.rf.Snapshot(index, w.Bytes())
}

// 将快照数据恢复到状态机、lastOperations 等状态
func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]OperationContext
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.rf.Me())
	}
	kv.stateMachine, kv.lastOperations = &stateMachine, lastOperations
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandResponse {
	var value string
	var err Err
	switch command.Op {
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	}
	return &CommandResponse{err, value}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	
	labgob.Register(Command{})			// 注册 Command 结构体，使其可以被 RPC 库序列化和反序列化
	applyCh := make(chan raft.ApplyMsg)	// 创建通道，用于接收 Raft 层已提交的日志条目

	kv := &KVServer{
		maxRaftState:   maxraftstate,
		applyCh:        applyCh,
		dead:           0,
		lastApplied:    0,
		rf:             raft.Make(servers, me, persister, applyCh),
		stateMachine:   NewMemoryKV(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandResponse),
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	// 启动应用器协程
	go kv.applier()

	DPrintf("{Node %v} has started", kv.rf.Me())
	return kv
}
