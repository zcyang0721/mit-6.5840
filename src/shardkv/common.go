package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ExecuteTimeout            = 500 * time.Millisecond
	ConfigureMonitorTimeout   = 100 * time.Millisecond
	MigrationMonitorTimeout   = 50 * time.Millisecond
	GCMonitorTimeout          = 50 * time.Millisecond
	EmptyEntryDetectorTimeout = 200 * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrOutDated
	ErrTimeout
	ErrNotReady
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrOutDated:
		return "ErrOutDated"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrNotReady:
		return "ErrNotReady"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota   // 正常提供服务
    Pulling                      // 正在从旧群组拉取数据（新接管方）
    BePulling                    // 正在被别的 group 拉取（旧持有方）
    GCing                        // 正在进行垃圾回收（旧持有方），此时已可服务
)

func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	}
	panic(fmt.Sprintf("unexpected ShardStatus %d", status))
}

// 客户端去重上下文
type OperationContext struct {
	MaxAppliedCommandId int64
	LastResponse        *CommandResponse
}

func (context OperationContext) deepCopy() OperationContext {
	return OperationContext{context.MaxAppliedCommandId, &CommandResponse{context.LastResponse.Err, context.LastResponse.Value}}
}

// 写入 Raft 的统一日志条目封装体
type Command struct {
	Op   CommandType	// 命令类型
	Data interface{}	// 具体 payload（不同类型下会是不同结构）
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(request *CommandRequest) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(response *ShardOperationResponse) Command {
	return Command{InsertShards, *response}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShards, *request}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

type CommandType uint8

const (
	Operation CommandType = iota	// 客户端的 Put/Get/Append 操作（Data 为 CommandRequest）
	Configuration	// config 变更（Data 为 shardctrler.Config）
	InsertShards	// 插入（安装）从旧 group 拉来的 shard（Data 为 ShardOperationResponse）
	DeleteShards	// 删除本组上已经迁移走的 shard（Data 为 ShardOperationRequest）
	EmptyEntry		// 空条目，常用于快照/保持 leader 活跃或作为 commit 空检测
)

func (op CommandType) String() string {
	switch op {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case InsertShards:
		return "InsertShards"
	case DeleteShards:
		return "DeleteShards"
	case EmptyEntry:
		return "EmptyEntry"
	}
	panic(fmt.Sprintf("unexpected CommandType %d", op))
}

type OperationOp uint8

const (
	OpPut OperationOp = iota
	OpAppend
	OpGet
)

func (op OperationOp) String() string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	}
	panic(fmt.Sprintf("unexpected OperationOp %d", op))
}

type CommandRequest struct {
	Key       string
	Value     string
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

func (request CommandRequest) String() string {
	return fmt.Sprintf("Shard:%v,Key:%v,Value:%v,Op:%v,ClientId:%v,CommandId:%v}", key2shard(request.Key), request.Key, request.Value, request.Op, request.ClientId, request.CommandId)
}

type CommandResponse struct {
	Err   Err
	Value string
}

func (response CommandResponse) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", response.Err, response.Value)
}

// 新组向旧组发起“拉取”请求
type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}

func (request ShardOperationRequest) String() string {
	return fmt.Sprintf("{ConfigNum:%v,ShardIDs:%v}", request.ConfigNum, request.ShardIDs)
}

// 旧组回复给新组的迁移包
type ShardOperationResponse struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string	// 每个要迁移的 shard 的具体 key/value 数据
	LastOperations map[int64]OperationContext	// 旧组对每个客户端的去重上下文
}

func (response ShardOperationResponse) String() string {
	return fmt.Sprintf("{Err:%v,ConfigNum:%v,ShardIDs:%v,LastOperations:%v}", response.Err, response.ConfigNum, response.Shards, response.LastOperations)
}
