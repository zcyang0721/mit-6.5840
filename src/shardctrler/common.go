package shardctrler

import (
	"fmt"
	"log"
	"time"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// 分片数量，每个key映射到一个分片，如hash(key) % NShards
const NShards = 10

// 配置项，将分片划分配到指定组
type Config struct {
	Num    int              // Config 编号
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]  该 group 的 server 列表
}

func DefaultConfig() Config {
	return Config{Groups: make(map[int][]string)}
}

func (cf Config) String() string {
	return fmt.Sprintf("{Num:%v,Shards:%v,Groups:%v}", cf.Num, cf.Shards, cf.Groups)
}

const ExecuteTimeout = 500 * time.Millisecond

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	*CommandRequest
}

type OperationContext struct {
	MaxAppliedCommandId int64
	LastResponse        *CommandResponse
}

type OperationOp uint8

const (
	OpJoin OperationOp = iota
	OpLeave
	OpMove
	OpQuery
)

func (op OperationOp) String() string {
	switch op {
	case OpJoin:
		return "OpJoin"
	case OpLeave:
		return "OpLeave"
	case OpMove:
		return "OpMove"
	case OpQuery:
		return "OpQuery"
	}
	panic(fmt.Sprintf("unexpected CommandOp %d", op))
}

type Err uint8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type CommandRequest struct {
	Servers   map[int][]string // for Join：新加入的gid -> server[]
	GIDs      []int            // for Leave：删除的group
	Shard     int              // for Move：要移动的分片
	GID       int              // for Move：移动到该group
	Num       int              // for Query：查询的config编号
	Op        OperationOp	   // RPC命令
	ClientId  int64
	CommandId int64
}

func (request CommandRequest) String() string {
	switch request.Op {
	case OpJoin:
		return fmt.Sprintf("{Servers:%v,Op:%v,ClientId:%v,CommandId:%v}", request.Servers, request.Op, request.ClientId, request.CommandId)
	case OpLeave:
		return fmt.Sprintf("{GIDs:%v,Op:%v,ClientId:%v,CommandId:%v}", request.GIDs, request.Op, request.ClientId, request.CommandId)
	case OpMove:
		return fmt.Sprintf("{Shard:%v,Num:%v,Op:%v,ClientId:%v,CommandId:%v}", request.Shard, request.Num, request.Op, request.ClientId, request.CommandId)
	case OpQuery:
		return fmt.Sprintf("{Num:%v,Op:%v,ClientId:%v,CommandId:%v}", request.Num, request.Op, request.ClientId, request.CommandId)
	}
	panic(fmt.Sprintf("unexpected CommandOp %d", request.Op))
}

type CommandResponse struct {
	Err    Err
	Config Config
}

func (response CommandResponse) String() string {
	return fmt.Sprintf("{Err:%v,Config:%v}", response.Err, response.Config)
}
