package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd	// 服务器节点的RPC客户端列表
	leaderId  int64					// 领导者服务器索引
	clientId  int64 	// 客户端唯一标识，由nrand()生成随机数，更好的方式是使用分布式id生成算法避免随机数冲突
	commandId int64 	// 命令序列号，(clientId, commandId) 唯一确定一个操作
}

// 生成一个范围在[0, 2^62)之间的随机整数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

// 客户端操作接口

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

// 发送Get/Put/Append请求RPC		ok := ck.servers[i].Call("KVServer.Command", &request, &response)
func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		// 向当前认为的领导者发送RPC请求
		var response CommandResponse
		if !ck.servers[ck.leaderId].Call("KVServer.Command", request, &response) ||
			response.Err == ErrWrongLeader || response.Err == ErrTimeout {
			// 循环尝试发送请求直到成功
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		// 客户端收到服务器的成功响应，客户端序列号递增启用新命令
		ck.commandId++
		return response.Value
	}
}
