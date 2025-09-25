package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// 将一个key映射到一个shard（在此用key首字母简单分片）
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

type Clerk struct {
	sm        *shardctrler.Clerk	// shardctrler 的客户端，用来 Query 最新配置
	config    shardctrler.Config	// 本地缓存的最新配置（sm.Query(-1) 初始化）
	makeEnd   func(string) *labrpc.ClientEnd	// 把服务器名字转成 RPC endpoint
	leaderIds map[int]int			// 对每个 group 记录一个猜测的 leader index（便于轮询）
	clientId  int64 				// 和commandId一起用于去重/幂等
	commandId int64 				// (clientId, commandId) 定义一个唯一的操作
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		makeEnd:   makeEnd,
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		commandId: 0,
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

// 客户端发送一次 KV 操作命令
func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		// 只有当本地缓存的 config 中存在该 gid 才尝试向组内的 servers 发 RPC
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			// 按猜测 leader 轮询尝试 RPC
			for {
				var response CommandResponse
				ok := ck.makeEnd(servers[newLeaderId]).Call("ShardKV.Command", request, &response)
				if ok && (response.Err == OK || response.Err == ErrNoKey) {
					ck.commandId++
					return response.Value
				} else if ok && response.Err == ErrWrongGroup {
					// 这个group不负责该shard，直接break到外层去查询最新配置再重试，组内其它的servers也不用尝试
					break
				} else {
					// ErrWrongLeader/ErrTimeout/ErrNotReady，尝试下一个sever直到遍历完所有后退出
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}

		// 该组不存在，sleep，然后去 shardctrler 查询最新配置后重试
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
