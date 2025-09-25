package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// Raft 层与上层服务交互的消息格式，通过 applyCh 管道传递。
type ApplyMsg struct {
	CommandValid bool	// true：普通日志消息
	Command      interface{}	// 存放被提交的应用层命令
	CommandTerm  int	// 这条日志在 Raft 中写入时的 term
	CommandIndex int	// 这条日志在 Raft 日志里的索引

	SnapshotValid bool	// true：快照消息
	Snapshot      []byte	// 快照的二进制内容
	SnapshotTerm  int	// 这个快照对应的最后一条日志的 term
	SnapshotIndex int	// 这个快照包含的最后一条日志的 index
}

func (msg ApplyMsg) String() string {
	if msg.CommandValid {
		return fmt.Sprintf("{Command:%v,CommandTerm:%v,CommandIndex:%v}", msg.Command, msg.CommandTerm, msg.CommandIndex)
	} else if msg.SnapshotValid {
		return fmt.Sprintf("{Snapshot:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
	} else {
		panic(fmt.Sprintf("unexpected ApplyMsg{CommandValid:%v,CommandTerm:%v,CommandIndex:%v,SnapshotValid:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.CommandValid, msg.CommandTerm, msg.CommandIndex, msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex))
	}
}

type NodeState uint8

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	panic(fmt.Sprintf("unexpected NodeState %d", state))
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry Entry) String() string {
	return fmt.Sprintf("{Index:%v,Term:%v}", entry.Index, entry.Term)
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

// 心跳频率稳定在125毫秒/次
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

// 得到1000到1999毫秒之间的随机超时时间
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func insertionSort(sl []int) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// 释放切片底层数组中未被使用的、多余的内存空间，防止内存泄漏
func shrinkEntriesArray(entries []Entry) []Entry {
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {	// 使用的空间不到总容量的一半
		// 创建并返回紧凑数组
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}