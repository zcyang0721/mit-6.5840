package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // 读写锁，用于保护节点共享状态的并发访问
	peers     []*labrpc.ClientEnd // 集群中所有节点的 RPC 通信端点列表
	persister *Persister          // 持久化存储对象，用于保存 Raft 的核心状态和状态机快照
	me        int                 // 当前节点索引，当前节点 peers[me]
	dead      int32               // set by Kill()，控制 goroutine 的退出逻辑

	applyCh        chan ApplyMsg  // 用于向状态机传递已提交日志条目的通道
	applyCond      *sync.Cond     // 条件变量，用于在新日志被提交后唤醒负责将日志应用到状态机的 goroutine
	replicatorCond []*sync.Cond   // 每个跟随者对应一个条件变量，用于触发批量复制日志的 goroutine
	state          NodeState	  // 节点角色状态

	currentTerm int				// 当前任期号，节点发起选举时递增
	votedFor    int				// 当前任期内投票给的候选者 ID
	logs        []Entry 		// 日志条目数组，每个entry包含了日志索引、状态机命令和接收该条目的任期号

    // 以下4个字段为易失状态

	commitIndex int             // 已提交的日志条目的最大索引（已提交指已被大多数服务器复制）
	lastApplied int				// 已应用到状态机的最大日志索引

	nextIndex   []int           // 记录对每个跟随者的下一个要发送的日志索引（初始为leader最后一条日志索引+1）
	matchIndex  []int           // 记录每个跟随者已经复制的最高的日志索引，进而判断日志是否提交

	electionTimer  *time.Timer  // 选举计时器，由跟随者和候选者维护
	heartbeatTimer *time.Timer  // 心跳计时器，仅领导者使用
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	// there will always be at least one entry in rf.logs
	rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// 被上层服务（状态机）调用，用于有条件地安装通过 applyCh 接收到的快照
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check"+
            "whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// 快照的最后包含索引<=当前的提交索引，说明在快照传递过程中，节点已经提交了更新的日志，拒绝安装快照
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger",
                    rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)  // 情况1：如果快照索引超过最后日志索引，创建全新的日志数组（只包含一个虚拟条目）
	} else {
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil    // 情况2：如果快照索引在日志范围内，截断日志并保留快照索引之后的条目
	}

	// 更新元数据和虚拟条目
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)   // 持久化状态
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
            "after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v",
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(),
            rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// 创建快照（日志压缩）
// 上层服务通知 Raft 模块，已经将直到索引 index 的所有状态都保存到快照里了，不再需要维护这些日志条目了，可以安全截断；
// index: 应用程序已经完成处理的最高日志索引
// snapshot: 应用程序序列化后的状态机数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index     // 获取当前快照索引
	if index <= snapshotIndex {     // 拒绝过时请求
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v"+
                "is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
            "after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller",
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
            rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

// 投票者处理投票请求的RPC
func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
                "before processing requestVoteRequest %v and reply requestVoteResponse %v",
                rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

    // 拒绝投票：1.请求任期小于当前任期 2.请求任期等于当前任期，但本节点已经投过票，且投的不是这个候选人
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}

    // 发现更高任期：退位并更新任期，但只是重置投票记录，还未投票给本候选人
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
    // 拒绝投票：3.候选人日志不如本节点更新
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}

    // 同意投票
	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	response.Term, response.VoteGranted = rf.currentTerm, true
}

// Follower 节点调用，用于处理 Leader 发来的心跳或日志复制请求的RPC
func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	// 锁定与持久化
    rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
            "before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", 
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, 
            rf.getFirstLog(), rf.getLastLog(), request, response)

    //【1】 任期检查与状态维护
    // 请求来自一个过期的 Leader（任期号小于自己），则直接拒绝
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

    // 来自更高任期，无条件承认其权威
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

    // 收到合法任期的心跳后，转变为 Follower
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

    //【2】 日志一致性检查
    // Leader 要复制的日志起点已经被本节点制作成了快照并截断了，触发 Leader 的快照发送逻辑
	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v}" + 
                "because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId,
                request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

    // 任期冲突处理
	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
        // 情况1：Follower的日志太短，在PrevLogIndex处根本没有条目
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
        // 情况2：Follower在PrevLogIndex处有条目，但任期不匹配
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term   // 获取冲突的任期
			// 回溯到冲突任期的第一个条目的位置
            index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index  // 告诉Leader冲突任期的第一个索引
		}
		return
	}

    //【3】 接受并追加日志条目
	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
        // 从 PrevLogIndex + 1 的位置开始，检查每个条目
        // 超出了本地日志的范围，或者该位置的条目任期与 Leader 发来的不匹配，则执行强制覆盖
        // 保证了 Follower 的日志最终会和 Leader 的日志完全一致
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true
}

// follower 应用快照，通过 applyCh 通知状态机应用快照
func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
            "before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v",
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// outdated snapshot
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

    // 启动 goroutine 异步发送快照到应用通道
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

// 发送 RPC 到目标服务器的示例代码
// labrpc 包模拟了一个有损网络，其中服务器可能无法访问，请求和回复可能会丢失。

// RequestVote RPC
func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	return rf.peers[server].Call("Raft.RequestVote", request, response)
}

// AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", request, response)
}

// InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", request, response)
}

// 领导选举核心部分，由 Candidate 状态的节点调用，用于向集群中的所有其他节点拉票
func (rf *Raft) StartElection() {

    /// 准备选举请求并为自己投票
	request := rf.genRequestVoteRequest()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()

    /// 遍历除自身外所有节点，为每个节点开一个协程并行发送 RequestVote RPC
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteResponse)
            // 执行实际的 RPC 调用。这是一个同步阻塞调用，会等待直到收到响应、超时或出现网络错误。
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", 
                            rf.me, response, peer, request, rf.currentTerm)
                // 检查当前节点是否仍为 Candidate 状态，并且 term 与发送请求时相同
                // 因为在等待投票结果的过程中，节点可能会收到其他节点的心跳或投票请求，导致节点任期和状态发生变化
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {                   // 处理同意投票
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {  // 处理拒绝投票（发现更高任期）
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v",
                                    rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// 需要立即发送以保持领导地位
			go rf.replicateOneRound(peer)
		} else {
			// 只是向信号复制器协程发出信号，以便批量发送条目
			rf.replicatorCond[peer].Signal()
		}
	}
}

// Leader 节点向特定 Follower 执行单轮日志复制。
// 根据 Follower 的日志落后情况，智能地选择是发送普通的日志追加请求（AppendEntries）还是发送快照（InstallSnapshot）
func (rf *Raft) replicateOneRound(peer int) {
    // 状态检查
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
    // rf.getFirstLog().Index 是 Leader 日志中最早的一条日志的索引。如果 Leader 已经制作了快照，这个值会大于 1
	if prevLogIndex < rf.getFirstLog().Index {
		// 分支 1: 需要发送快照
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		// 分支 2: 可以发送日志条目
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

// 生成一个 RequestVote RPC 的请求参数
func (rf *Raft) genRequestVoteRequest() *RequestVoteRequest {
	lastLog := rf.getLastLog()
	return &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}

// 领导者生成一个 AppendEntries RPC 的请求参数，需要复制的为prevLogIndex之后的日志条目
func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

// Leader 节点处理来自 Follower 的 AppendEntries RPC 响应的核心方法。
// 根据响应的成功与否来更新 Follower 的复制状态，并可能推进提交索引或处理任期过期问题
func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Success {   // 响应成功，表示日志条目已成功复制
            // 更新 matchIndex 为最后一条被复制的日志的索引（请求中前一条日志索引+本次发送的日志条目数）
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if response.Term > rf.currentTerm {     // 失败原因一：发现更高任期
				rf.ChangeState(StateFollower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm { // 失败原因二：日志不一致
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+ 
            "after handling AppendEntriesResponse %v for AppendEntriesRequest %v",
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
            rf.getFirstLog(), rf.getLastLog(), response, request)
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	firstLog := rf.getFirstLog()
	return &InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

// leader处理安装快照RPC响应
func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Term > rf.currentTerm {
			rf.ChangeState(StateFollower)
			rf.currentTerm, rf.votedFor = response.Term, -1
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = request.LastIncludedIndex, request.LastIncludedIndex+1
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}"+
            "after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v",
            rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
            rf.getFirstLog(), rf.getLastLog(), response, request)
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %d} changes state from %s to %s in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateCandidate:	// 候选者不在此处处理，而是放在ticker函数中方便查看
	case StateLeader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// 检查所有Follower的matchIndex，若某个日志索引已被大多数节点复制，则提交该索引及之前的所有日志
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	insertionSort(srt)
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// only advance commitIndex for current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d",
                        rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			DPrintf("{Node %d} can not advance commitIndex from %d because the term of newCommitIndex %d"+
                    "is not equal to currentTerm %d", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}

// 更新 Follower 节点的提交索引
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
    // Follower 只能提交那些它本地已经存在的日志条目
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d",
                rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()   // 通过条件变量来唤醒 applier 协程
	}
}

// 获取日志中的最后一个条目
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

// 投票者用于判断候选人日志是否更新，这两个条件都是确保请求节点的日志更长更新
func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	// 要么任期大，任期相同索引要更长，这样确保该候选者日志更新
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// 跟随者判断日志是否匹配
func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

// used by Start function to append a new Entry to logs
func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.persist()
	return newLog
}

// 被 replicator 协程调用，判断是否需要复制日志
//
// 1. 节点必须是 Leader 状态
// 2. 跟随者节点的 matchIndex 必须小于领导者日志的最后一个索引
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// used by upper layer to detect whether there are any logs in current term
func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastLog().Term == rf.currentTerm
}

// Raft 模块对上层服务的主要接口，允许服务请求（如键值存储）向 Raft 集群提交命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

    // 确保只有领导者才能接受客户端请求
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) Me() int {
	return rf.me
}

// 超时处理函数
// 负责处理选举超时（跟随者和候选者）和心跳超时（领导者）
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
        // Case 1: 选举超时触发 -> 状态转移、任期递增、发起选举、重置计时
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
        // Case 2: 心跳超时触发 -> 状态检查、广播心跳、重置计时
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// 安全地将已提交（committed）的日志条目应用到上层状态机
// 独立的 goroutine 确保每个日志条目都恰好通过 applyCh 发送给上层服务一次，且与Raft模块内部提交新日志并行执行
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
            // 没有新的已提交日志需要应用，释放锁，阻塞等待applyCond信号唤醒
            // 避免忙等待，协程让出CPU而不是不断循环检查；其他协程commitIndex增加时可立即唤醒本applier协程
			rf.applyCond.Wait()
		}

        // 准备好要应用的日志条目
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()

        // 应用日志到上层状态机
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}

        // 更新已应用的索引 lastApplied
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v",
                    rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// rf.commitIndex 可能已经更改，因此使用 commitIndex
        // 处理快照时lastApplied可能回滚，直接赋值commitIndex会覆盖，使用Max确保递增的同时不会覆盖正确的回滚操作
        rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// 为每个 Follower 节点专门创建一个独立的日志复制协程
//
// 替代简单的周期心跳：Leader 只有一个定时器，每隔一段时间就向所有 Follower 发送一次心跳或日志。
// 这种方式的问题是：
// 1.延迟高：即使有新日志产生，也要等到下一个心跳周期才能发送。
// 2.不均衡：有些 Follower 可能落后很多，需要更频繁的更新，而有些则是最新的，不需要发送数据。
// replicator 模式的优势：
// 即时性：当 Leader 有新的日志产生时，立即调用 rf.replicatorCond[peer].Signal() 来唤醒正在休眠的特定 replicator 协程。
// 这使得日志可以几乎立即被发送给 Follower，而不是等待下一个心跳周期，大大降低了复制延迟。
// 专用性：每个 Follower 都有自己的复制协程。这意味着为一个缓慢的 Follower 进行重试或追赶不会影响到向其他 Follower 的复制。
// 效率：使用条件变量 Wait() 而不是简单的睡眠循环，在无事可做时协程会彻底阻塞，不消耗 CPU 资源。
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
        // 在不需要工作时释放 CPU，等待信号
		// 一旦需要工作，它会持续调用 replicateOneRound(peer) 直到该 peer 追上最新日志
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

// 创建一个 Raft 服务器
// @persister: 用于持久化状态的对象
// @applyCh: 用于发送 ApplyMsg 消息的通道
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	rf.readPersist(persister.ReadRaftState())	// 从持久化状态中恢复
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
        // 初始化复制索引，实际复制时如果发现日志不匹配，会递减 nextIndex 并重试（Raft 协议的日志回溯机制）
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
        // 为其他节点启动复制协程
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// 为每个foller启动一个独立的日志复制协程，避免单协程处理所有复制任务导致的阻塞
			go rf.replicator(i)
		}
	}
	// 开启节点状态转移协程
	go rf.ticker()
	// 开启应用日志到状态机的协程
	go rf.applier()

	return rf
}
