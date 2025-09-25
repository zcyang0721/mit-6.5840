package raft

import "fmt"

// candidate请求投票RPC的请求参数，用于领导选举
type RequestVoteRequest struct {
	Term         int	// 候选者任期号
	CandidateId  int	// 候选者节点 ID
	LastLogIndex int	// 候选者最后一条日志条目的索引（用于判断 “日志是否比投票者更新”）
	LastLogTerm  int	// 候选者最后一条日志条目的任期
}
func (request RequestVoteRequest) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}",
					request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

// candidate请求投票RPC的响应参数
type RequestVoteResponse struct {
	Term        int		// 投票者的当前任期号
	VoteGranted bool	// 是否给候选者投票
}
func (response RequestVoteResponse) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", response.Term, response.VoteGranted)
}

// leader复制日志和心跳RPC的请求参数（同步==复制）
type AppendEntriesRequest struct {
	Term         int		// leader 任期号
	LeaderId     int		// leader 节点 ID（用于 Follower 重定向客户端）
	PrevLogIndex int		// leader 中要复制的新日志条目之前一个条目的索引（用于一致性检查）
	PrevLogTerm  int		// leader 中要复制的新日志条目之前一个条目的任期（用于一致性检查）
	LeaderCommit int		// leader 已提交的最高的日志条目索引，更新follower的commitIndex
	Entries      []Entry	// 需要复制的日志条目，心跳时为空
}
func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}",
					request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm,
					request.LeaderCommit, request.Entries)
}

// leader复制日志和心跳RPC的响应参数
type AppendEntriesResponse struct {
	Term          int       // 接收的 follower 任期号，用于 leader 更新自己的任期号
	Success       bool      // 是否复制成功，true 为成功
	ConflictIndex int       // 冲突index
	ConflictTerm  int       // 冲突任期
}
func (response AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,ConflictIndex:%v,ConflictTerm:%v}",
				response.Term, response.Success, response.ConflictIndex, response.ConflictTerm)
}

// leader发送快照数据RPC的请求参数
type InstallSnapshotRequest struct {
	Term              int		// leader 任期号
	LeaderId          int		// leader 节点 ID（用于 Follower 重定向客户端）
	LastIncludedIndex int		// 快照中包含的最后一条日志条目的索引
	LastIncludedTerm  int		// 快照中包含的最后一条日志条目的任期
	Data              []byte	// 快照数据
}

func (request InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,DataSize:%v}",
			request.Term, request.LeaderId, request.LastIncludedIndex, request.LastIncludedTerm, len(request.Data))
}

// leader发送快照数据RPC的响应参数
type InstallSnapshotResponse struct {
	Term int	// 接收的 follower 任期号，用于 leader 更新自己的任期号
}

func (response InstallSnapshotResponse) String() string {
	return fmt.Sprintf("{Term:%v}", response.Term)
}
