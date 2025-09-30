package shardctrler

import (
	"sort"
)

type ConfigStateMachine interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

// ConfigStateMachine 的内存版本实现（本实验简化，实际是要在不同物理机存储硬盘上的）
type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

// 增加一个或多个新 group
func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	// 把要加入的 group 合并进 newConfig.Groups
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {	// 只在 gid 不存在时加入（不会覆盖已有的 group）
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	// gid->[]shard，便于按 gid 聚合 shard 列表
	s2g := Group2Shards(newConfig)

	// 负载均衡循环：每次把source（拥有最多分片的gid）的第一个shard移给target（拥有最少分片的gid），
	// 直到任意两个组的shard数差≤1，并且没有分片未分配组。
	for {
		source, target := GetGIDWithMaximumShards(s2g), GetGIDWithMinimumShards(s2g)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}

	// 把 s2g 更新到 [NShards]int
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 删除若干 group
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	s2g := Group2Shards(newConfig)	// gid->[]shard，便于按 gid 聚合 shard 列表


	orphanShards := make([]int, 0)
	for _, gid := range gids {
		// 删除gid对应的组
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// 如果 s2g 有该 gid，对应的 shards 都加入 orphanShards（待重新分配），并从 s2g 中删除该条目
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		// 对每个落单的 shard，选择当前最少 shard 的 gid 作为目标，逐个分配（保证尽量均衡）
		for _, shard := range orphanShards {
			target := GetGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}

		// 把 s2g 更新到 [NShards]int
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 强制把某个 shard 移动到指定 group
func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 查询某个版本的配置
func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}

// 把shard属于哪个gid 转换成 某个gid拥有哪些shard（包括未装载分片的组）
func Group2Shards(config Config) map[int][]int {
	s2g := make(map[int][]int)
	// 先保证 s2g 中至少有 config.Groups 里的所有 gid 键
	for gid := range config.Groups {
		s2g[gid] = make([]int, 0)
	}
	// 然后遍历 config.Shards，把每个 shard 追加到对应的 s2g[gid]
	// Groups里没有gid=0，但Shards里可能有，s2g[0]也会被创建，gid=0代表没有group管理，负载均衡时会优先分配
	for shard, gid := range config.Shards {
		s2g[gid] = append(s2g[gid], shard)
	}
	return s2g
}

// 返回拥有最少分片的组
func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// 把 s2g 的键取出并 sort.Ints(keys) 保证遍历顺序稳定
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	
	// 在遍历中选择 gid != 0 且拥有分区数量最小的 gid 返回
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			index, min = gid, len(s2g[gid])
		}
	}
	return index
}

// 返回拥有最多分片的组，优先把未分配组（gid=0）的shard分配出去
func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// 如果s2g[0]存在且非空，直接返回0（优先把未分配的分片作为source分配出去）
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}

	// 否则用排序后的keys，返回拥有最多shard的gid
	// 排序保证遍历顺序稳定，否则有相同数量shard的gid时，返回不确定
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
