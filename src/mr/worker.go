package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

// 执行Map任务，将输入文件转换为中间键值对
func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	fileName := response.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapF(fileName, string(content))

	// 分区，作用类似于顺序结构的排序
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := iHash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}

	// 并发写入中间文件
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		// 不同index代表不同分区，intermediate代表分区内的所有键值对
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.Id, index)	// mr-mapId-reduceId
			// 将键值对编码为JSON格式，每行一个记录
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			atomicWriteFile(intermediateFilePath, &buf)
		}(index, intermediate)
	}
	wg.Wait()
	doReport(response.Id, MapPhase)
}

// 执行Reduce任务，处理Map阶段生成的中间文件，产生最终输出
func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {

	// 遍历当前Reduce任务分区的中间文件，将键值对解码到kva数组
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		filePath := generateMapResultFileName(i, response.Id)	// mr-mapId-reduceId
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 按key分组到result中
	results := make(map[string][]string)
	// Maybe we need merge sort for larger data
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}

	// 执行Reduce函数，将结果写入缓冲区
	var buf bytes.Buffer
	for key, values := range results {
		output := reduceF(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}

	// 写入最终结果文件
	atomicWriteFile(generateReduceResultFileName(response.Id), &buf)
	doReport(response.Id, ReducePhase)
}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func call(rpcName string, args interface{}, reply interface{}) bool {
	// 建立RPC客户端连接
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// 发起 RPC 调用
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
