package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

// 任务描述
type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

// A laziest, worker-stateless, channel-based implementation of Coordinator
type Coordinator struct {
	files   []string		// 待处理的输入文件列表
	nReduce int
	nMap    int
	phase   SchedulePhase	// 当前任务调度的阶段
	tasks   []Task			// 当前阶段的所有任务列表
	
	heartbeatCh chan heartbeatMsg	// 接收来自 worker 的心跳消息
	reportCh    chan reportMsg		// 接收 worker 的任务完成报告
	doneCh      chan struct{}		// 通知 Coordinator 结束整个调度过程
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg	// 将消息发送到心跳通道heartbeatCh
	<-msg.ok				// 阻塞等待处理完成的信号
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) {
				switch c.phase {
				case MapPhase:
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase:
					log.Printf("Coordinator: %v finished, Congratulations \n", ReducePhase)
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				case CompletePhase:
					panic("Coordinator: enter unexpected branch")
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			// 确保报告的任务阶段与Coordinator当前阶段一致
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

// 从任务池中选择一个合适的任务分配给工作节点，所有任务已完成返回true
func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewJob := true, false
	for id, task := range c.tasks {
		switch task.status {
		case Idle:	// 空闲任务：分配为working，填充响应，停止遍历（hasNewJob）
			allFinished, hasNewJob = false, true
			c.tasks[id].status, c.tasks[id].startTime = Working, time.Now()
			response.NReduce, response.Id = c.nReduce, id
			if c.phase == MapPhase {
				response.JobType, response.FilePath = MapJob, c.files[id]
			} else {
				response.JobType, response.NMap = ReduceJob, c.nMap
			}
		case Working:	// 运行中任务：检测超时，超时重新分配任务退出遍历
			allFinished = false
			if time.Since(task.startTime) > MaxTaskRunInterval {
				hasNewJob = true
				c.tasks[id].startTime = time.Now()
				response.NReduce, response.Id = c.nReduce, id
				if c.phase == MapPhase {
					response.JobType, response.FilePath = MapJob, c.files[id]
				} else {
					response.JobType, response.NMap = ReduceJob, c.nMap
				}
			}
		case Finished:	// 已完成任务：继续遍历检查下一任务
		}
		if hasNewJob {
			break
		}
	}

	// 无任务可分配
	if !hasNewJob {
		response.JobType = WaitJob
	}
	return allFinished
}

// 初始化 Map 任务阶段，填充任务描述
func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName: file,
			id:       index,
			status:   Idle,
		}
	}
}

// 初始化 Reduce 任务阶段，填充任务描述
func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

// 初始化 Complete 任务阶段，通知调度任务已完成
func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// 注册 Coordinator 的 RPC 方法
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	name := coordinatorSock()
	os.Remove(name)
	// 用 HTTP 协议监听 Unix socket
	l, e := net.Listen("unix", name)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 启动后台线程，等待 Worker 发起的 RPC 请求
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	<-c.doneCh
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}, 1),
	}
	c.server()
	go c.schedule()
	return &c
}
