package raft

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DebugCM 是否要输出debug
const DebugCM = 1

// LogEntry 代表log条目
// 客户端的任何 command 都会被存到 raft 节点的 logEntry 里面
// 还可以使用数据库持久化
//--------------------log entry------------------------//
//    0      1     2    3      4    5    log index
//--------------------------------------
// |  1  |  1  |  1  |  2  |  3  |  3 |  term index
// |x<-3 | y<-1| y<-9| x<-2| x<-0|x<-7|  key-value
//---------------------------------------------------//
type LogEntry struct {
	Command interface{}
	Term    int
}

// CommitEntry 返回给 客户端的 commit channel 的消息体
type CommitEntry struct {
	Command interface{} // 已经commited的command
	Index   int         // 已经commited的command 的log index
	Term    int         // term啦
}

// CMState 代表当前raft节点的状态
// 包括 leader / follower / candidate / Dead
type CMState int

// 状态常量
const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

// ConsensusModule 一致性模块，Raft算法实现
// 代表一个raft节点
type ConsensusModule struct {
	// 同步锁
	mu sync.Mutex

	// 节点的id
	id int

	// 集群中peers的id
	// peers代表集群中的其他 raft 副本
	peerIDs []int

	// 服务
	// 一致性模块需要通过server发起和接收RPC调用
	server *Server

	storage Storage // 保存

	commitChan         chan<- CommitEntry
	newCommitReadyChan chan struct{}
	triggerAEChan      chan struct{}

	// 这些参数都需要持久化到所有的server
	// term类似于选举任期，每次选举都会生成一个新的term
	currentTerm int        // 当前term
	votedFor    int        // 投票给谁
	log         []LogEntry // log条目

	state              CMState   // 节点状态
	electionResetEvent time.Time // election 重置

	// log entry index
	nextIndex  map[int]int // 对于每一个server，需要发送给他的下一个日志条目的索引值
	matchIndex map[int]int // 对于每一个server，已经复制给他的日志的最高索引值

	commitIndex int // commit 位置

	lastApplied int
}

func NewConsensusModule(
	id int,
	peerIDs []int,
	server *Server,
	storage Storage,
	ready <-chan interface{},
	commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIDs = peerIDs
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower // 默认是follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	
	// storage 有数据， 从 storage里面重新加载
	if cm.storage.HasData() {
		
	}

}

// Submit 节点接收client的command
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlog("Submit received by %v: %v", cm.state, command)
	// 将客户端的command加入log里
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{
			Command: command,
			Term:    cm.currentTerm,
		})
		cm.dlog("... log=%v", cm.log)
		return true
	}
	return false
}

// electionTimeout 代表选举时间
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 压测
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		// 论文里推荐的时间为 150~300 milliseconds
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}

}

// runElectionTimer 运行选举时间
// election timer
// follower会一直在election timer时间内运行
// 收到leader的心跳包之后重新计时
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()

	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// 控制每 10ms 发生一次循环
	// 每 10ms 运行一次 election timer
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// 循环执行 election timer
	for {
		<-ticker.C

		cm.mu.Lock()
		// 如果节点状态不是 candidate 或者 follower，退出循环
		// 因为leader定期发送心跳给follower，follower不断循环election timer
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state = %s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		// 如果当前term不匹配，退出
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 如果follower接收不到leader的心跳，就开始选举
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			// 开始选举
			cm.startElection()
			cm.mu.Unlock()
			return
		}

		cm.mu.Unlock()

	}

}

//RequestVoteArgs 投票选项
type RequestVoteArgs struct {
	Term         int // term
	CandidateId  int // 候选人id
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // term
	VoteGranted bool // 投票与否
}

// startElection 选举过程做了三件事，
// 1. 选择那些成为候选人的节点，然后新增一个term
// 2. 发送PRC给所有的副本，让他们投票
// 3. 等待投票结果，选择leader
func (cm *ConsensusModule) startElection() {
	// 在选举开始
	// 每个节点都是候选人
	cm.state = Candidate

	//term +1
	cm.currentTerm += 1

	// 保存当前term，便于比较
	savedCurrentTerm := cm.currentTerm

	// 选举重置事件
	cm.electionResetEvent = time.Now()

	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// 先给自己投一票
	var votesReceived int32 = 1

	// 然后给其他peer发送消息，选举开始啦！
	for _, peerID := range cm.peerIDs {
		// go current 保证同时发送
		go func(peerID int) {

			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				// 当前所处 term
				Term: savedCurrentTerm,
				// 都请求别人投自己一票
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerID, args)
			// 发送 rpc
			if err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				// 如果在等待投票的过程不是候选人了
				// 为啥呢，因为rpc也是要时间的，在等待收到投票的时候
				// 其他节点可能已经被选为leader了
				// 退出
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}
				// 如果返回的term大于当前term
				// 说明投票都结束了！有leader产生了
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					// 当前节点就变为follower
					cm.becomeFollower(reply.Term)
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						// 收到的投票数
						votes := int(atomic.AddInt32(&votesReceived, 1))
						// 如果多数同意，就成为leader
						if votes*2 > len(cm.peerIDs)+1 {
							cm.dlog("wins election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}

			}

		}(peerID)

	}

	// 开始新的election timer，如果当前选举其他节点成为了leader
	go cm.runElectionTimer()
}

// startLeader leader会不断发送心跳包给follower
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	// 同步发送给所有peer
	go func() {
		// 每50ms发送一次心跳
		ticker := time.NewTicker(50 * time.Microsecond)
		defer ticker.Stop()

		// 不断发送心跳
		for {
			// 发送心跳
			cm.leaderSendHeartbeats()

			<-ticker.C

			cm.mu.Lock()
			// 判断还是不是leader
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// AppendEntriesArgs 代表 AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // leader term
	LeaderID     int        // leader id
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // prev log term
	Entries      []LogEntry // 新增的 log entry （如果为空代表 heartBeats）
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

// AppendEntriesReply AE RPC返回
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// leaderSendHeartbeats leader 发送心跳包
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	// 两步：
	// 1. leader 发送新的 log entry 给 follower
	// 2. leader 发送 新的 commit index 给 follower，以让其返回给client
	for _, peerID := range cm.peerIDs {

		// rpc 调用
		go func(peerID int) {

			cm.mu.Lock()
			ni := cm.nextIndex[peerID]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderID:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerID, 0, args)
			var reply AppendEntriesReply
			// 第一步：开始rpc调用，向 每个 peer发送 log entry
			if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 如果收到的term大，成为follower
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
				// term 相同发送心跳和 log
				if cm.state == Leader && reply.Term == savedCurrentTerm {
					// 先确认当前发送的peer收到了
					if reply.Success {
						// 更新nextIndex 和 matchIndex
						cm.nextIndex[peerID] = ni + len(entries)
						cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerID, cm.nextIndex, cm.matchIndex)

						// 统计收到 log entry 的 peer，如果大多数收到了，设置 commit index
						// 依次对 log entry 里的每一个 log 进行确认，如果这个log大多数都收到了，commit index 前移
						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								// 统计
								for _, peerID := range cm.peerIDs {
									if cm.matchIndex[peerID] >= i {
										matchCount++
									}
								}
								// 大多数 match
								if matchCount*2 > len(cm.peerIDs)+1 {
									cm.commitIndex = i
								}

							}
						}
						// 根据新旧 commit 位置，设置新的commit index
						// 提交到新的 commit ready chan
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)

							// 将新的 commited 的 log 发动到chan
							// 然后 client 从 chan 收到 log commited 的消息
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						// 这个peer没有收到心跳
						cm.nextIndex[peerID] = ni - 1
						cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerID, ni-1)
					}
				}
			}
		}(peerID)

	}
}

// commitChanSender 用来将最新的 commited log entry 发送给客户端
// 它会从 newCommitReadyChan 找到最新的entry 然后 sending
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()

		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry

		// 如果当前的commit index 大，更新 lastApplied
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

//becomeFollower 如果收到的term比自己的大，成为follower
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

//----------------------PRC server 方法-------------------------//

// RequestVote 节点请求投票的RPC调用
// cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply)
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()

	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	// term 大， follower
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 投票
	// 选举限制：
	// 只有那些包含了之前所有 log entry 的 （通过 lastIndex和lastTerm控制）节点才有可能成为leader
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		// 投了
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

//AppendEntries (RPC) leader 发送心跳和 log entry 给follower
// leader 发送 log entry 给 follower的时候：
// 1. follower 添加到自己的 log entry 里，然后返回 Succee=true
// 2. leader更新自己的match index，以确认当前发送给follower的log的位置，
// 当大多数follower收到log了（也就是next index 的记录和match的相同）
// leader更新自己的 commit index， 然后发送给follower（在下一次AE RPC）
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {

	// 这里 cm 就是 follower 的角色

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)

	// term 大， follower
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// 如果不是follower，成为follower
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// follower 更新 log

		// 如果收到的 prev log index符合
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {

			// 收到心跳包啦！
			reply.Success = true

			// 确定当前log插入的位置
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// 更新插入位置
			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				// 插入新的 log
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			// 更新 commit index
			// leader 的 commit index 更大就更新
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				// 更新到 commit channel
				// 这样客户端就能接收到啦
				cm.newCommitReadyChan <- struct{}{}
			}

		}

	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil

}
