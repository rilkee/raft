package raft

import (
	"fmt"
	"log"
)

// lastLogIndexAndTerm 获取last log index 和 term
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}
// intMin 比较大小
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// dlog 日志输出
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}
