package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

// Storage 存储raft数据
type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	HasData() bool
}

// persistToStorage 数据持久化
func (cm *ConsensusModule) persistToStorage() {
	// term
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	// vote for
	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	// log entry
	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

// restoreFromStorage 从sotage里面重新加载
func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}
