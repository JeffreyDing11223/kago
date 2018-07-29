package kago

import (
	"log"
	"os"
	"sync"
)

type offsetFile struct {
	file *os.File
	sync.Mutex
}

type offsetObj struct {
	GroupId   string `json:"group_id"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type cfgObj struct {
	Data []offsetObj `json:"data"`
}

var topicFileMap sync.Map // map[string] *offsetFile

func InitOffsetFile() {
	cfgs, err := ListDir("./offsetCfg", "cfg")
	if err != nil {
		log.Println("read offset cfg error:", err)
		return
	}
	for _, cfg := range cfgs {
		fi, err := os.Open(cfg)
		if err != nil {
			log.Println("read "+cfg+" error:", err)
			continue
		}
		topic := cfg[:len(cfg)-4]
		result := setTopicFile(topic, fi)
		if result == false {
			log.Println("set file error")
			continue
		}
	}

}

func getTopicFile(topic string) (*offsetFile, bool) {
	mapValue, ok := topicFileMap.Load(topic)
	if ok {
		fi, valid := mapValue.(*offsetFile)
		if valid {
			return fi, true
		} else {
			log.Println("invalid type assertion error:", mapValue)
			return nil, false
		}
	}
	return nil, false
}

func setTopicFile(topic string, fi *os.File) (*offsetFile, bool) {
	if topic == "" || fi == nil {
		return nil, false
	}
	var lock sync.Mutex
	var offsetFile = &offsetFile{
		fi,
		lock,
	}
	topicFileMap.Store(topic, offsetFile)
	return offsetFile, true
}
