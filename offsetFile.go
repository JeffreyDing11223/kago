package kago

import (
	"encoding/json"
	"io/ioutil"
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

//init fileMap
func InitOffsetFile() {
	cfgs, err := ListDir("./offsetCfg", "cfg")
	if err != nil {
		log.Println("read offset cfg error:", err)
		return
	}
	for _, cfg := range cfgs {
		fi, err := os.OpenFile("./offsetCfg/"+cfg, os.O_RDWR, 0)
		if err != nil {
			log.Println("read "+cfg+" error:", err)
			continue
		}
		topic := cfg[:len(cfg)-4]
		_, result := setTopicFile(topic, fi)
		if result == false {
			log.Println("set file error, topic:", topic)
			continue
		}
	}

}

//read file and write offset into it
func fileOffset(topic string, partition int32, offset int64, groupId string) {

	offsetFi, exist := getTopicFile(topic)
	if exist == false {
		var newOffsetFi *offsetFile
		fi, err := os.Create("./offsetCfg/" + topic + ".cfg")
		if err != nil {
			log.Println("create file error:", err)
		} else {
			var result bool
			newOffsetFi, result = setTopicFile(topic, fi)
			if result == false {
				log.Println("set file error")
				newOffsetFi = new(offsetFile)
			}
		}
		offsetFi = newOffsetFi
	}

	//file
	offsetFi.Lock()
	offsetFi.file.Seek(0, 0)
	content, _ := ioutil.ReadAll(offsetFi.file)
	var cfgEntity = cfgObj{}
	err := json.Unmarshal(content, &cfgEntity)
	if err != nil {
		log.Println("cfg json.Unmarshal error", err.Error())
	}
	var flag bool
	for i, value := range cfgEntity.Data {
		if value.Partition == partition && value.GroupId == groupId {
			cfgEntity.Data[i].Offset = offset
			flag = true
		}
	}
	if flag == false {
		var offsetEntity = offsetObj{
			GroupId:   groupId,
			Partition: partition,
			Offset:    offset,
		}
		cfgEntity.Data = append(cfgEntity.Data, offsetEntity)
	}
	content2, _ := json.Marshal(cfgEntity)
	err = offsetFi.file.Truncate(0)
	offsetFi.file.Seek(0, 0)
	_, err = offsetFi.file.Write(content2)
	offsetFi.Unlock()
	if err != nil {
		log.Println("write file error:", err, " topic:", topic)
	}
	//file
}

//read file and read the offset
func getFileOffset(topic, groupId string, partition int32) int64 {
	var content []byte
	offsetFi, exist := getTopicFile(topic)
	if exist == false {
		return -2
	}
	//file
	offsetFi.Lock()
	offsetFi.file.Seek(0, 0)
	content, _ = ioutil.ReadAll(offsetFi.file)
	offsetFi.Unlock()
	var cfgEntity = cfgObj{}
	err := json.Unmarshal(content, &cfgEntity)
	if err != nil {
		log.Println("cfg json.Unmarshal error", err.Error())
	}
	for _, value := range cfgEntity.Data {
		if value.Partition == partition && value.GroupId == groupId {
			return value.Offset
		}
	}
	return -2
}

//get file from map
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

//set file into map
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
