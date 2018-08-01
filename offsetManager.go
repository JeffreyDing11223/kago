package kago

import (
	"encoding/json"
	"github.com/Shopify/sarama"

	"io/ioutil"
	"log"
	"os"
)

type PartitionOffsetManager struct {
	client sarama.Client
	om     sarama.OffsetManager
	pom    sarama.PartitionOffsetManager
}

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

func (pom *PartitionOffsetManager) MarkOffset(topic string, partition int32, offset int64, groupId string, ifExactOnce bool) {
	if ifExactOnce {
		fileOffset(topic, partition, offset, groupId)
	}
	pom.pom.MarkOffset(offset, "")
}

func (pom *PartitionOffsetManager) ResetOffset(topic string, partition int32, offset int64, groupId string, ifExactOnce bool) {
	if ifExactOnce {
		fileOffset(topic, partition, offset, groupId)
	}
	pom.pom.ResetOffset(offset, "")
}

func (pom *PartitionOffsetManager) NextOffset() (offset int64) {
	offset, _ = pom.pom.NextOffset()
	return offset
}

func (pom *PartitionOffsetManager) Close() (error, error) {
	pom.pom.AsyncClose()
	err := pom.om.Close()
	err2 := pom.client.Close()
	return err, err2
}

func (pom *PartitionOffsetManager) Errors() <-chan *ConsumerError {
	return pom.pom.Errors()
}

func InitPartitionOffsetManager(addr []string, topic, groupId string, partition int32, conf *Config) (*PartitionOffsetManager, error) {
	client, err := sarama.NewClient(addr, &conf.Config.Config)
	if err != nil {
		log.Println("client create error")
		return nil, err
	}
	defer client.Close()

	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, client)
	if err != nil {
		log.Println("offsetManager create error")
		return nil, err
	}
	defer offsetManager.Close()

	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		log.Println("partitionOffsetManager create error")
		return nil, err
	}
	var pom = PartitionOffsetManager{
		client: client,
		om:     offsetManager,
		pom:    partitionOffsetManager,
	}
	return &pom, nil
}
