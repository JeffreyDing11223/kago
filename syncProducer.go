package kago

import (
	"github.com/Shopify/sarama"

	"log"
)

type SyncProducer struct {
	producer        sarama.SyncProducer
	Id              int
	ProducerGroupId string
}

// one syncProducer without retry
func InitManualRetrySyncProducer(addr []string, conf *Config) (*SyncProducer, error) {
	conf.Config.Producer.Retry.Max = 0
	syncProducer := &SyncProducer{
		Id:              0,
		ProducerGroupId: "",
	}
	var err error
	syncProducer.producer, err = sarama.NewSyncProducer(addr, &conf.Config.Config)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	return syncProducer, nil
}

//some(config.SyncProducerAmount) syncProducer without retry
func InitManualRetrySyncProducerGroup(addr []string, conf *Config, groupId string) ([]*SyncProducer, error) {
	conf.Config.Producer.Retry.Max = 0
	producerAmount := conf.SyncProducerAmount
	if producerAmount < 1 {
		producerAmount = 1
	}
	var producerSli []*SyncProducer
	for i := 0; i < producerAmount; i++ {
		syncProducer := &SyncProducer{
			Id:              i,
			ProducerGroupId: groupId,
		}
		var err error
		syncProducer.producer, err = sarama.NewSyncProducer(addr, &conf.Config.Config)
		if err != nil {
			log.Print(err)
		} else {
			producerSli = append(producerSli, syncProducer)
		}
	}
	return producerSli, nil
}

func (sp *SyncProducer) SendMessage(msg *ProducerMessage) (string, int32, int64, error) {
	_, _, err := sp.producer.SendMessage(msg)
	return msg.Topic, msg.Partition, msg.Offset, err
}

func (sp *SyncProducer) Close() (err error) {
	err = sp.producer.Close()
	return
}
