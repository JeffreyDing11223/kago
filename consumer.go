package kago

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
	"log"
)

type Consumer struct {
	consumer sarama_cluster.Consumer
	Topic    string
	GroupId  string
}

type PartitionConsumer struct {
	consumer       sarama.PartitionConsumer
	parentConsumer sarama.Consumer
	Topic          string
	Partition      int32
	GroupId        string
}

func InitOneConsumerOfGroup(addr []string, topic string, groupId string, conf *Config) (*Consumer, error) {

}

func InitConsumersOfGroup(addr []string, topic string, groupId string, conf *Config) ([]*Consumer, error) {

}

func (cs *Consumer) Close() error {
	cs.consumer.Close()
}

func (cs *Consumer) Recv() <-chan *ConsumerMessage {
	return cs.consumer.Messages()
}

func (cs *Consumer) Notifications() <-chan *NotifyMessage {
	return cs.consumer.Notifications()
}

func (cs *Consumer) Errors() <-chan error {
	return cs.consumer.Errors()
}

func (cs *Consumer) MarkOffset(topic string, partition int32, offset int64, groupId string, ifExactOnce bool) {
	//file
	cs.consumer.MarkPartitionOffset(topic, partition, offset, "")
}

func (cs *Consumer) ResetOffset(topic string, partition int32, offset int64, groupId string, ifExactOnce bool) {
	//file
	cs.consumer.ResetPartitionOffset(topic, partition, offset, "")
}

func (cs *Consumer) CommitOffsets() error {
	err := cs.consumer.CommitOffsets()
	if err != nil {
		return err
	} else {
		return nil
	}
}

//issue github.com/Shopify/sarama/issues/1130 has been recovered
func Topics(addr []string, conf *Config) ([]string, error) {
	client, err := sarama.NewClient(addr, &conf.Config.Config)
	if err != nil {
		log.Print("get topics error", err)
		return []string{}, err
	}
	defer client.Close()
	return client.Topics()
}

func Partitions(addr []string, topic string, conf *Config) ([]int32, error) {
	client, err := sarama.NewClient(addr, &conf.Config.Config)
	if err != nil {
		log.Print("get partitions error", err)
		return []int32{}, err
	}
	defer client.Close()
	return client.Partitions(topic)

}

// don't forget to close sarama.Consumer,then close partitionConsumer
func InitPartitionConsumer(addr []string, topic string, partition int32, groupId string, conf *Config) (*PartitionConsumer, error) {
	client, err := sarama.NewClient(addr, &conf.Config.Config)
	if err != nil {
		log.Println("client create error")
		return nil, err
	}
	defer client.Close()
	c, err := sarama.NewConsumer(addr, &conf.Config.Config)
	if err != nil {
		log.Println("consumer create error")
		return nil, err
	}
	if conf.OffsetLocalOrServer != 1 {
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
		defer partitionOffsetManager.Close()
		nextOffset, _ := partitionOffsetManager.NextOffset()

		//file

		partitionConsumer, err := c.ConsumePartition(topic, partition, nextOffset)
		if err != nil {
			log.Println("partitionConsumer create error")
			return nil, err
		}
		var pcs = PartitionConsumer{
			consumer:       partitionConsumer,
			parentConsumer: c,
			Topic:          topic,
			Partition:      partition,
			GroupId:        groupId,
		}
		return &pcs, nil

	} else {
		partitionConsumer, err := c.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Println("partitionConsumer create error")
			return nil, err
		}
		var pcs = PartitionConsumer{
			consumer:       partitionConsumer,
			parentConsumer: c,
			Topic:          topic,
			Partition:      partition,
			GroupId:        groupId,
		}
		return &pcs, nil
	}
	return nil, nil
}

func InitPartitionConsumers(addr []string, topic string, groupId string, conf *Config) ([]*PartitionConsumer, error) {

}

func (pcs *PartitionConsumer) Recv() <-chan *ConsumerMessage {
	return pcs.consumer.Messages()
}

func (pcs *PartitionConsumer) Errors() <-chan *ConsumerError {
	return pcs.consumer.Errors()
}

func (pcs *PartitionConsumer) Close() error {
	pcs.consumer.AsyncClose()
	err := pcs.parentConsumer.Close()
	return err
}
