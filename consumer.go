package kago

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
	"log"
)

type Consumer struct {
	consumer sarama_cluster.Consumer
}

type PartitionConsumer struct {
	consumer sarama.PartitionConsumer
}

func (cs *Consumer) InitOneConsumerOfGroup(addr []string, topic string, groupId string, conf *Config) (*Consumer, error) {

}

func (cs *Consumer) InitConsumersOfGroup(addr []string, topic string, groupId string, conf *Config) ([]*Consumer, error) {

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
func (pcs *PartitionConsumer) InitPartitionConsumer(addr []string, topic string, partition int32, groupId string, conf *Config) (*PartitionConsumer, sarama.Consumer, error) {
	client, err := sarama.NewClient(addr, &conf.Config.Config)
	if err != nil {
		log.Println("client create error")
		return nil, nil, err
	}
	defer client.Close()
	c, err := sarama.NewConsumer(addr, &conf.Config.Config)
	if err != nil {
		log.Println("consumer create error")
		return nil, nil, err
	}
	if conf.OffsetLocalOrServer != 1 {
		offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, client)
		if err != nil {
			log.Println("offsetManager create error")
			return nil, c, err
		}
		defer offsetManager.Close()
		partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
		if err != nil {
			log.Println("partitionOffsetManager create error")
			return nil, c, err
		}
		defer partitionOffsetManager.Close()
		nextOffset, _ := partitionOffsetManager.NextOffset()

		//file

		partitionConsumer, err := c.ConsumePartition(topic, partition, nextOffset)
		if err != nil {
			log.Println("partitionConsumer create error")
			return nil, c, err
		}
		var pcs = PartitionConsumer{
			consumer: partitionConsumer,
		}
		return &pcs, c, nil

	} else {
		partitionConsumer, err := c.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Println("partitionConsumer create error")
			return nil, c, err
		}
		var pcs = PartitionConsumer{
			consumer: partitionConsumer,
		}
		return &pcs, c, nil
	}
	return nil, c, nil
}

func (pcs *PartitionConsumer) InitPartitionConsumers(addr []string, topic string, partition int, groupId string, conf *Config) ([]*PartitionConsumer, error) {

}

func (pcs *PartitionConsumer) Recv() <-chan *ConsumerMessage {

}

func (pcs *PartitionConsumer) Errors() <-chan *ConsumerError {

}

func (pcs *PartitionConsumer) Close() error {
	pcs.consumer.AsyncClose()
}
