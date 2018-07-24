package kago

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
	"log"
)

type Consumer struct {
	consumer sarama_cluster.Consumer
}

type Consumer2 sarama_cluster.Consumer

var safaf Consumer
var safaf2 Consumer2

type PartitionConsumer struct {
	consumer sarama.PartitionConsumer
}

func (cs *Consumer) InitOneConsumerOfGroup(addr []string, topic string, conf *Config) (*Consumer, error) {

}

func (cs *Consumer) InitConsumersOfGroup(addr []string, topic string, conf *Config) ([]*Consumer, error) {

}

func (cs *Consumer) Close() error {

}

func (cs *Consumer) Recv() <-chan *ConsumerMessage {

}

func (cs *Consumer) Notifications() <-chan *NotifyMessage {

}

func (cs *Consumer) Errors() <-chan error {

}

func (cs *Consumer) MarkOffset(*ConsumerMessage) {

}

func (cs *Consumer) ResetOffset(*ConsumerMessage) {

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

func (pcs *PartitionConsumer) InitPartitionConsumer(addr []string, topic string, partition int, conf *Config) (*PartitionConsumer, error) {

}
