package kago

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
)

type Config struct {
	sarama_cluster.Config
	SyncProducerAmount    int
	AsyncProducerAmount   int
	ConsumerOfGroupAmount int
}

func NewConfig() (conf *Config) {
	conf.Config.Config = *sarama.NewConfig()
	conf.SyncProducerAmount = 1
	conf.AsyncProducerAmount = 1
	conf.ConsumerOfGroupAmount = 1
	return
}
