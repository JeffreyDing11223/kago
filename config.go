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
	OffsetLocalOrServer   int //0,local  1,server  2,newest
}

func NewConfig() (conf *Config) {
	conf = new(Config)
	conf.Config = *sarama_cluster.NewConfig()
	conf.Config.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.SyncProducerAmount = 1
	conf.AsyncProducerAmount = 1
	conf.ConsumerOfGroupAmount = 1
	conf.OffsetLocalOrServer = 1
	return
}
