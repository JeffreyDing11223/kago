package kago

import (
	"github.com/Shopify/sarama"
)

type AsyncProducer struct {
	producer        sarama.AsyncProducer
	Id              int
	ProducerGroupId string
}

func InitManualRetryAsyncProducer(addr []string, conf *Config) (*AsyncProducer, error) {

}

func InitManualRetryAsyncProducerGroup(addr []string, conf *Config, groupId string) ([]*AsyncProducer, error) {

}
