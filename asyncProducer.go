package kago

import (
	"github.com/Shopify/sarama"
	"log"
)

type AsyncProducer struct {
	producer        sarama.AsyncProducer
	Id              int
	ProducerGroupId string
}

// one asyncProducer without retry
func InitManualRetryAsyncProducer(addr []string, conf *Config) (*AsyncProducer, error) {
	conf.Config.Producer.Retry.Max = 0
	aSyncProducer := &AsyncProducer{
		Id:              0,
		ProducerGroupId: "",
	}
	var err error
	aSyncProducer.producer, err = sarama.NewAsyncProducer(addr, &conf.Config.Config)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	return aSyncProducer, nil
}

// some(config.AsyncProducerAmount) asyncProducer without retry
func InitManualRetryAsyncProducerGroup(addr []string, conf *Config, groupId string) ([]*AsyncProducer, error) {
	conf.Config.Producer.Retry.Max = 0
	producerAmount := conf.AsyncProducerAmount
	if producerAmount < 1 {
		producerAmount = 1
	}
	var err2 error
	var producerSli []*AsyncProducer
	for i := 0; i < producerAmount; i++ {
		aSyncProducer := &AsyncProducer{
			Id:              i,
			ProducerGroupId: groupId,
		}
		var err error
		aSyncProducer.producer, err = sarama.NewAsyncProducer(addr, &conf.Config.Config)
		if err != nil {
			err2 = err
			log.Print(err)
		} else {
			producerSli = append(producerSli, aSyncProducer)
		}
	}
	return producerSli, err2
}

//send message
func (asp *AsyncProducer) Send() chan<- *ProducerMessage {
	return asp.producer.Input()
}

func (asp *AsyncProducer) Successes() <-chan *ProducerMessage {
	return asp.producer.Successes()
}

func (asp *AsyncProducer) Errors() <-chan *ProducerError {
	return asp.producer.Errors()
}

func (asp *AsyncProducer) Close() (err error) {
	err = asp.producer.Close()
	return
}
