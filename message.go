package kago

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
)

type ProducerMessage = sarama.ProducerMessage

type ProducerError = sarama.ProducerError

type ConsumerMessage = sarama.ConsumerMessage

type NotifyMessage = sarama_cluster.Notification

type ConsumerError = sarama.ConsumerError
