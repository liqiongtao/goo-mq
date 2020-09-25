package goo_mq

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/liqiongtao/goo"
	"time"
)

type KafkaProducer struct {
	*Kafka
	producer sarama.AsyncProducer
}

func (*KafkaProducer) config() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 5 * time.Second
	config.Version = sarama.V0_10_2_0
	return config
}

func (p *KafkaProducer) Init() {
	producer, err := sarama.NewAsyncProducer(p.Addrs, p.config())
	if err != nil {
		goo.Log.Error("[kafka-producer]", err.Error())
		panic(err.Error())
	}

	go func() {
		for {
			select {
			case suc := <-producer.Successes():
				goo.Log.Debug("[kafka-producer]",
					fmt.Sprintf("partitions=%d topic=%s offset=%d value=%s",
						suc.Partition, suc.Topic, suc.Offset, suc.Value))

			case err := <-producer.Errors():
				goo.Log.Error("[kafka-producer]", err.Error())

			case <-p.Context.Done():
				return
			}
		}
	}()

	p.producer = producer
}

func (p *KafkaProducer) SendMessage(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", time.Now().UnixNano())),
	}

	p.producer.Input() <- msg

	return nil
}
