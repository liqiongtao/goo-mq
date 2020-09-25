package goo_mq

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/liqiongtao/goo"
	"sync"
)

type KafkaConsumer struct {
	*Kafka
	wg sync.WaitGroup
}

func (*KafkaConsumer) config() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_10_2_0
	return config
}

func (c *KafkaConsumer) Init() {
}

func (c *KafkaConsumer) Consume(topic string, handler HandlerFunc) error {
	consumer, err := sarama.NewConsumer(c.Addrs, c.config())
	if err != nil {
		goo.Log.Error("[kafka-consumer]", err.Error())
		panic(err.Error())
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		goo.Log.Error("[kafka-consumer]", err.Error())
		return err
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			goo.Log.Error("[kafka-consumer]", err.Error())
			continue
		}

		c.wg.Add(1)
		goo.AsyncFunc(c.message(pc, handler))
	}

	c.wg.Wait()

	return nil
}

func (c *KafkaConsumer) message(pc sarama.PartitionConsumer, handler HandlerFunc) func() {
	return func() {
		defer c.wg.Done()
		defer pc.Close()

		for {
			select {
			case msg := <-pc.Messages():
				handler(msg.Value)
				goo.Log.Debug("[kafka-consume]",
					fmt.Sprintf("partitions=%d topic=%s offset=%d key=%s value=%s",
						msg.Partition, msg.Topic, msg.Offset, string(msg.Key), string(msg.Value)))

			case err := <-pc.Errors():
				goo.Log.Error("[kafka-consumer]", err.Error())

			case <-c.Context.Done():
				return
			}
		}
	}
}
