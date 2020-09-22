# 示例

```
package main

import (
	"context"
	"fmt"
	goo_mq "github.com/liqiongtao/goo-mq"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	topic  = "test"
	topics = []string{"test"}
	addrs  = []string{"s100:9092"}

	sig         = make(chan os.Signal)
	ctx, cancel = context.WithCancel(context.Background())
)

func init() {
	goo_mq.Init(&goo_mq.Kafka{Context: ctx, Addrs: addrs})
	// goo_mq.Init(&goo_mq.KafkaProducer{})
	// goo_mq.Init(&goo_mq.KafkaConsumer{})
	// goo_mq.Init(&goo_mq.KafkaConsumerGroup{})

	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for ch := range sig {
			switch ch {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				cancel()
			default:
				continue
			}
		}
	}()
}

func main() {
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			goo_mq.SendMessage(topic, []byte(fmt.Sprintf("msg-%d", i)))
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		goo_mq.Consume(topic, func(data []byte) bool {
			return true
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		goo_mq.Consume(topic, func(data []byte) bool {
			return true
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		goo_mq.ConsumeGroup("A100", topics, func(data []byte) bool {
			return true
		})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		goo_mq.ConsumeGroup("A101", topics, func(data []byte) bool {
			return true
		})
	}()

	wg.Wait()
}
```