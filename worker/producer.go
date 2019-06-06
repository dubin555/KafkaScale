package worker

import (
	"KafkaScale/model"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
	"context"
	"github.com/lexkong/log"
)

type Producer interface {
	Produce()
}

type KafkaScaleProducer struct {
	taskName              string
	name                  string
	brokers               []string
	topic                 string
	res                   <-chan model.KafkaLog
	ctx                   context.Context
	factor                int
	numProduceSucceed     int64
	numProduceFailed      int64
	numProduceSucceedChan chan<- int64
	numProduceFailedChan  chan<- int64
}

func NewKafkaScaleProducer(
	taskName string,
	name string,
	brokers []string,
	topic string,
	res <-chan model.KafkaLog,
	ctx context.Context,
	factor int,
	numProduceSucceedChan chan<- int64,
	numProduceFailedChan chan<- int64) *KafkaScaleProducer {
	producer := &KafkaScaleProducer{
		taskName:              taskName,
		name:                  name,
		brokers:               brokers,
		topic:                 topic,
		res:                   res,
		ctx:                   ctx,
		factor:                factor,
		numProduceSucceedChan: numProduceSucceedChan,
		numProduceFailedChan:  numProduceFailedChan,
	}
	return producer
}

func (p KafkaScaleProducer) Produce() {
	config := sarama.NewConfig()
	config.Metadata.Retry.Max = 2
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.Timeout = 500 * time.Millisecond
	//config.Producer.Flush.Frequency = 10 * time.Millisecond
	//config.Producer.Flush.Bytes = 1 << 10 //1KB
	//config.Producer.MaxMessageBytes = 2 << 20
	config.Producer.Flush.Messages = 5
	config.Producer.Flush.MaxMessages = 5
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Version = sarama.V1_1_0_0

	producer, err := sarama.NewAsyncProducer(p.brokers, config)
	if err != nil {
		panic(err)
	}
	//defer producer.Close()

	if err != nil {
		log.Info("fail to start")
	}

	//Send the metric to channel
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				p.numProduceSucceedChan <- p.numProduceSucceed
				p.numProduceSucceed = 0
				p.numProduceFailedChan <- p.numProduceFailed
				p.numProduceFailed = 0
			case <-p.ctx.Done():
				log.Infof("task: %s, producer: %s, ticker exit!", p.taskName, p.name)
				return
			}
		}
	}()

	go func() {
		for err := range producer.Errors() {
			fmt.Println(err, p.name)
			p.numProduceFailed += 1
		}
	}()

	for {
		select {
		case msg := <-p.res:
			message := &sarama.ProducerMessage{
				Topic: p.topic,
				Value: sarama.ByteEncoder(msg.Content),
			}
			producer.Input() <- message
			p.numProduceSucceed += 1
		case <-p.ctx.Done():
			log.Infof("task: %s, producer: %s, producer exit!", p.taskName, p.name)
			return
		}
	}

}
