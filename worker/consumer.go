package worker

import (
	"KafkaScale/model"
	"github.com/bsm/sarama-cluster"
	"github.com/lexkong/log"
	"context"
	"time"
	"KafkaScale/util"
)

type Consumer interface {
	Consume()
}

type KafkaScaleConsumer struct {
	taskName       string
	name           string
	brokers        []string
	topic          []string
	groupId        string
	res            chan<- model.KafkaLog
	ctx            context.Context
	factor         int
	numConsume     int64
	numConsumeChan chan<- int64
}

func NewKafkaScaleConsumer(
	taskName string,
	name string,
	brokers []string,
	topic []string,
	res chan<- model.KafkaLog,
	ctx context.Context,
	numConsumeChan chan<- int64,
	factor int) *KafkaScaleConsumer {
	consumer := &KafkaScaleConsumer{
		taskName:       taskName,
		name:           name,
		brokers:        brokers,
		topic:          topic,
		res:            res,
		ctx:            ctx,
		groupId:        "KafkaScale_" + util.GenUUID(),
		numConsumeChan: numConsumeChan,
		factor:         factor,
	}
	return consumer
}

func (c KafkaScaleConsumer) Consume() {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(c.brokers, c.groupId, c.topic, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	go func() {
		for err := range consumer.Errors() {
			log.Infof("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Infof("Rebalanced: %+v\n", ntf)
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				c.numConsumeChan <- c.numConsume
				c.numConsume = 0
			case <-c.ctx.Done():
				log.Infof("task: %s, consumer: %s, ticker exit!", c.taskName, c.name)
				return
			}
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				consumer.MarkOffset(msg, "") // mark message as processed
				for i := 1; i <= c.factor; i++ {
					c.res <- model.KafkaLog{Topic: msg.Topic, Partition: msg.Partition, Content: msg.Value}
				}
				c.numConsume += 1
			}
		case <-c.ctx.Done():
			log.Infof("task: %s, consumer: %s, consumer exit!", c.taskName, c.name)
			return
		}
	}

}
