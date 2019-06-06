package model

import (
	"time"
)

type KafkaLog struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Content   []byte `json:"content"`
}

type TaskRequest struct {
	Name          string   `json:"name" binding:"required"`
	InputBrokers  []string `json:"input_brokers" binding:"required"`
	InputTopics   []string `json:"input_topics" binding:"required"`
	OutputBrokers []string `json:"output_brokers" binding:"required"`
	OutputTopic   string   `json:"output_topic" binding:"required"`
	Factor        int      `json:"factor" binding:"required"`
	Duration      string   `json:"duration" binding:"required"`
}

type TaskMetric struct {
	Name              string    `json:"name"`
	Timestamp         time.Time `json:"timestamp"`
	Start             time.Time `json:"start"`
	ConsumeNum        int64     `json:"consume_num"`
	ProduceSuccessNum int64     `json:"produce_success_num"`
	ProduceFailNum    int64     `json:"produce_fail_num"`
	WorkerAddress     string    `json:"worker_address"`
	Duration          string    `json:"duration"`
	Lag               int       `json:"lag"`
	InputSpeed        string    `json:"input_speed"`
	OutputSpeed       string    `json:"output_speed"`
	IsRunning         bool      `json:"is_running"`
}

type WorkerMetric struct {
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`
	TaskNum   int       `json:"task_num"`
	Lag       int       `json:"lag"`
}

func NewTaskMetric(
	name string,
	timestamp time.Time,
	start time.Time,
	consumeNum int64,
	produceSuccessNum int64,
	produceFailNum int64,
	workerAddress string,
	duration string,
	lag int,
	inputSpeed string,
	outputSpeed string,
) TaskMetric {
	return TaskMetric{
		Name:              name,
		Timestamp:         timestamp,
		Start:             start,
		ConsumeNum:        consumeNum,
		ProduceSuccessNum: produceSuccessNum,
		ProduceFailNum:    produceFailNum,
		WorkerAddress:     workerAddress,
		Duration:          duration,
		Lag:               lag,
		InputSpeed:        inputSpeed,
		OutputSpeed:       outputSpeed,
	}

}

func NewWorkerMetric(
	name string,
	timestamp time.Time,
	taskNum int,
	lag int,
) WorkerMetric {
	return WorkerMetric{
		Name:      name,
		Timestamp: timestamp,
		TaskNum:   taskNum,
		Lag:       lag,
	}
}
