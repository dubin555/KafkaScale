package metric

import (
	"KafkaScale/model"
	"time"
	"sync"
	"fmt"
	"encoding/json"
	"KafkaScale/service"
)

type MetricSystem struct {
	taskMetrics map[string]model.TaskMetric
	workerMetrics map[string]model.WorkerMetric
	mux         sync.Mutex
}

func NewMetricSystem() *MetricSystem {
	ms := &MetricSystem{}
	ms.taskMetrics = make(map[string]model.TaskMetric)
	return ms
}

func (ms *MetricSystem) InitTaskMetricIfNotExist(name string) {
	ms.taskMetrics[name] = model.TaskMetric{
		Name:      name,
		Timestamp: time.Now(),
	}
}

func (ms *MetricSystem) ReceiveTaskMetric(name string, tm model.TaskMetric) {
	ms.mux.Lock()
	defer ms.mux.Unlock()
	ms.taskMetrics[name] = tm
}

func (ms *MetricSystem) ReportTaskMetric(name string) string {
	if tm, ok := ms.taskMetrics[name]; ok {
		b, err := json.Marshal(tm)
		if err != nil {
			fmt.Println(err)
			return ""
		}
		return string(b)
	}
	return ""
}

func (ms *MetricSystem) ReportAllTaskMetric() string {
	b, err := json.Marshal(ms.taskMetrics)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(b)
}

func (ms *MetricSystem) ReportAllTaskMetricToRedis() {
	for k := range ms.taskMetrics {
		taskName := "kafkaScale:taskName:" + k
		taskMetric := ms.ReportTaskMetric(k)
		service.Set(taskName, taskMetric)
	}
}
