package worker

import (
	"time"
	"context"
	"strconv"
	"KafkaScale/model"
	"KafkaScale/metric"
	"github.com/lexkong/log"
)

var AllTasks map[string]Worker

func init() {
	AllTasks = make(map[string]Worker)
}

type Worker struct {
	taskName          string
	inputBrokers      []string
	inputTopics       []string
	outputBrokers     []string
	outputTopic       string
	factor            int
	numProduceSucceed int64
	numProduceFailed  int64
	numConsume        int64
	duration          time.Duration
	start             time.Time
	timestamp         time.Time
	kafkaLogPool      chan model.KafkaLog
	maxProducerNum    int
	// consumer context to control when to stop the consumer
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	// same as above
	producerCtx           context.Context
	producerCancel        context.CancelFunc
	numProduceSuccessChan chan int64
	numProduceFailedChan  chan int64
	numConsumeChan        chan int64
	metricSystem          *metric.MetricSystem
	workerAddress         string
}

func NewWorker(
	name string,
	inputBrokers []string,
	inputTopics []string,
	outputBrokers []string,
	outputTopic string,
	factor int,
	duration time.Duration,
	ms *metric.MetricSystem,
	workerAddress string) *Worker {
	w := &Worker{
		taskName:      name,
		inputBrokers:  inputBrokers,
		inputTopics:   inputTopics,
		outputBrokers: outputBrokers,
		outputTopic:   outputTopic,
		factor:        factor,
		duration:      duration,
		metricSystem:  ms,
		workerAddress: workerAddress,
	}
	// todo find reasonable count of producer.
	w.maxProducerNum = int(factor/10) + 1
	w.start = time.Now()
	w.timestamp = time.Now()
	w.kafkaLogPool = make(chan model.KafkaLog, 100000)
	w.consumerCtx, w.consumerCancel = context.WithCancel(context.Background())
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.numProduceSuccessChan = make(chan int64, 1000)
	w.numProduceFailedChan = make(chan int64, 1000)
	w.numConsumeChan = make(chan int64, 1000)
	w.metricSystem.InitTaskMetricIfNotExist(name)
	return w
}

func (w Worker) Fire() {
	log.Infof("worker for task -> %s started", w.taskName)
	go w.fireProducer()
	go w.fireConsumer()
	go w.fireEndTimer()
	go w.fireMetricReporter()
	AllTasks[w.taskName] = w
}

func (w Worker) fireConsumer() {
	log.Infof("%d consumers for task -> %s started", 1, w.taskName)
	c := NewKafkaScaleConsumer(
		w.taskName,
		w.taskName+"-consumer-"+"0",
		w.inputBrokers,
		w.inputTopics,
		w.kafkaLogPool,
		w.consumerCtx,
		w.numConsumeChan,
		w.factor,
	)
	go c.Consume()
}

func (w Worker) fireProducer() {
	log.Infof("%d producers for task -> %s started", w.maxProducerNum, w.taskName)
	for i := 0; i < w.maxProducerNum; i++ {
		p := NewKafkaScaleProducer(
			w.taskName,
			w.taskName+"-producer-"+strconv.Itoa(i),
			w.outputBrokers,
			w.outputTopic,
			w.kafkaLogPool,
			w.producerCtx,
			w.factor,
			w.numProduceSuccessChan,
			w.numProduceFailedChan,
		)
		go p.Produce()
	}
}

func (w Worker) fireEndTimer() {
	log.Infof("task -> %s timer fire", w.taskName)
	// Context cancel to close all the fired consumer and producer.
	go func() {
		select {
		case <-time.After(w.duration):
			w.StopAndClean()

		}
	}()
}

func (w Worker) StopAndClean() {
	if _, ok := AllTasks[w.taskName]; !ok {
		log.Infof("Already stop by kill operation or times up")
		return
	}
	delete(AllTasks, w.taskName)
	log.Infof("task -> %s consumer times up", w.taskName)
	w.consumerCancel()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			if len(w.kafkaLogPool) == 0 {
				log.Infof("task -> %s queue length is 0, ready to stop all the producers", w.taskName)
				w.producerCancel()
				time.Sleep(30 * time.Second)
				log.Infof("task -> %s ready to close the kafka log queue", w.taskName)
				close(w.kafkaLogPool)
				return
			}
		}
	}
}

func (w Worker) fireMetricReporter() {
	log.Infof("task -> %s metric reporter fire", w.taskName)
	go func() {
		for {
			select {
			case successDelta := <-w.numProduceSuccessChan:
				w.numProduceSucceed += successDelta
			case failDelta := <-w.numProduceFailedChan:
				w.numProduceFailed += failDelta
			case consumeDelta := <-w.numConsumeChan:
				w.numConsume += consumeDelta
			case <-w.producerCtx.Done():
				log.Infof("task -> %s metric receiver exit!", w.taskName)
				return
			}
		}
	}()

	// Report metric repeat.
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				tm := w.genCurrentTaskMetric()
				tm.IsRunning = true
				w.metricSystem.ReceiveTaskMetric(w.taskName, tm)

				// Fire metric to Redis
				w.metricSystem.ReportAllTaskMetricToRedis()

			case <-w.producerCtx.Done():
				tm := w.genCurrentTaskMetric()
				tm.IsRunning = false
				w.metricSystem.ReceiveTaskMetric(w.taskName, tm)

				w.metricSystem.ReportAllTaskMetricToRedis()

				ticker.Stop()
				log.Infof("task -> %s metric reporter exit!", w.taskName)
				return
			}
		}
	}()

}

func (w Worker) genCurrentTaskMetric() model.TaskMetric {
	timePass := time.Now().Sub(w.start)
	outSpeed := strconv.FormatFloat(float64(w.numProduceSucceed)/timePass.Seconds(), 'f', 2, 64) + " r/s"
	inSpeed := strconv.FormatFloat(float64(w.numConsume)/timePass.Seconds(), 'f', 2, 64) + " r/s"
	return model.NewTaskMetric(
		w.taskName,
		time.Now(),
		w.start,
		w.numConsume,
		w.numProduceSucceed,
		w.numProduceFailed,
		w.workerAddress,
		time.Now().Sub(w.start).String(),
		len(w.kafkaLogPool),
		inSpeed,
		outSpeed,
	)
}

func (w Worker) GetTaskName() string {
	return w.taskName
}

