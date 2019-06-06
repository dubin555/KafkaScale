package handler

import "KafkaScale/metric"

var MS *metric.MetricSystem

func init() {
	MS = metric.NewMetricSystem()
}