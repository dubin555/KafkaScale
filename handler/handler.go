package handler

import (
	"github.com/gin-gonic/gin"
	"KafkaScale/model"
	"net/http"
	"KafkaScale/worker"
	"time"
	"KafkaScale/util"
	"github.com/lexkong/log"
	"encoding/json"
	"bytes"
)


func PostTask(c *gin.Context) {
	if util.IsDeployLocal() {
		PostTaskLocal(c)
	}
	PostTaskCluster(c)
}

func PostTaskCluster(c *gin.Context) {
	server := util.GetLowestLoad()
	if server == "" {
		c.JSON(http.StatusForbidden, "No server available")
		return
	}
	serverAddr := "http://" + server + "/task/local"
	log.Infof("Transfer task to the server: %s", serverAddr)

	var task model.TaskRequest
	if err := c.Bind(&task); err != nil {
		c.JSON(http.StatusForbidden, "Error format")
		return
	}
	_, err := time.ParseDuration(task.Duration)
	if err != nil {
		c.JSON(http.StatusBadRequest, "Error Duration format")
		return
	}

	jsonValue, _ := json.Marshal(task)
	resp, err := http.Post(serverAddr, "application/json", bytes.NewBuffer(jsonValue))
	c.String(http.StatusOK, "task submitted to " + serverAddr + " status -> " + resp.Status)
	return
}

func PostTaskLocal(c *gin.Context) {
	var task model.TaskRequest
	if err := c.Bind(&task); err != nil {
		c.JSON(http.StatusForbidden, "Error format")
		return
	}

	d, err := time.ParseDuration(task.Duration)
	if err != nil {
		c.JSON(http.StatusBadRequest, "Error Duration format")
		return
	}
	w := worker.NewWorker(
		task.Name,
		task.InputBrokers,
		task.InputTopics,
		task.OutputBrokers,
		task.OutputTopic,
		task.Factor,
		d,
		MS,
		util.GetLocalAddress(),
	)
	w.Fire()
	c.String(http.StatusOK, "task submitted!")
	return
}

func GetMetric(c *gin.Context) {
	taskName := c.Param("task_name")
	res := MS.ReportTaskMetric(taskName)
	c.String(http.StatusOK, res)
	return
}

func GetAllMetric(c *gin.Context) {
	res := MS.ReportAllTaskMetric()
	c.String(http.StatusOK, res)
	return
}

// @Summary Kill all the running tasks
// @Description Kill all the running tasks
// @Success 200 {string} "kill all the tasks"
// @Router /kill [get]
func Kill(c *gin.Context) {
	for _, w := range worker.AllTasks {
		log.Infof("Clean task -> %s resources", w.GetTaskName())
		w.StopAndClean()
	}
	c.String(http.StatusOK, "kill all the tasks")
	return
}

