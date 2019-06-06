package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/lexkong/log"
	"KafkaScale/router"
	"KafkaScale/config"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"strconv"
	"os"
	"os/signal"
	"KafkaScale/worker"
	"KafkaScale/util"
	"time"
	"KafkaScale/service"
)

var (
	cfg        = pflag.StringP("env", "e", "", "env: local or lab or nothing.")
	port       = pflag.IntP("port", "p", 8093, "port to bind")
	deployMode = pflag.StringP("deploy", "d", "local", "mode to run: local or cluster")
)

func main() {

	pflag.Parse()

	if err := config.Init(*cfg); err != nil {
		panic(err)
	}

	gin.SetMode(viper.GetString("runmode"))

	g := gin.New()

	middlewares := []gin.HandlerFunc{}

	router.Load(g, middlewares...)

	address := ":" + strconv.Itoa(*port)
	viper.Set("port", address)
	viper.Set("deploy", deployMode)

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	log.Infof("Start to Listen and serve : %s", address)
	defer log.Infof("Stop to Listen and serve : %s", address)
	go g.Run(address)

	if !util.IsDeployLocal() {
		// After 10 seconds, register to the zookeeper
		timeToWarmUp := time.After(time.Second * 10)
		<-timeToWarmUp
		go service.Register(viper.GetStringSlice("zk.addr"), viper.GetString("zk.path"), util.GetLocalAddress())
	}

	<-quit
	log.Infof("Receive interrupt signal")

	for _, w := range worker.AllTasks {
		log.Infof("Clean task -> %s resources", w.GetTaskName())
		w.StopAndClean()
	}

}

func recoverOverload() {
	if r := recover(); r != nil {
		fmt.Println("recovered from this system")
	}
}
