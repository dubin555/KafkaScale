package service

import (
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"github.com/lexkong/log"
	"fmt"
	"KafkaScale/model"
	"encoding/json"
)

func Set(key string, value string) {
	client := redis.NewClient(&redis.Options{
		Addr: viper.GetString("db.addr"),
	})
	defer client.Close()
	client.Do("SET", key, value)
	log.Infof("Report metric to Redis, key: %s, value: %s \n", key, value)
}

func GetLoad() map[string]int {
	client := redis.NewClient(&redis.Options{
		Addr: viper.GetString("db.addr"),
	})
	defer client.Close()
	resultList, err := client.Keys("kafkaScale:taskName:*").Result()
	if err == redis.Nil {
		return map[string]int{}
	} else if err != nil {
		fmt.Println(err)
		return map[string]int{}
	}

	res := make(map[string]int)
	for _, task := range resultList {
		tmStr := client.Get(task).Val()
		var tm model.TaskMetric
		err := json.Unmarshal([]byte(tmStr), &tm)
		if err != nil {
			panic(err)
		}
		if tm.IsRunning == true {
			addr := tm.WorkerAddress
			if _, ok := res[addr]; !ok {
				res[addr] = 0
			}
			res[addr] += 1
		}
	}
	return res
}