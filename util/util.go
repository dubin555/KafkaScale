package util

import (
	"github.com/satori/go.uuid"
	"net"
	"github.com/spf13/viper"
	"KafkaScale/service"
	"sort"
	"github.com/lexkong/log"
)

func GenUUID() string {
	u, err := uuid.NewV4()
	if err != nil {
		panic("error when generate orderId")
	}
	return u.String()
}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func GetLocalAddress() string {
	return GetLocalIP() + viper.GetString("port")
}

func IsDeployLocal() bool {
	if viper.GetString("deploy") == "cluster" {
		return false
	}
	return true
}

func GetLowestLoad() string {
	serverList := service.GetServerList(viper.GetStringSlice("zk.addr"), viper.GetString("zk.path"))
	log.Infof("All available server list: %s", serverList)
	loads := service.GetLoad()
	log.Infof("The current loads for now: %s", loads)

	type kv struct {
		Key string
		Value int
	}

	var ss []kv
	for k, v := range loads {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	// Find if some server not empty load
	for _, server := range serverList {
		if _, ok := loads[server]; !ok {
			return server
		}
	}

	for _, kv := range ss {
		for _, server := range serverList {
			if kv.Key == server {
				return server
			}
		}
	}

	return ""
}
