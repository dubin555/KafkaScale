package service

import (
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/lexkong/log"
	"fmt"
)

func Register(hosts []string, path string, node string) {

	zkPath := path + "/" + node
	log.Infof("Try to create service path: %s, in zk: %s", zkPath, hosts)

	conn, events, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		log.Infof("Error when Register in ZK %s", err)
		fmt.Println(err)
		return
	}
	//defer conn.Close()

	var flags int32 = 1
	var acls = zk.WorldACL(zk.PermAll)

	//todo close this goroutine
	go func() {
		for {
			select {
			case <- events:
				p, err := conn.Create(zkPath, []byte(""), flags, acls)

				if err != nil {
					log.Infof("Error when Create path -> %s", zkPath)
					fmt.Println(err)
					//return
				} else {
					log.Infof("Success register in path -> %s", p)
				}
			}
		}
	}()

}

func GetServerList(hosts []string, path string) []string {

	log.Infof("Try to get server list")

	conn, _, err := zk.Connect(hosts, time.Second * 5)
	if err != nil {
		log.Infof("Error when Get server list in %s", path)
		fmt.Println(err)
		return []string{}
	}
	defer conn.Close()
	serverList, _, err := conn.Children(path)
	if err != nil {
		log.Infof("Error when Get server list")
		fmt.Println(err)
		return []string{}
	}
	return serverList
}
