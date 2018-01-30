package main

import (
	"lintest/201801/zookeeper"
	"fmt"
	"time"
)

var paths = []string{"/","/lin"}
func main()  {
	 zookeeper.NewZkManager("127.0.0.1:2181",5)
	zookeeper.CreateNode("/lin","onlyTest")
	zookeeper.SetWatcherPaths(paths)
	fmt.Println("------" + zookeeper.GetNode("/lin/a"))
	fmt.Println("------" + zookeeper.GetNode("/lin/name"))
	for i:=0;i<10;i++  {
		time.Sleep(1*time.Second)

		fmt.Println("------" + zookeeper.GetNode("/lin/a"))
		fmt.Println("------" + zookeeper.GetNode("/lin/name"))
		fmt.Printf("=======%+v\n",zookeeper.GetPathData())
		if i>=5 {
			reult,err := zookeeper.CreateNode("/lin/createNode"+time.Now().Format("20060102150405"),time.Now().Format("20060102150405"))
			fmt.Printf("zookeeper.CreateNode|result=%v,err=%+v\n",reult,err)
			reult, err = zookeeper.CreateNode("/createRootNode"+time.Now().Format("20060102150405"),time.Now().Format("20060102150405"))
			fmt.Printf("zookeeper.CreateNode|result=%v,err=%+v\n",reult,err)
			i = 0
		}
	}
}
