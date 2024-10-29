package main

import (
	"fmt"

	"github.com/AIntelligenceGame/bus/consul"
)

func main() {
	info := &consul.ClientInfo{
		Name:    "serverNode",
		Tag:     "v1000",
		Address: "localhost:8500",
	}
	//获取 server 注册的 IP和地址
	mp, err := consul.SearchServer(info)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(mp)
}
