package main

import "C"

// 定义一个结构体
type MyStruct struct {
	A int
	B int
}

//export add
func add(a, b int) int {
	return a + b
}

//export addStruct
func addStruct(s MyStruct) int {
	return s.A + s.B
}

func main() {}
