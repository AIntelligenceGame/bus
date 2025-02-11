package main

import (
	"fmt"
	"runtime"

	"github.com/ebitengine/purego"
)

// 定义与so.go中相同的结构体
type MyStruct struct {
	A int
	B int
}

func getSystemLibrary() string {
	switch runtime.GOOS {
	case "darwin":
		return "/usr/lib/libSystem.B.dylib"
	case "linux":
		return "example/sogo/libso.so"
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
}

// 无法支持结构体定义出入和使用（so库）
func main() {
	libc, err := purego.Dlopen(getSystemLibrary(), purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		panic(err)
	}
	var addStruct func(MyStruct) int
	purego.RegisterLibFunc(&addStruct, libc, "addStruct")

	// 创建结构体实例并调用addStruct函数
	s := MyStruct{A: 1, B: 2}
	result := addStruct(s)
	fmt.Println(result)
}
