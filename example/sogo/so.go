package main

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L. -lso
#include <stdlib.h>
#include "libso.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

//export PrintHello
func PrintHello() {
	fmt.Println("hello, world!")
}

//export PrintMessage
func PrintMessage(msg *C.char) {
	cstr := C.GoString(msg)
	fmt.Println(cstr)
}

func main() {
	// 调用共享库中的PrintHello函数
	C.PrintHello()

	// 调用共享库中的PrintMessage函数
	message := C.CString("Hello from Go!")
	defer C.free(unsafe.Pointer(message))
	C.PrintMessage(message)
}

/*
解释
cgo指令：

#cgo CFLAGS: -g -Wall：编译选项。
#cgo LDFLAGS: -L. -lso：链接选项，告诉链接器在当前目录下查找libso.so库。
#include <stdlib.h>：包含stdlib.h以确保C.free可用。
#include "libso.h"：包含生成的头文件libso.h，以便使用其中声明的函数。
调用函数：

C.PrintHello()：直接调用共享库中的PrintHello函数。
C.PrintMessage(message)：调用共享库中的PrintMessage函数，并传递一个C字符串。注意使用C.CString将Go字符串转换为C字符串，并使用defer C.free(unsafe.Pointer(message))释放内存。
*/
