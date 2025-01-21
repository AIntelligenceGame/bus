package main

/*
#cgo CFLAGS: -g -Wall
#include <stdlib.h>
#include <stdio.h>
*/
import "C"
import (
	"fmt"
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
	// This function is not used when building a shared library
}
