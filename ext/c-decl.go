package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

//export goobj_mark
func goobj_mark(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Println("MARK log obj", obj)
	}
}

//export goobj_log
func goobj_log(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Println("log obj", obj)
	}
}

//export goobj_retain
func goobj_retain(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Println("retain obj")
	}
	objects[obj] = true
}

//export goobj_free
func goobj_free(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Println("CALLED GOOBJ FREE")
	}
	delete(objects, obj)
}
