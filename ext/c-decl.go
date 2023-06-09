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

var marked = make(map[unsafe.Pointer]int)

//export goobj_mark
func goobj_mark(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		marked[obj] = marked[obj] + 1
		fmt.Printf("MARK log obj %v; counter: %d; total number of MARKED objects: %d\n", obj, marked[obj], len(marked))
	}
}

//export goobj_log
func goobj_log(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Println("log obj", obj)
	}
}

//export goobj_retain
func goobj_retain(obj unsafe.Pointer, x *C.char) {
	if LOG_LEVEL > 0 {
		fmt.Printf("retain obj [%v] %v - currently keeping %d\n", C.GoString(x), obj, len(objects))
	}
	objects[obj] = true
	marked[obj] = 0
}

//export goobj_free
func goobj_free(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Printf("CALLED GOOBJ FREE %v - CURRENTLY %d objects left\n", obj, len(objects))
	}
	delete(objects, obj)
	delete(marked, obj)
}

//export goobj_compact
func goobj_compact(obj unsafe.Pointer) {
	if LOG_LEVEL > 0 {
		fmt.Printf("CALLED GOOBJ COMPACT %v", obj)
	}
}
