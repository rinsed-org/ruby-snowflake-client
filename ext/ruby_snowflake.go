package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
void Connect(VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE);
VALUE ObjFetch(VALUE,VALUE);
VALUE ObjNextRow(VALUE);
VALUE Inspect(VALUE);
VALUE GetRows(VALUE);
VALUE GetRowsNoEnum(VALUE);

void RbGcGuard(VALUE ptr);
VALUE ReturnEnumerator(VALUE cls);
VALUE RbNumFromDouble(double v);
*/
import "C"

import (
	"fmt"
)

var rbSnowflakeClientClass C.VALUE
var rbSnowflakeResultClass C.VALUE
var rbSnowflakeModule C.VALUE

var RESULT_DURATION C.VALUE
var ERROR_IDENT C.VALUE

var objects = make(map[any]bool)
var resultMap = make(map[C.VALUE]*SnowflakeResult)
var clientRef = make(map[C.VALUE]*SnowflakeClient)

var LOG_LEVEL = 0
var empty C.VALUE = C.Qnil

//export Inspect
func Inspect(self C.VALUE) C.VALUE {
	x := clientRef[self]
	return RbString(fmt.Sprintf("Snowflake::Client <%+v>", x))
}

//export Init_ruby_snowflake_client_ext
func Init_ruby_snowflake_client_ext() {
	if LOG_LEVEL > 0 {
		fmt.Println("Initializing consts")
	}
	RESULT_DURATION = C.rb_intern(C.CString("@query_duration"))
	ERROR_IDENT = C.rb_intern(C.CString("@error"))
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Initializing module")
	}
	rbSnowflakeModule = C.rb_define_module(C.CString("Snowflake"))
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Initializing client class")
	}
	rbSnowflakeClientClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Client"), C.rb_cObject)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Initializing result class")
	}
	rbSnowflakeResultClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Result"), C.rb_cObject)

	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Setting up objects 1")
	}
	objects[rbSnowflakeResultClass] = true
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Setting up objects 2")
	}
	objects[rbSnowflakeClientClass] = true
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Setting up objects 3")
	}
	objects[rbSnowflakeModule] = true
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Setting up objects 4")
	}
	objects[RESULT_DURATION] = true
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Setting up objects 5")
	}
	objects[ERROR_IDENT] = true
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] GCGUARD call 1")
	}
	C.RbGcGuard(RESULT_DURATION)
	//C.RbGcGuard(RESULT_IDENTIFIER)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] GCGUARD call 2")
	}
	C.RbGcGuard(ERROR_IDENT)

	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 1")
	}
	C.rb_define_method(rbSnowflakeResultClass, C.CString("next_row"), (*[0]byte)(C.ObjNextRow), 0)
	// `get_rows` is private as this can lead to SEGFAULT errors if not invoked
	// with GC.disable due to undetermined issues caused by the Ruby GC.
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 2")
	}
	C.rb_define_private_method(rbSnowflakeResultClass, C.CString("_get_rows"), (*[0]byte)(C.GetRows), 0)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 3")
	}
	C.rb_define_method(rbSnowflakeResultClass, C.CString("get_rows_no_enum"), (*[0]byte)(C.GetRowsNoEnum), 0)

	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 4")
	}
	C.rb_define_private_method(rbSnowflakeClientClass, C.CString("_connect"), (*[0]byte)(C.Connect), 7)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 5")
	}
	C.rb_define_method(rbSnowflakeClientClass, C.CString("inspect"), (*[0]byte)(C.Inspect), 0)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 6")
	}
	C.rb_define_method(rbSnowflakeClientClass, C.CString("to_s"), (*[0]byte)(C.Inspect), 0)
	if LOG_LEVEL > 0 {
		fmt.Println("[ruby-snowflake] Define method 7")
	}
	C.rb_define_method(rbSnowflakeClientClass, C.CString("_fetch"), (*[0]byte)(C.ObjFetch), 1)

	if LOG_LEVEL > 0 {
		fmt.Println("init ruby snowflake client")
	}
}

func main() {}
