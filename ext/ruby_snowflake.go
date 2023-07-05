package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
void Connect(VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE);
VALUE ObjFetch(VALUE,VALUE);
VALUE ObjFetchWithDB(VALUE,VALUE,VALUE);
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

var RESULT_IDENTIFIER = C.rb_intern(C.CString("rows"))
var RESULT_DURATION = C.rb_intern(C.CString("@query_duration"))
var ERROR_IDENT = C.rb_intern(C.CString("@error"))

var objects = make(map[interface{}]bool)
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
	rbSnowflakeModule = C.rb_define_module(C.CString("Snowflake"))
	rbSnowflakeClientClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Client"), C.rb_cObject)
	rbSnowflakeResultClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Result"), C.rb_cObject)

	objects[rbSnowflakeResultClass] = true
	objects[rbSnowflakeClientClass] = true
	objects[rbSnowflakeModule] = true
	objects[RESULT_DURATION] = true
	objects[ERROR_IDENT] = true
	C.RbGcGuard(RESULT_DURATION)
	//C.RbGcGuard(RESULT_IDENTIFIER)
	C.RbGcGuard(ERROR_IDENT)

	C.rb_define_method(rbSnowflakeResultClass, C.CString("next_row"), (*[0]byte)(C.ObjNextRow), 0)
	// `get_rows` is private as this can lead to SEGFAULT errors if not invoked
	// with GC.disable due to undetermined issues caused by the Ruby GC.
	C.rb_define_private_method(rbSnowflakeResultClass, C.CString("_get_rows"), (*[0]byte)(C.GetRows), 0)
	C.rb_define_method(rbSnowflakeResultClass, C.CString("get_rows_no_enum"), (*[0]byte)(C.GetRowsNoEnum), 0)

	C.rb_define_private_method(rbSnowflakeClientClass, C.CString("_connect"), (*[0]byte)(C.Connect), 7)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("inspect"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("to_s"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("_fetch"), (*[0]byte)(C.ObjFetch), 1)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("_fetch_with_db"), (*[0]byte)(C.ObjFetchWithDB), 2)

	if LOG_LEVEL > 0 {
		fmt.Println("init ruby snowflake client")
	}
}

func main() {}
