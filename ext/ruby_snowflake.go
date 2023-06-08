package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
void Connect(VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE);
VALUE ObjFetch(VALUE,VALUE);
VALUE ObjNextRow(VALUE);
VALUE Inspect(VALUE);
VALUE GetRows(VALUE);

VALUE NewGoStruct(VALUE klass, void *p);
VALUE GoRetEnum(VALUE,int,VALUE);
void* GetGoStruct(VALUE obj);
void RbGcGuard(VALUE ptr);
VALUE ReturnEnumerator(VALUE cls);
VALUE RbNumFromDouble(double v);
*/
import "C"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gopointer "github.com/mattn/go-pointer"
	sf "github.com/snowflakedb/gosnowflake"
)

type SnowflakeResult struct {
	rows     *sql.Rows
	keptHash C.VALUE
	cols     []C.VALUE
}
type SnowflakeClient struct {
	db *sql.DB
}

var rbSnowflakeClientClass C.VALUE
var rbSnowflakeResultClass C.VALUE
var rbSnowflakeModule C.VALUE

var DB_IDENTIFIER = C.rb_intern(C.CString("db"))
var RESULT_IDENTIFIER = C.rb_intern(C.CString("rows"))
var RESULT_DURATION = C.rb_intern(C.CString("@query_duration"))
var ERROR_IDENT = C.rb_intern(C.CString("@error"))

var objects = make(map[interface{}]bool)

var LOG_LEVEL = 0
var empty C.VALUE = C.Qnil

//export Connect
func Connect(self C.VALUE, account C.VALUE, warehouse C.VALUE, database C.VALUE, schema C.VALUE, user C.VALUE, password C.VALUE, role C.VALUE) {
	// other optional parms: Application, Host, and alt auth schemes
	cfg := &sf.Config{
		Account:   RbGoString(account),
		Warehouse: RbGoString(warehouse),
		Database:  RbGoString(database),
		Schema:    RbGoString(schema),
		User:      RbGoString(user),
		Password:  RbGoString(password),
		Role:      RbGoString(role),
		Port:      int(443),
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		errStr := fmt.Sprintf("Snowflake Config Creation Error: '%s'", err.Error())
		C.rb_ivar_set(self, ERROR_IDENT, RbString(errStr))
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		errStr := fmt.Sprintf("Connection Error: '%s'", err.Error())
		C.rb_ivar_set(self, ERROR_IDENT, RbString(errStr))
	}
	rs := SnowflakeClient{db}
	ptr := gopointer.Save(&rs)
	rbStruct := C.NewGoStruct(
		rbSnowflakeClientClass,
		ptr,
	)

	C.rb_ivar_set(self, DB_IDENTIFIER, rbStruct)
}

func (x SnowflakeClient) Fetch(statement C.VALUE) C.VALUE {
	t1 := time.Now()

	if LOG_LEVEL > 0 {
		fmt.Println("statement", RbGoString(statement))
	}
	rows, err := x.db.QueryContext(sf.WithHigherPrecision(context.Background()), RbGoString(statement))
	if err != nil {
		result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
		errStr := fmt.Sprintf("Query error: '%s'", err.Error())
		C.rb_ivar_set(result, ERROR_IDENT, RbString(errStr))
		return result
	}

	duration := time.Now().Sub(t1).Seconds()
	if LOG_LEVEL > 0 {
		fmt.Printf("Query duration: %s\n", time.Now().Sub(t1))
	}
	if err != nil {
		result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
		errStr := fmt.Sprintf("Query error: '%s'", err.Error())
		C.rb_ivar_set(result, ERROR_IDENT, RbString(errStr))
		return result
	}

	result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
	rs := SnowflakeResult{rows, C.Qnil, []C.VALUE{}}
	rs.Initialize()
	ptr := gopointer.Save(&rs)
	rbStruct := C.NewGoStruct(
		rbSnowflakeClientClass,
		ptr,
	)
	C.RbGcGuard(rbStruct)
	C.RbGcGuard(rbSnowflakeResultClass)
	C.rb_ivar_set(result, RESULT_IDENTIFIER, rbStruct)
	C.rb_ivar_set(result, RESULT_DURATION, RbNumFromDouble(C.double(duration)))
	return result
}

//export ObjFetch
func ObjFetch(self C.VALUE, statement C.VALUE) C.VALUE {
	var q C.VALUE
	q = C.rb_ivar_get(self, DB_IDENTIFIER)

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	x, ok := f.(*SnowflakeClient)
	if !ok {
		wrapRbRaise((errors.New("cannot convert SnowflakeClient pointer in ObjFetch")))
	}

	return x.Fetch(statement)
}

//export Inspect
func Inspect(self C.VALUE) C.VALUE {
	q := C.rb_ivar_get(self, DB_IDENTIFIER)
	if q == C.Qnil {
		return RbString("Object is not instantiated")
	}

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	x := f.(*SnowflakeClient)
	return RbString(fmt.Sprintf("%+v", x))
}

//export Init_ruby_snowflake_client_ext
func Init_ruby_snowflake_client_ext() {
	rbSnowflakeModule = C.rb_define_module(C.CString("Snowflake"))
	rbSnowflakeClientClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Client"), C.rb_cObject)
	rbSnowflakeResultClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Result"), C.rb_cObject)

	C.rb_define_method(rbSnowflakeResultClass, C.CString("next_row"), (*[0]byte)(C.ObjNextRow), 0)
	// `get_rows` is private as this can lead to SEGFAULT errors if not invoked
	// with GC.disable due to undetermined issues caused by the Ruby GC.
	C.rb_define_private_method(rbSnowflakeResultClass, C.CString("_get_rows"), (*[0]byte)(C.GetRows), 0)

	C.rb_define_private_method(rbSnowflakeClientClass, C.CString("_connect"), (*[0]byte)(C.Connect), 7)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("inspect"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("to_s"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("_fetch"), (*[0]byte)(C.ObjFetch), 1)

	if LOG_LEVEL > 0 {
		fmt.Println("init ruby snowflake client")
	}
}

func main() {}
