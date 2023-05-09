package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
void Connect(VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE);
VALUE ObjFetch(VALUE,VALUE);
VALUE ObjNextRow(VALUE);
VALUE Inspect(VALUE);
VALUE GetRows(VALUE);
VALUE GetAllRows(VALUE);

VALUE NewGoStruct(VALUE klass, void *p);
VALUE GoRetEnum(VALUE,int,VALUE);
void* GetGoStruct(VALUE obj);
void RbGcGuard(VALUE ptr);
VALUE ReturnEnumerator(VALUE cls);
VALUE RbNumFromDouble(double v);
*/
import "C"

import (
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
	//cols     C.VALUE
	cols       []C.VALUE
	colRbArr   C.VALUE
	parsedRows []C.VALUE
}
type SnowflakeClient struct {
	db *sql.DB
}

var rbSnowflakeClientClass C.VALUE
var rbSnowflakeResultClass C.VALUE
var rbSnowflakeModule C.VALUE

var DB_IDENTIFIER = C.rb_intern(C.CString("db"))
var RESULT_IDENTIFIER = C.rb_intern(C.CString("rows"))

var objects = make(map[interface{}]bool)

var LOG_LEVEL = 1

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
		rb_raise(C.rb_eArgError, "Snowflake Config Creation Error: '%s'", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		rb_raise(C.rb_eArgError, "Connection Error: '%s'", err)
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
	//fmt.Println(x.db)
	//d := time.Now().Add(5 * time.Second)
	//ctxWithTimeout, _ := context.WithDeadline(context.Background(), d)
	//rows, err := x.db.QueryContext(ctxWithTimeout, RbGoString(statement))
	rows, err := x.db.Query(RbGoString(statement))
	if LOG_LEVEL > 0 {
		fmt.Printf("Query duration: %s\n", time.Now().Sub(t1))
	}
	if err != nil {
		rb_raise(C.rb_eArgError, "Query error: '%s'", err)
	}

	var bla C.VALUE
	result := C.rb_class_new_instance(0, &bla, rbSnowflakeResultClass)
	rs := SnowflakeResult{rows, C.Qnil, []C.VALUE{}, C.Qnil, []C.VALUE{}}
	rs.Initialize()
	ptr := gopointer.Save(&rs)
	rbStruct := C.NewGoStruct(
		rbSnowflakeClientClass,
		ptr,
	)
	C.rb_ivar_set(result, RESULT_IDENTIFIER, rbStruct)
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
		rb_raise(C.rb_eArgError, "%s", errors.New("cannot convert x to pointer"))
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

//export Init_ruby_snowflake_client
func Init_ruby_snowflake_client() {
	rbSnowflakeModule = C.rb_define_module(C.CString("Snowflake"))
	rbSnowflakeClientClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Client"), C.rb_cObject)
	rbSnowflakeResultClass = C.rb_define_class_under(rbSnowflakeModule, C.CString("Result"), C.rb_cObject)

	C.rb_define_method(rbSnowflakeResultClass, C.CString("next_row"), (*[0]byte)(C.ObjNextRow), 0)
	C.rb_define_method(rbSnowflakeResultClass, C.CString("get_rows"), (*[0]byte)(C.GetRows), 0)
	C.rb_define_method(rbSnowflakeResultClass, C.CString("get_all_rows"), (*[0]byte)(C.GetAllRows), 0)

	C.rb_define_method(rbSnowflakeClientClass, C.CString("connect"), (*[0]byte)(C.Connect), 7)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("inspect"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("to_s"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflakeClientClass, C.CString("fetch"), (*[0]byte)(C.ObjFetch), 1)
	//debug.SetGCPercent(-1)

	fmt.Println("init ruby snowflake client")
}

func main() {}
