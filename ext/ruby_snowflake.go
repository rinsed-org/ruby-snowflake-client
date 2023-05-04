package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
VALUE hello();
void Connect(VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE,VALUE);
void ObjFetch(VALUE,VALUE);
VALUE ObjNextRow(VALUE);
VALUE Inspect(VALUE);
VALUE GetRows(VALUE,VALUE);

VALUE NewGoStruct(VALUE klass, void *p);
VALUE GoRetEnum(VALUE,int,VALUE);
void* GetGoStruct(VALUE obj);
void RbGcGuard(VALUE ptr);
*/
import "C"

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unsafe"

	gopointer "github.com/mattn/go-pointer"
	sf "github.com/snowflakedb/gosnowflake"
)

type RubySnowflake struct {
	db       *sql.DB
	rows     *sql.Rows
	keptHash C.VALUE
}

var rbSnowflake C.VALUE

var objects = make(map[interface{}]bool)

//export goobj_log
func goobj_log(obj unsafe.Pointer) {
	fmt.Println("log obj", obj)
}

//export goobj_retain
func goobj_retain(obj unsafe.Pointer) {
	fmt.Println("retain obj")
	objects[obj] = true
}

//export goobj_free
func goobj_free(obj unsafe.Pointer) {
	fmt.Println("CALLED GOOBJ FREE")
	delete(objects, obj)
}

// @returns db pointer
// ugh, ruby and go were disagreeing about the length of `int` so I had to be particular here and in the ffi
//
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
	xx := RubySnowflake{db, nil, C.Qnil}
	ptr := gopointer.Save(&xx)
	q := C.NewGoStruct(
		rbSnowflake,
		ptr,
	)

	id := C.rb_intern(C.CString("db"))
	C.rb_ivar_set(self, id, q)
}

//export Close
func Close(db_pointer unsafe.Pointer) {
	//db := decodeDbPointer(db_pointer)
	//if db != nil {
	//db.Close()
	//}
}

//export ObjFetch
func ObjFetch(self C.VALUE, statement C.VALUE) {
	var q C.VALUE
	id := C.rb_intern(C.CString("db"))
	q = C.rb_ivar_get(self, id)

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	fmt.Println(q, req, f)
	x, ok := f.(*RubySnowflake)
	if !ok {
		rb_raise(C.rb_eArgError, "%s", errors.New("cannot convert x to pointer"))
	}

	t1 := time.Now()
	fmt.Println("statement", RbGoString(statement))
	//fmt.Println(x.db)
	//d := time.Now().Add(5 * time.Second)
	//ctxWithTimeout, _ := context.WithDeadline(context.Background(), d)
	//rows, err := x.db.QueryContext(ctxWithTimeout, RbGoString(statement))
	rows, err := x.db.Query(RbGoString(statement))
	fmt.Printf("Query duration: %s\n", time.Now().Sub(t1))
	if err != nil {
		rb_raise(C.rb_eArgError, "Query error: '%s'", err)
	}
	x.rows = rows

	return
}

//export Inspect
func Inspect(self C.VALUE) C.VALUE {
	id := C.rb_intern(C.CString("db"))
	q := C.rb_ivar_get(self, id)
	if q == C.Qnil {
		return RbString("Object is not instantiated")
	}

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	x := f.(*RubySnowflake)
	return RbString(fmt.Sprintf("%+v", x))
}

func (x RubySnowflake) ScanNextRow(debug bool) C.VALUE {
	rows := x.rows
	columns, _ := rows.Columns()
	rowLength := len(columns)

	rawResult := make([]interface{}, rowLength)
	rawData := make([]interface{}, rowLength)
	for i := range rawResult {
		rawData[i] = &rawResult[i]
	}

	err := rows.Scan(rawData...)
	if err != nil {
		rb_raise(C.rb_eArgError, "Cannot scan row: '%s'", err)
	}

	var hash C.VALUE

	// trick from postgres; keep hash: pg_result.c:1088
	if x.keptHash == C.Qnil {
		hash = C.rb_hash_new()
	} else {
		hash = x.keptHash
	}
	//C.RbGcGuard(hash)

	for idx, raw := range rawResult {
		//fix go pointer for for loop variable
		if debug {
			fmt.Printf("here4 - %d\n", idx)
		}
		raw := raw
		col_name := RbString(strings.ToLower(columns[idx]))
		if raw == nil {
			C.rb_hash_aset(hash, col_name, C.Qnil)
		} else {
			switch v := raw.(type) {
			case float64:
				C.rb_hash_aset(hash, col_name, C.rb_float_new(C.double(v)))
			case bool:
				var qq C.VALUE
				qq = C.Qfalse
				if v {
					qq = C.Qtrue
				}
				C.rb_hash_aset(hash, col_name, qq)
			case time.Time:
				ts := &C.struct_timespec{C.long(v.Unix()), C.long(0)}
				qq := C.rb_time_timespec_new(ts, 0)
				C.rb_hash_aset(hash, col_name, qq)
			default:
				str := fmt.Sprintf("%v", raw)
				C.rb_hash_aset(hash, col_name, C.rb_str_new(C.CString(str), C.long(len(str))))
			}
		}
	}

	x.keptHash = C.rb_hash_dup(hash)

	return hash
}

//export GetRows
func GetRows(self C.VALUE, inputDebug C.VALUE) C.VALUE {
	id := C.rb_intern(C.CString("db"))
	q := C.rb_ivar_get(self, id)
	//C.RbGcGuard(q)

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	x := f.(*RubySnowflake)
	rows := x.rows
	if rows == nil {
		rb_raise(C.rb_eArgError, "%s", errors.New("Empty result; please run a query first"))
	}
	d := RbGoString(inputDebug)
	var dbg bool
	if d == "debug" {
		dbg = true
	}

	if C.rb_block_given_p() == C.Qfalse {
		rb_raise(C.rb_eArgError, "%s", errors.New("this causes a memleak; please provide a block"))
		i := 0
		arr := []any{}
		t1 := time.Now()
		for rows.Next() {
			if i%100 == 0 {
				fmt.Println("scanning row: ", i)
			}
			q := x.ScanNextRow(dbg)
			//C.RbGcGuard(q)
			arr = append(arr, q)
			i = i + 1
		}
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
		t1 = time.Now()
		res := C.rb_ary_new2(C.long(len(arr)))
		//C.RbGcGuard(res)
		for idx, qqq := range arr {
			if idx%100 == 0 {
				fmt.Println("added to array: ", idx)
			}
			C.rb_ary_push(res, qqq.(C.VALUE))
			//C.rb_ary_store(res, C.long(idx), qqq)
		}
		fmt.Printf("done with creating ruby array: %s\n", time.Now().Sub(t1))
		x.rows = nil
		x.keptHash = C.Qnil
		return res
	} else {
		i := 0
		t1 := time.Now()
		for rows.Next() {
			if i%5000 == 0 {
				fmt.Println("scanning row: ", i)
			}
			C.rb_yield(x.ScanNextRow(false))
			i = i + 1
		}
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
		x.rows = nil
		x.keptHash = C.Qnil
	}

	return self
}

//export ObjNextRow
func ObjNextRow(self C.VALUE) C.VALUE {
	id := C.rb_intern(C.CString("db"))
	q := C.rb_ivar_get(self, id)

	req := C.GetGoStruct(q)
	f := gopointer.Restore(req)
	x := f.(*RubySnowflake)

	rows := x.rows
	if rows == nil {
		return C.Qnil
	}

	if rows.Next() {
		return x.ScanNextRow(false)
	} else if rows.Err() == io.EOF {
		x.rows = nil        // free up for gc
		x.keptHash = C.Qnil // free up for gc
	}
	return C.Qnil
}

//export hello
func hello() C.VALUE {
	ts := &C.struct_timespec{C.long(1682441971), C.long(5000)}

	fmt.Println("depress", ts)
	qq := C.rb_time_timespec_new(ts, 0)
	array := C.rb_ary_new2(5)
	C.rb_ary_push(array, qq)
	str := "dadsadsa"
	C.rb_ary_push(array, C.rb_str_new(C.CString(str), C.long(len(str))))
	C.rb_ary_push(array, C.VALUE(C.long(123)))
	C.rb_ary_push(array, C.VALUE(C.rb_float_new(C.double(123.5878))))

	return array
}

var rb_cGoSnow C.VALUE

//export Init_ruby_snowflake_client
func Init_ruby_snowflake_client() {
	rb_cGoSnow = C.rb_define_module(C.CString("AlexLibrary"))
	rbSnowflake = C.rb_define_class_under(rb_cGoSnow, C.CString("Snow"), C.rb_cObject)

	C.rb_define_method(rbSnowflake, C.CString("connect"), (*[0]byte)(C.Connect), 7)
	C.rb_define_method(rbSnowflake, C.CString("inspect"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflake, C.CString("to_s"), (*[0]byte)(C.Inspect), 0)
	C.rb_define_method(rbSnowflake, C.CString("fetch"), (*[0]byte)(C.ObjFetch), 1)
	C.rb_define_method(rbSnowflake, C.CString("next_row"), (*[0]byte)(C.ObjNextRow), 0)
	C.rb_define_method(rbSnowflake, C.CString("get_rows"), (*[0]byte)(C.GetRows), 1)

	//C.rb_define_method(rb_cGoSnow, C.CString("fetch"), (*[0]byte)(C.fetch), 1)
	C.rb_define_singleton_method(rb_cGoSnow, C.CString("library_version"), (*[0]byte)(C.hello), 0)

	fmt.Println("init ruby snowflake client")
	//C.rb_define_method(cls, C.CString("my_method"), (*[0]byte)(unsafe.Pointer(&q)), 2)
}

func main() {}
