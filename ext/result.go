package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
#include "ruby/encoding.h"
//#include "ruby/internal/hash.h"

VALUE ReturnEnumerator(VALUE cls);
//void RbGcGuard(VALUE ptr);
VALUE createRbString(char* str);
VALUE funcall0param(VALUE obj, ID id);
//void RbEnc();
//VALUE MakeRbString(char* str);
//VALUE RbHashWithSize(int size);
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	gopointer "github.com/mattn/go-pointer"
)

func getResultStruct(self C.VALUE) *SnowflakeResult {
	ivar := C.rb_ivar_get(self, RESULT_IDENTIFIER)

	str := GetGoStruct(ivar)
	ptr := gopointer.Restore(str)
	sr, ok := ptr.(*SnowflakeResult)
	if !ok || sr.rows == nil {
		rb_raise(C.rb_eArgError, "%s", errors.New("Empty result; please run a query via `client.fetch(\"SQL\")`"))
		return nil
	}

	return sr
}

//export GetAllRows
func GetAllRows(self C.VALUE) C.VALUE {
	res := getResultStruct(self)
	rows := res.rows

	d := ""
	var dbg bool
	if d == "debug" {
		dbg = true
	}
	rb_raise(
		C.rb_eArgError,
		"%s",
		errors.New("this causes a memleak; please use the `get_rows` which returns an enumerator"),
	)
	// Below code never runs; as the exception above gets triggered.
	i := 0
	arr := []any{}
	t1 := time.Now()
	for rows.Next() {
		if i%1 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		q := res.ScanNextRow(dbg)
		//C.RbGcGuard(q)
		arr = append(arr, q)
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}
	t1 = time.Now()
	rbArr := C.rb_ary_new2(C.long(len(arr)))
	//C.RbGcGuard(res)
	for idx, qqq := range arr {
		if idx%100 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("added to array: ", idx)
			}
		}
		C.rb_ary_push(rbArr, qqq.(C.VALUE))
		//C.rb_ary_store(res, C.long(idx), qqq)
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with creating ruby array: %s\n", time.Now().Sub(t1))
	}
	res.rows = nil
	res.keptHash = C.Qnil
	return rbArr
}

//export GetRows
func GetRows(self C.VALUE) C.VALUE {
	res := getResultStruct(self)
	rows := res.rows

	enumRet := C.ReturnEnumerator(self)
	if enumRet != C.Qnil {
		return enumRet
	}

	i := 0
	t1 := time.Now()

	for rows.Next() {
		//for i < 50000 {
		if i%5000 == 0 {
			//if i%1 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		x := res.ScanNextRow(false)
		res.parsedRows = append(res.parsedRows, x)
		//objects[x] = true
		//x := RbString("abctest")
		//C.RbGcGuard(x)
		//C.funccall(x, rb_intern("to_h"), 0)
		C.rb_yield(x)
		//var empty C.VALUE
		//C.rb_yield(C.rb_funcall(x, C.rb_intern(C.CString("to_h")), C.int(0), &empty))
		//C.rb_yield(C.funcall0param(x, TO_H_ID))
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}
	res.rows = nil
	res.keptHash = C.Qnil

	return self
}

var TO_H_ID = C.rb_intern(C.CString("to_h"))

//export ObjNextRow
func ObjNextRow(self C.VALUE) C.VALUE {
	res := getResultStruct(self)
	rows := res.rows

	if rows == nil {
		return C.Qnil
	}

	if rows.Next() {
		r := res.ScanNextRow(false)
		return r
	} else if rows.Err() == io.EOF {
		res.rows = nil        // free up for gc
		res.keptHash = C.Qnil // free up for gc
	}
	return C.Qnil
}

type rowResult []any

func (x SnowflakeResult) ScanAllRows() []rowResult {
	res := make([]rowResult, 0)

	// This will need to be changed so it can be used in GetAllRows - to delay
	// creating ruby objects until we have scanned all rows; thus reducing the
	// time the Ruby objects live in memory.
	return res
}

func (res SnowflakeResult) ScanNextRow(debug bool) C.VALUE {
	rows := res.rows
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

	//hash := res.keptHash
	//hash := SafeMakeHash(len(res.cols), res.cols)
	//hash := C.rb_hash_dup(res.keptHash)
	//C.RbGcGuard(hash)
	//objects[hash] = true
	rbArr := C.rb_ary_new2(C.long(rowLength))
	//C.RbGcGuard(rbArr)
	//objects[rbArr] = true
	//C.RbGcGuard(hash)

	//var arr []C.VALUE

	//objects[&arr] = true

	//fmt.Println("hashID - ", hash)
	for idx, raw := range rawResult {
		//C.createRbString(C.CString("123"))
		//C.rb_utf8_str_new_cstr(C.CString("123"))
		//continue

		raw := raw
		col_name := res.cols[idx]
		//fmt.Println(col_name)
		var rbVal C.VALUE
		//arr = append(arr, col_name)

		if raw == nil {
			//C.rb_hash_aset(hash, col_name, C.Qnil)
			rbVal = C.Qnil
			//C.rb_ary_store(rbArr, C.long(idx), C.Qnil)
			//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, C.Qnil))
		} else {
			switch v := raw.(type) {
			case float64:
				//fmt.Println("float", v)
				rbVal = RbNumFromDouble(C.double(v))
				//C.rb_hash_aset(hash, col_name, rbVal)
				//C.rb_ary_store(rbArr, C.long(idx), RbNumFromDouble(C.double(v)))
				//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, RbNumFromDouble(C.double(v))))
			case bool:
				//fmt.Println("bool")
				var boolean C.VALUE
				boolean = C.Qfalse
				if v {
					boolean = C.Qtrue
				}
				rbVal = boolean
				//C.rb_hash_aset(hash, col_name, rbVal)
				//C.rb_ary_store(rbArr, C.long(idx), boolean)
				//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, boolean))
			case time.Time:
				//fmt.Println("time")
				ts := &C.struct_timespec{C.long(v.Unix()), C.long(0)}
				rbTs := C.rb_time_timespec_new(ts, 0)
				rbVal = rbTs
				//C.rb_hash_aset(hash, col_name, rbVal)
				//C.rb_ary_store(rbArr, C.long(idx), rbTs)
				//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, rbTs))
			case string:
				str := v
				//objects[str] = true
				rbStr := RbString(str)
				//C.RbGcGuard(rbStr)
				//objects[rbStr] = true
				//fmt.Println("string", str)
				//C.rb_hash_aset(hash, col_name, rbStr)
				rbVal = rbStr
				//C.rb_ary_store(rbArr, C.long(idx), INT2NUM(123))
				//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, RbString(str)))
				//C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, INT2NUM(1)))
				//objects[str] = true
				//x := RbString(str)
				//objects[x] = true

			default:
				fmt.Println("default %T", v)
			}
		}
		//C.rb_ary_store(rbArr, C.long(idx), INT2NUM(123))
		C.rb_ary_store(rbArr, C.long(idx), StoreInArrSize2(col_name, rbVal))
		//C.rb_hash_aset(hash, col_name, rbVal)
		//arr = append(arr, rbVal)
	}
	//objects[arr[0]] = true
	//C.RbGcGuard(arr[0])
	//C.rb_hash_bulk_insert(C.long(len(arr)), &arr[0], hash)

	//C.rb_obj_freeze(hash)
	//dup := C.rb_hash_dup(hash)
	//res.keptHash = dup
	//C.RbGcGuard(dup)
	//res.keptHash = SafeMakeHash(len(res.cols), res.cols)

	//zippedArr := C.rb_ary_zip(rbArr, res.colRbArr)
	//zippedHash := C.rb_funcall0param(rbArr, C.rb_intern(C.CString("to_h")))
	//return zippedHash
	C.rb_ivar_set(res.rbInstance, C.rb_intern(C.CString("@latest_row")), (rbArr))

	//C.rb_ivar_set(result, RESULT_IDENTIFIER, rbStruct)

	return rbArr
	//return hash
}

func StoreInArrSize2(v1 C.VALUE, v2 C.VALUE) C.VALUE {
	arr := C.rb_ary_new2(C.long(2))
	C.rb_ary_store(arr, 0, v1)
	C.rb_ary_store(arr, 1, v2)
	//C.RbGcGuard(arr)
	//objects[arr] = true
	//C.RbGcGuard(v1)
	//C.RbGcGuard(v2)
	//objects[v1] = true
	//objects[v2] = true
	//objects[arr] = true
	return arr
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func SafeMakeHash(lenght int, cols []C.VALUE) C.VALUE {
	var hash C.VALUE
	hash = C.rb_hash_new()

	fmt.Println("starting make hash")
	for _, col := range cols {
		C.rb_hash_aset(hash, col, C.Qnil)
	}
	fmt.Println("end make hash", hash)
	return hash
}

func (res *SnowflakeResult) Initialize() {
	//var hash C.VALUE

	//// trick from postgres; keep hash: pg_result.c:1088
	//hash = C.rb_hash_new()

	columns, _ := res.rows.Columns()
	rbArr := C.rb_ary_new2(C.long(len(columns)))

	cols := make([]C.VALUE, len(columns))
	for idx, colName := range columns {
		str := strings.ToLower(colName)
		sym := C.rb_str_new2(C.CString(str))
		sym = C.rb_str_freeze(sym)
		//sym = C.rb_obj_freeze(sym)
		cols[idx] = sym
		C.rb_ary_store(rbArr, C.long(idx), sym)
	}

	res.cols = cols
	//res.keptHash = hash
	//res.keptHash = SafeMakeHash(len(columns), cols)
	res.colRbArr = rbArr
}
