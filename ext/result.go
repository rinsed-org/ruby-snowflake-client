package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"
#include "ruby/encoding.h"
//#include "ruby/internal/hash.h"

VALUE ReturnEnumerator(VALUE cls);
void RbGcGuard(VALUE ptr);
VALUE createRbString(char* str);
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
		if i%100 == 0 {
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
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		x := res.ScanNextRow(false)
		//x := RbString("abctest")
		//C.RbGcGuard(x)
		C.rb_yield(x)
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}
	res.rows = nil
	res.keptHash = C.Qnil

	return self
}

//export ObjNextRow
func ObjNextRow(self C.VALUE) C.VALUE {
	res := getResultStruct(self)
	rows := res.rows

	if rows == nil {
		return C.Qnil
	}

	if rows.Next() {
		return res.ScanNextRow(false)
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

func (res *SnowflakeResult) ScanNextRow(debug bool) C.VALUE {
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

	hash := res.keptHash
	//hash := SafeMakeHash(len(res.cols), res.cols)

	//fmt.Println("hashID - ", hash)
	for idx, raw := range rawResult {
		//C.createRbString(C.CString("123"))
		//C.rb_utf8_str_new_cstr(C.CString("123"))
		//continue

		raw := raw
		col_name := res.cols[idx]

		if raw == nil {
			C.rb_hash_aset(hash, col_name, C.Qnil)
		} else {
			switch v := raw.(type) {
			case float64:
				//fmt.Println("float", v)
				C.rb_hash_aset(hash, col_name, RbNumFromDouble(C.double(v)))
			case bool:
				//fmt.Println("bool")
				var boolean C.VALUE
				boolean = C.Qfalse
				if v {
					boolean = C.Qtrue
				}
				C.rb_hash_aset(hash, col_name, boolean)
			case time.Time:
				//fmt.Println("time")
				ts := &C.struct_timespec{C.long(v.Unix()), C.long(0)}
				rbTs := C.rb_time_timespec_new(ts, 0)
				C.rb_hash_aset(hash, col_name, rbTs)
			case int64:
				fmt.Println("int64")
			case string:
				//str := fmt.Sprintf("(%v)", raw)
				str := v
				//b := utf8.ValidString(str)
				//if b == false {
				//fmt.Println()
				//fmt.Println()
				//fmt.Println()
				//fmt.Println()
				//fmt.Println("invalid UTF8 ", v)
				//fmt.Println()
				//fmt.Println()
				//fmt.Println()
				//fmt.Println()
				//fmt.Println()
				//}
				//str := RandStringBytesRmndr(15)
				//fmt.Printf("default %T; (%s)\n", raw, str)
				//sym := C.rb_str_new2(C.CString(str))
				//sym := C.rb_tainted_str_new2(C.CString(str))
				//sym := C.rb_tainted_str_new_cstr(C.CString(str))
				//cstr := C.CString(str)
				//sym := C.rb_external_str_new_cstr(cstr)
				//sym := C.rb_str_new(cstr, C.long(len(str)))
				//rb_enc := C.RbEnc()
				//rb_enc := C.rb_utf8_encoding()
				//C.rb_enc_associate(sym, rb_enc)

				//sym := C.rb_utf8_str_new_cstr(cstr)
				//sym := C.rb_str_new_cstr(unsafe.Pointer(cstr))
				//C.free(unsafe.Pointer(cstr))
				//sym := C.rb_str_new_static(cstr, len(str))
				//C.RbGcGuard(sym)
				//sym := RbString(str)
				//C.rb_str_modify(sym)
				//C.RbGcGuard(sym)

				//enc_idx := C.GetEncoding(cstr)
				//C.rb_enc_set_index(sym, enc_idx)

				//rb_enc_set_index((obj), (i)); \

				//C.rb_str_new2(C.CString(str))
				//sym = C.rb_str_freeze(sym)
				//sym := RbString(letterBytes)
				//cstr := C.CString(str)
				//sym := C.rb_external_str_new_cstr(cstr)
				//sym := C.rb_str_new2(cstr, C.long(len(str)))
				//C.rb_str_new2(cstr)
				//fmt.Printf("string ; (%s) %v\n", str, sym)
				//rb_tainted_str_new
				//objects[sym] = true
				//C.rb_tainted_str_new_cstr(C.CString(str))
				//cstr := (*C.char)(unsafe.Pointer(&(*(*[]byte)(unsafe.Pointer(&str)))[0]))
				//q := C.createRbString(C.CString("123"))
				//C.RbGcGuard(q)
				C.rb_hash_aset(
					hash,
					col_name,
					C.rb_utf8_str_new_cstr(C.CString(str)),
				)
				//INT64toNUM(123456),
				//RbString(str),
				//C.rb_str_new2(C.CString(str)),
				//C.Qnil,
				//)
				//C.RbGcGuard(sym)
				////C.rb_str_new2(C.CString(fmt.Sprintf("%v", raw))),
				////C.rb_str_new2(C.CString(fmt.Sprintf("%v", raw))),
				//)
			default:
				fmt.Println("default %T", v)
			}
		}
	}

	//C.rb_obj_freeze(hash)
	//dup := C.rb_hash_dup(hash)
	//res.keptHash = dup
	//C.RbGcGuard(dup)
	//res.keptHash = SafeMakeHash(len(res.cols), res.cols)

	return hash
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

	cols := make([]C.VALUE, len(columns))
	for idx, colName := range columns {
		str := strings.ToLower(colName)
		sym := C.rb_str_new2(C.CString(str))
		sym = C.rb_str_freeze(sym)
		//sym = C.rb_obj_freeze(sym)
		cols[idx] = sym
	}

	res.cols = cols
	//res.keptHash = hash
	res.keptHash = SafeMakeHash(len(columns), cols)
}
