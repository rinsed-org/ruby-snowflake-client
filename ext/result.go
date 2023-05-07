package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"

VALUE ReturnEnumerator(VALUE cls);
void RbGcGuard(VALUE ptr);
//VALUE RbHashWithSize(int size);
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
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

	var hash C.VALUE

	// trick from postgres; keep hash: pg_result.c:1088
	if res.keptHash == C.Qnil {
		hash = C.rb_hash_new()
	} else {
		hash = res.keptHash
	}
	return hash
	//C.RbGcGuard(hash)
	//if res.cols == C.Qnil {
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println("SETTING COLS")
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//cols := C.rb_hash_new() //make([]C.VALUE, rowLength)
	//for idx, col := range columns {
	//C.rb_hash_aset(cols, INT2NUM(idx), C.rb_intern(C.CString(strings.ToLower(col))))
	//}
	//res.cols = cols
	//C.rb_obj_freeze(res.cols)
	//}
	//if len(res.cols) == 0 {
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println("SETTING COLS")
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	//fmt.Println()
	cols := make([]C.VALUE, rowLength)
	for idx, colName := range columns {
		//cols[idx] = RbString(strings.ToLower(colName))
		cols[idx] = C.rb_intern(C.CString(strings.ToLower(colName)))
	}

	for idx, raw := range rawResult {
		//fix go pointer for for loop variable
		if debug {
			fmt.Printf("here4 - %d\n", idx)
		}
		raw := raw
		col_name := cols[idx]
		//col_name := RbString(strings.ToLower(columns[idx]))
		//col_name := C.rb_intern(C.CString(strings.ToLower(columns[idx])))
		//col_name := C.rb_hash_aref(res.cols, INT2NUM((idx)))
		if raw == nil {
			C.rb_hash_aset(hash, col_name, C.Qnil)
		} else {
			switch v := raw.(type) {
			case float64:
				//C.rb_hash_aset(hash, col_name, C.rb_float_new(C.double(v)))
			case bool:
				qq := C.Qfalse
				if v {
					qq = C.Qtrue
				}
				fmt.Println(qq)
				//C.rb_hash_aset(hash, col_name, qq)
			case time.Time:
				ts := &C.struct_timespec{C.long(v.Unix()), C.long(0)}
				qq := C.rb_time_timespec_new(ts, 0)
				fmt.Println(qq)
				//C.rb_hash_aset(hash, col_name, qq)
			default:
				str := fmt.Sprintf("%v", raw)
				C.rb_hash_aset(hash, col_name, RbString(str))
			}
		}
	}

	//C.rb_obj_freeze(hash)
	//dup := C.rb_hash_dup(hash)
	//C.RbGcGuard(dup)
	//res.keptHash = dup

	return hash
}
