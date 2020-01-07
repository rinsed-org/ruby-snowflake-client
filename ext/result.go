package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"

VALUE ReturnEnumerator(VALUE cls);
VALUE createRbString(char* str);
VALUE funcall0param(VALUE obj, ID id);
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
		if i%5000 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		x := res.ScanNextRow(false)
		res.parsedRows = append(res.parsedRows, x)
		C.rb_yield(x)
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}

	//empty for GC
	res.rows = nil
	res.keptHash = C.Qnil
	res.parsedRows = []C.VALUE{}
	res.colRbArr = C.Qnil
	res.cols = []C.VALUE{}

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
		r := res.ScanNextRow(false)
		return r
	} else if rows.Err() == io.EOF {
		res.rows = nil        // free up for gc
		res.keptHash = C.Qnil // free up for gc
	}
	return C.Qnil
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

	// trick from postgres; keep hash: pg_result.c:1088
	hash := C.rb_hash_dup(res.keptHash)
	for idx, raw := range rawResult {
		raw := raw
		col_name := res.cols[idx]

		var rbVal C.VALUE

		if raw == nil {
			rbVal = C.Qnil
		} else {
			switch v := raw.(type) {
			case float64:
				rbVal = RbNumFromDouble(C.double(v))
			case bool:
				rbVal = C.Qfalse
				if v {
					rbVal = C.Qtrue
				}
			case time.Time:
				ts := &C.struct_timespec{C.long(v.Unix()), C.long(0)}
				rbVal = C.rb_time_timespec_new(ts, 0)
			case string:
				str := v
				rbVal = RbString(str)
			default:
				rb_raise(C.rb_eArgError, "Cannot parse type '%s'", fmt.Errorf("%T", v))
			}
		}
		C.rb_hash_aset(hash, col_name, rbVal)
	}
	return hash
}

func SafeMakeHash(lenght int, cols []C.VALUE) C.VALUE {
	var hash C.VALUE
	hash = C.rb_hash_new()

	if LOG_LEVEL > 0 {
		fmt.Println("starting make hash")
	}
	for _, col := range cols {
		C.rb_hash_aset(hash, col, C.Qnil)
	}
	if LOG_LEVEL > 0 {
		fmt.Println("end make hash", hash)
	}
	return hash
}

func (res *SnowflakeResult) Initialize() {
	columns, _ := res.rows.Columns()
	rbArr := C.rb_ary_new2(C.long(len(columns)))

	cols := make([]C.VALUE, len(columns))
	for idx, colName := range columns {
		str := strings.ToLower(colName)
		sym := C.rb_str_new2(C.CString(str))
		sym = C.rb_str_freeze(sym)
		cols[idx] = sym
		C.rb_ary_store(rbArr, C.long(idx), sym)
	}

	res.cols = cols
	res.keptHash = SafeMakeHash(len(columns), cols)
	res.colRbArr = rbArr
}
