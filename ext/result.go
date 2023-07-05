package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"

VALUE ReturnEnumerator(VALUE cls);
*/
import "C"

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	gopointer "github.com/mattn/go-pointer"
)

type SnowflakeResult struct {
	rows    *sql.Rows
	columns []string
	conn    *sql.Conn
}

func wrapRbRaise(err error) {
	fmt.Printf("[ruby-snowflake-client] Error encountered: %s\n", err.Error())
	fmt.Printf("[ruby-snowflake-client] Will call `rb_raise`\n")
	rb_raise(C.rb_eArgError, "%s", err.Error())
}

func getResultStruct(self C.VALUE) *SnowflakeResult {
	return resultMap[self]
}

//export GetRowsNoEnum
func GetRowsNoEnum(self C.VALUE) C.VALUE {
	res := getResultStruct(self)
	rows := res.rows

	i := 0
	t1 := time.Now()
	var arr []C.VALUE

	for rows.Next() {
		if i%5000 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		x := res.ScanNextRow(false)
		objects[x] = true
		gopointer.Save(x)
		if LOG_LEVEL > 1 {
			// This is VERY noisy
			fmt.Printf("alloced %v\n", &x)
		}
		arr = append(arr, x)
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}

	rbArr := C.rb_ary_new2(C.long(len(arr)))
	for idx, elem := range arr {
		C.rb_ary_store(rbArr, C.long(idx), elem)
	}

	res.Close()
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
		if i%5000 == 0 {
			if LOG_LEVEL > 0 {
				fmt.Println("scanning row: ", i)
			}
		}
		x := res.ScanNextRow(false)
		C.rb_yield(x)
		i = i + 1
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("done with rows.next: %s\n", time.Now().Sub(t1))
	}
	res.Close()

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
	}
	res.Close()
	return C.Qnil
}

func (res SnowflakeResult) Close() {
	if LOG_LEVEL > 0 {
		fmt.Println("called res.close")
	}
	res.rows.Close()
	res.conn.Close()
}

func (res SnowflakeResult) ScanNextRow(debug bool) C.VALUE {
	rows := res.rows
	if LOG_LEVEL > 0 {
		cts, _ := rows.ColumnTypes()
		fmt.Printf("column types: %+v; %+v\n", cts[0], cts[0].ScanType())
	}

	rawResult := make([]any, len(res.columns))
	rawData := make([]any, len(res.columns))
	for i := range rawResult {
		rawData[i] = &rawResult[i]
	}

	err := rows.Scan(rawData...)
	if err != nil {
		err = fmt.Errorf("Cannot scan row: '%s'", err)
		wrapRbRaise(err)
	}

	// trick from postgres; keep hash: pg_result.c:1088
	//hash := C.rb_hash_dup(res.keptHash)
	hash := C.rb_hash_new()
	if LOG_LEVEL > 1 {
		// This is very noisy
		fmt.Println("alloc'ed new hash", &hash)
	}

	for idx, raw := range rawResult {
		raw := raw

		var rbVal C.VALUE

		if raw == nil {
			rbVal = C.Qnil
		} else {
			switch v := raw.(type) {
			case int64:
				rbVal = RbNumFromLong(C.long(v))
			case float64:
				rbVal = RbNumFromDouble(C.double(v))
			case *big.Float:
				f64, _ := v.Float64()
				rbVal = RbNumFromDouble(C.double(f64))
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
				err := fmt.Errorf("Cannot parse type : '%T'", v)
				wrapRbRaise(err)
			}
		}
		colstr := C.rb_str_new2(C.CString(res.columns[idx]))
		if LOG_LEVEL > 1 {
			// This is very noisy
			fmt.Printf("alloc string: %+v; rubyVal: %+v\n", &colstr, &rbVal)
		}
		C.rb_hash_aset(hash, colstr, rbVal)
	}
	return hash
}
