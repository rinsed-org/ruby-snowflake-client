package main

/*
#include <stdlib.h>
#include "ruby/ruby.h"

void RbGcGuard(VALUE ptr);
VALUE ReturnEnumerator(VALUE cls);
VALUE RbNumFromDouble(double v);
*/
import "C"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

type SnowflakeClient struct {
	db *sql.DB
}

func FetchNoGVL(ptr C.VALUE) C.VALUE {
	x, _ := clientRef[self]
	return x.Fetch(statement)
}

func (x SnowflakeClient) Fetch(statement C.VALUE) C.VALUE {
	t1 := time.Now()

	if LOG_LEVEL > 0 {
		fmt.Println("statement", RbGoString(statement))
	}
	// this row needs to run w/o GVL
	C.rb_thread_call_without_gvl(ObjFetch, stmt, RUBY_UBF_IO, NULL)
	rows, err := x.db.QueryContext(sf.WithHigherPrecision(context.WithTimeout()), RbGoString(statement))
	// end
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
	cols, _ := rows.Columns()
	for idx, col := range cols {
		col := col
		cols[idx] = strings.ToLower(col)
	}
	rs := SnowflakeResult{rows, cols}
	resultMap[result] = &rs
	C.rb_ivar_set(result, RESULT_DURATION, RbNumFromDouble(C.double(duration)))
	return result
}

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
	clientRef[self] = &rs
}

//export ObjFetch
func ObjFetch(self C.VALUE, statement C.VALUE) C.VALUE {
	x, _ := clientRef[self]
	arrayOfStmtAndClient[self] = []C.VALUE{self, statement}
	C.rb_thread_call_without_gvl(
		FetchNoGVL,
		self,
		C.Ruby_UBF_IO,
		C.Qnil,
	)
}
