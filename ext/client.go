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
	db  *sql.DB
	cfg *sf.Config
}

func (x SnowflakeClient) Fetch(statement string) C.VALUE {
	t1 := time.Now()

	if LOG_LEVEL > 0 {
		fmt.Println("statement", statement)
	}
	if LOG_LEVEL > 0 {
		fmt.Println("getting conn")
	}
	dbCtx := context.Background()
	conn, _ := x.db.Conn(dbCtx)
	if LOG_LEVEL > 0 {
		fmt.Println("got conn")
	}
	rows, err := conn.QueryContext(sf.WithHigherPrecision(context.Background()), statement)
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

	result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
	if LOG_LEVEL > 0 {
		fmt.Println("create new instance")
	}
	cols, _ := rows.Columns()
	for idx, col := range cols {
		col := col
		cols[idx] = strings.ToLower(col)
	}
	rs := SnowflakeResult{rows, cols, conn}
	resultMap[result] = &rs
	if LOG_LEVEL > 0 {
		fmt.Println("after for & map")
	}
	C.rb_ivar_set(result, RESULT_DURATION, RbNumFromDouble(C.double(duration)))
	if LOG_LEVEL > 0 {
		fmt.Println("end of func")
	}
	return result
}

func (x SnowflakeClient) FetchWithDB(statement string, dbName string) C.VALUE {
	t1 := time.Now()

	if LOG_LEVEL > 0 {
		fmt.Println("statement", statement)
	}
	if LOG_LEVEL > 0 {
		fmt.Println("getting conn")
	}
	//dbCtx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	dbCtx := context.Background()
	conn, _ := x.db.Conn(dbCtx)
	if LOG_LEVEL > 0 {
		fmt.Println("got conn")
	}
	stmt := fmt.Sprintf("USE DATABASE %s", dbName)
	_, err := conn.ExecContext(context.Background(), stmt)
	if err != nil {
		result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
		errStr := fmt.Sprintf("Query %s had error \n error: '%s'", stmt, err.Error())
		C.rb_ivar_set(result, ERROR_IDENT, RbString(errStr))
		return result
	}
	if LOG_LEVEL > 0 {
		fmt.Printf("exec duration: %s\n", time.Now().Sub(t1))
		t1 = time.Now()
	}
	rows, err := conn.QueryContext(sf.WithHigherPrecision(context.Background()), statement)
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

	result := C.rb_class_new_instance(0, &empty, rbSnowflakeResultClass)
	if LOG_LEVEL > 0 {
		fmt.Println("create new instance")
	}
	cols, _ := rows.Columns()
	for idx, col := range cols {
		col := col
		cols[idx] = strings.ToLower(col)
	}
	rs := SnowflakeResult{rows, cols, conn}
	resultMap[result] = &rs
	if LOG_LEVEL > 0 {
		fmt.Println("after for & map")
	}
	C.rb_ivar_set(result, RESULT_DURATION, RbNumFromDouble(C.double(duration)))
	if LOG_LEVEL > 0 {
		fmt.Println("end of func")
	}
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
	rs := SnowflakeClient{db, cfg}
	clientRef[self] = &rs
}

//export ObjFetch
func ObjFetch(self C.VALUE, statement C.VALUE) C.VALUE {
	x, _ := clientRef[self]
	stmt := RbGoString(statement)

	return x.Fetch(stmt)
}

//export ObjFetchWithDB
func ObjFetchWithDB(self C.VALUE, statement C.VALUE, database C.VALUE) C.VALUE {
	x, _ := clientRef[self]
	stmt := RbGoString(statement)
	db := RbGoString(database)

	return x.FetchWithDB(stmt, db)
}
