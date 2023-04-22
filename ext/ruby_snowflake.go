package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"

	gopointer "github.com/mattn/go-pointer"
	sf "github.com/snowflakedb/gosnowflake"
)

// Lazy coding: storing last error and connection as global vars bc don't want to figure out how to pkg and pass them
// back and forth to ruby
var last_error error

//export LastError
func LastError() *C.char {
	if last_error == nil {
		return nil
	} else {
		return C.CString(last_error.Error())
	}
}

// @returns db pointer
// ugh, ruby and go were disagreeing about the length of `int` so I had to be particular here and in the ffi
//
//export Connect
func Connect(account *C.char, warehouse *C.char, database *C.char, schema *C.char,
	user *C.char, password *C.char, role *C.char, port int64) unsafe.Pointer {
	// other optional parms: Application, Host, and alt auth schemes
	cfg := &sf.Config{
		Account:   C.GoString(account),
		Warehouse: C.GoString(warehouse),
		Database:  C.GoString(database),
		Schema:    C.GoString(schema),
		User:      C.GoString(user),
		Password:  C.GoString(password),
		Role:      C.GoString(role),
		Port:      int(port),
	}

	dsn, last_error := sf.DSN(cfg)
	if last_error != nil {
		return nil
	}

	var db *sql.DB
	db, last_error = sql.Open("snowflake", dsn)
	if db == nil {
		return nil
	} else {
		return gopointer.Save(db)
	}
}

//export Close
func Close(db_pointer unsafe.Pointer) {
	db := decodeDbPointer(db_pointer)
	if db != nil {
		db.Close()
	}
}

// @return number of rows affected or -1 for error
//
//export Exec
func Exec(db_pointer unsafe.Pointer, statement *C.char) int64 {
	db := decodeDbPointer(db_pointer)
	var res sql.Result
	res, last_error = db.Exec(C.GoString(statement))
	if res != nil {
		rows, _ := res.RowsAffected()
		return rows
	}
	return -1
}

//export Fetch
func Fetch(db_pointer unsafe.Pointer, statement *C.char) unsafe.Pointer {
	db := decodeDbPointer(db_pointer)
	var rows *sql.Rows
	t1 := time.Now()
	rows, last_error = db.Query(C.GoString(statement))
	fmt.Printf("Query duration: %s\n", time.Now().Sub(t1))
	if rows != nil {
		result := gopointer.Save(rows)
		return result
	} else {
		return nil
	}
}

// @return column names[List<String>] for the given query.
//
//export QueryColumns
func QueryColumns(rows_pointer unsafe.Pointer) **C.char {
	rows := decodeRowsPointer(rows_pointer)
	if rows == nil {
		return nil
	}

	columns, _ := rows.Columns()
	rowLength := len(columns)

	// See `NextRow` for why this pattern
	pointerSize := unsafe.Sizeof(rows_pointer)
	// Allocate an array for the string pointers.
	var out **C.char
	out = (**C.char)(C.malloc(C.ulong(rowLength) * C.ulong(pointerSize)))

	pointer := out
	for _, raw := range columns {
		// Find where to store the address of the next string.
		// Copy each output string to a C string, and add it to the array.
		// C.CString uses malloc to allocate memory.
		*pointer = C.CString(string(raw))
		// inc pointer to next array ele
		pointer = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(pointer)) + pointerSize))
	}
	return out
}

// @return column names[List<String>] for the given query.
//
//export QueryColumnCount
func QueryColumnCount(rows_pointer unsafe.Pointer) int32 {
	rows := decodeRowsPointer(rows_pointer)
	if rows == nil {
		return 0
	}

	columns, _ := rows.Columns()
	return int32(len(columns))
}

// NOTE: gc's the rows_pointer object on EOF and returns nil. LastError is set to EOF
//
//export NextRow
func NextRow(rows_pointer unsafe.Pointer) *unsafe.Pointer {
	rows := decodeRowsPointer(rows_pointer)
	if rows == nil {
		return nil
	}

	if rows.Next() {
		columns, _ := rows.Columns()
		rowLength := len(columns)

		rawResult := make([]interface{}, rowLength)
		rawData := make([]interface{}, rowLength)
		for i := range rawResult {
			rawData[i] = &rawResult[i]
		}

		// https://stackoverflow.com/questions/58866962/how-to-pass-an-array-of-strings-and-get-an-array-of-strings-in-ruby-using-go-sha
		pointerSize := unsafe.Sizeof(rows_pointer)
		// Allocate an array for the string pointers.

		last_error = rows.Scan(rawData...)
		if last_error != nil {
			return nil
		}

		// Allocate an array for the string pointers.
		var out *unsafe.Pointer
		out = (*unsafe.Pointer)(C.malloc(C.ulong(rowLength) * C.ulong(pointerSize)))
		pointer := out

		var typesOut **C.char
		typesOut = (**C.char)(C.malloc(C.ulong(rowLength) * C.ulong(pointerSize)))
		typesPointer := typesOut

		for _, raw := range rawResult {
			// Copy each output string to a C string, and add it to the array.
			// C.CString uses malloc to allocate memory.

			//fix go pointer for for loop variable
			raw := raw
			if raw == nil {
				*pointer = nil
				*typesPointer = nil
			} else {
				rawType := "string"

				switch v := raw.(type) {
				case float64:
					qq := C.double(v)
					*pointer = unsafe.Pointer(&qq)
					rawType = "double"
				case bool:
					qq := C.short(0)
					if v {
						qq = C.short(1)
					}
					*pointer = unsafe.Pointer(&qq)
					rawType = "short"
				case time.Time:
					rawType = "time.Time"
					*pointer = unsafe.Pointer(C.CString(v.Format(time.RFC3339)))
				default:
					*pointer = unsafe.Pointer(C.CString(fmt.Sprintf("%v", raw)))
				}

				t := C.CString(rawType)
				*typesPointer = t
			}
			pointer = (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(pointer)) + pointerSize))
			typesPointer = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(typesPointer)) + pointerSize))
		}

		var finalOut *unsafe.Pointer
		finalOut = (*unsafe.Pointer)(C.malloc(C.ulong(unsafe.Sizeof(out)) + C.ulong(unsafe.Sizeof(typesOut))))
		finalPointer := finalOut
		*finalPointer = unsafe.Pointer(out)
		finalPointer = (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(finalPointer)) + unsafe.Sizeof(pointerSize)))
		*finalPointer = unsafe.Pointer(typesOut)
		finalPointer = (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(finalPointer)) + unsafe.Sizeof(pointerSize)))

		return finalOut

	} else if rows.Err() == io.EOF {
		gopointer.Unref(rows_pointer) // free up for gc
	}
	return nil
}

func decodeDbPointer(db_pointer unsafe.Pointer) *sql.DB {
	if db_pointer == nil {
		last_error = errors.New("db_pointer is null. Cannot process command.")
		return nil
	}
	return gopointer.Restore(db_pointer).(*sql.DB)
}

func decodeRowsPointer(rows_pointer unsafe.Pointer) *sql.Rows {
	if rows_pointer == nil {
		last_error = errors.New("rows_pointer null: cannot fetch")
		return nil
	}
	var rows *sql.Rows
	rows = gopointer.Restore(rows_pointer).(*sql.Rows)

	if rows == nil {
		last_error = errors.New("rows_pointer invalid: Restore returned nil")
	}
	return rows
}

func main() {}
