//Scan sql rows into structures

/*
Package gofastersql is a tool designed to significantly enhance the efficiency and simplicity of scanning SQL rows into structures.

Unlike the native MySQL package in GoLang, which is often slow and unwieldy, GoFasterSQL streamlines the process with remarkable speed and ease.

A critical flaw in the native scanning process is its repetitive type determination for each row scan. It laboriously matches each field's type with its native counterpart before converting the data string (byte array), a process that is both time-consuming and cumbersome. Furthermore, the requirement to specify each field for scanning seems unnecessarily tedious.

In contrast, GoFasterSQL precalculates string-to-type conversions for each field, utilizing pointers to dedicated conversion functions. This approach eliminates the need for type lookups during scanning, vastly improving performance. The library offers a 2 to 2.5 times speed increase compared to native scan methods, a boost that varies with the number of items in each scan. Moreover, its automatic structure determination feature is a significant time-saver during coding.

The library’s ModelStruct function, upon its first invocation for a type, determines the structure of that type through recursive reflection. This structure is then cached, allowing for swift and efficient reuse in subsequent calls to ModelStruct. This process needs to be executed only once, and its output is concurrency-safe.

RowReaders, created via StructModel.CreateReader(), are not concurrency safe and can only be used in one goroutine at a time.

GoFasterSQL supports the following member types in structures, including typedef derivatives, and pointers to any of these types. This flexibility ensures broad compatibility and ease of integration into diverse projects.
  - string, []byte, sql.RawBytes
  - bool
  - int, int8, int16, int32, int64
  - uint, uint8, uint16, uint32, uint64
  - float32, float64
  - struct (struct pointers add a very tiny bit of extra overhead)

Example Usage:

	type cardCatalogIdentifier uint
	type book struct {
		name string
		cardCatalogID cardCatalogIdentifier
		student
		l *loans
	}
	type student struct {
		currentBorrower string
		currentBorrowerId int
	}
	type loans struct {
		libraryID int8
		loanData []byte
	}

	var db sql.DB
	var b []book
	ms, err := ModelStruct(book{})
	if err != nil {
		panic(err)
	}
	msr := ms.CreateReader()
	rows, _ := db.Query("SELECT * FROM books")
	for rows.Next() {
		temp := book{l:new(loans)}
		if err := msr.ScanRows(rows, &temp); err != nil {
			panic(err)
		}
		b = append(b, temp)
	}

So:

	msr.ScanRows(rows, &temp)

is equivalent to:

	rows.Scan(&temp.name, &temp.cardCatalogID, &temp.currentBorrower, &temp.currentBorrowerId, &temp.l.libraryID, &temp.l.loanData)

and is much faster to boot!

---

Example 2: (Reading directly into multiple structs)

	type foo struct { bar, baz int }
	type moo struct { cow, calf int }
	var fooVar foo
	var mooVar moo

	if err := gofastersql.ScanRow(db.QueryRow("SELECT 2, 4, 8, 16"), &struct {*foo; *moo}{&fooVar, &mooVar}); err != nil {
		panic(err)
	}

Result:

	fooVar = {2, 4}
	mooVar = {8, 16}
*/
package gofastersql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// RowReader is used to scan sql rows into a struct. RowReader is NOT concurrency safe. It should only be used in one goroutine at a time.
type RowReader struct {
	sm          StructModel
	rawBytesArr []sql.RawBytes
	rawBytesAny []any            //This holds pointers to each member of rawBytesArr
	pointers    []unsafe.Pointer //Used to calculate struct pointer locations. Index 0 is the root struct pointer
}

// CreateReader creates a RowReader from the StructModel
func (sm StructModel) CreateReader() RowReader {
	rb := make([]sql.RawBytes, len(sm.fields))
	rba := make([]any, len(sm.fields))
	for i := range rb {
		rba[i] = &rb[i]
	}

	return RowReader{sm, rb, rba, make([]unsafe.Pointer, len(sm.pointers)+1)}
}

// ScanRows does an sql.Rows.Scan into the outPointer structure
func (r RowReader) ScanRows(rows *sql.Rows, outPointer any) error {
	//Make sure the outPointer type matches
	if err := r.checkType(outPointer); err != nil {
		return err
	}

	if err := rows.Scan(r.rawBytesAny...); err != nil {
		return err
	}
	return r.convert(outPointer)
}

// ScanRow does an sql.Row.Scan into the outPointer structure
func (r RowReader) ScanRow(row *sql.Row, outPointer any) error {
	//Make sure the outPointer type matches
	if err := r.checkType(outPointer); err != nil {
		return err
	}

	//Unfortunately, sql.Row.Scan does not support rawBytes, so we are going to have to recast the any-array to use *[]bytes instead
	bytesArrAny := make([]any, len(r.rawBytesArr))
	for i := range r.rawBytesArr {
		bytesArrAny[i] = (*[]byte)(&r.rawBytesArr[i])
	}

	if err := row.Scan(bytesArrAny...); err != nil {
		return err
	}
	return r.convert(outPointer)
}

/*
ScanRow does an sql.Row.Scan into the output variable. Output must be a pointer.

This is essentially the same as:

	ModelStruct(*output).CreateReader().ScanRow(row, output)

If you are scanning a lot of rows it is recommended to use a RowReader as it bypasses a mutex read lock, a lot of reflection manipulation, and a number of allocations.
In some rare cases this function may even be slower than the native sql.Row.Scan() method.
*/
func ScanRow(row *sql.Row, output any) error {
	//Get the StructModel
	var sm StructModel
	if reflect.TypeOf(output).Kind() != reflect.Pointer {
		return errors.New("Output must be a pointer")
	} else if _sm, err := ModelStruct(reflect.ValueOf(output).Elem().Interface()); err != nil {
		return err
	} else {
		sm = _sm
	}

	//Create the RowReader
	rb := make([]sql.RawBytes, len(sm.fields))
	rba := make([]any, len(sm.fields))
	for i := range rb {
		rba[i] = (*[]byte)(&rb[i])
	}
	r := RowReader{sm, rb, rba, make([]unsafe.Pointer, len(sm.pointers)+1)}

	if err := row.Scan(r.rawBytesAny...); err != nil {
		return err
	}
	return r.convert(output)
}

func (r RowReader) checkType(outPointer any) error {
	//Make sure the outPointer type matches
	t := reflect.TypeOf(outPointer)
	if t.Kind() != reflect.Pointer || t.Elem() != r.sm.rType {
		return fmt.Errorf("outPointer type is incorrect (%s)!=(*%s)", reflect.TypeOf(outPointer).String(), r.sm.rType.String())
	}
	return nil
}
func (r RowReader) convert(outPointer any) error {
	//Determine pointer indexes
	var errs []string
	r.pointers[0] = interface2Pointer(outPointer)
	for i, p := range r.sm.pointers {
		newPtr := unsafe.Pointer(nil)
		if r.pointers[p.parentIndex] != nil {
			newPtr = *(*unsafe.Pointer)(unsafe.Add(r.pointers[p.parentIndex], p.offset))
			if newPtr == nil {
				errs = append(errs, fmt.Sprintf("Error on %s: %s", p.name, "Pointer not initialized"))
			}
		}

		r.pointers[i+1] = newPtr
	}

	//Fill in data
	for i, sf := range r.sm.fields {
		//If parentPointer is not set then error was already issued
		parentPointer := r.pointers[sf.pointerIndex]
		if parentPointer == nil {
			continue
		}

		p := unsafe.Add(parentPointer, sf.offset)
		if sf.isPointer {
			if p = *(*unsafe.Pointer)(p); p == nil {
				errs = append(errs, fmt.Sprintf("Error on %s: %s", sf.name, "Pointer not initialized"))
				continue
			}
		}
		if err := sf.converter(r.rawBytesArr[i], p); err != nil {
			errs = append(errs, fmt.Sprintf("Error on %s: %s", sf.name, err.Error()))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}
