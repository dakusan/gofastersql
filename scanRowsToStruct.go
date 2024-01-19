//Scan sql rows into structures

/*
Package gofastersql is a tool designed to enhance the efficiency and simplicity of scanning SQL rows into structures.

The flaw in the native library scanning process is its repetitive and time-consuming type determination for each row scan. It must match each field’s type with its native counterpart before converting the data string (byte array). Furthermore, the requirement to specify each individual field for scanning is tedious.

GoFasterSQL instead precalculates string-to-type conversions for each field, utilizing pointers to dedicated conversion functions. This approach eliminates the need for type lookups during scanning, vastly improving performance. The library offers a 2 to 2.5 times speed increase compared to native scan methods (5*+ vs sqlx), a boost that varies with the number of items in each scan. Moreover, its automatic structure determination feature is a significant time-saver during coding.

The library’s `ModelStruct` function, upon its first invocation for a type, determines the structure of that type through recursive reflection. This structure is then cached, allowing for swift reuse in subsequent calls to `ModelStruct`. This process needs to be executed only once, and its output is concurrency-safe. The sole instance of reflection following a `ModelStruct` call occurs during the `ScanRow(s)` functions, where a verification ensures that the `outPointer` type aligns with the type specified in `ModelStruct`.

`ModelStruct` flattens all structures and records their flattened member indexes for reading into; so row scanning is by field index, not by name.

`RowReader`s, created via `StructModel.CreateReader()`, are not concurrency safe and can only be used in one goroutine at a time.

Both `ScanRow(s)` (plural and singular) functions only accept `sql.Rows` and not `sql.Row` due to the golang implementation limitations placed upon `sql.Row`. Non-plural `ScanRow` functions automatically call `Rows.Next()` and `Rows.Close()` like the native implementation.

GoFasterSQL supports the following member types in structures, including typedef derivatives, pointers to any of these types, and nullable derivatives (see nulltypes package).
  - string, []byte, sql.RawBytes
  - bool
  - int, int8, int16, int32, int64
  - uint, uint8, uint16, uint32, uint64
  - float32, float64
  - time.Time (also accepts unix timestamps ; does not currently accept typedef derivatives)
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

See README.md for further examples
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

// RowReader is used to scan sql rows into a struct by flattened member index. RowReader is NOT concurrency safe. It should only be used in one goroutine at a time.
type RowReader struct {
	sm          StructModel
	rawBytesArr []sql.RawBytes
	rawBytesAny []any            //This holds pointers to each member of rawBytesArr
	pointers    []unsafe.Pointer //Used to calculate struct pointer locations. Index 0 is the root struct pointer
}

// CreateReader creates a RowReader from the StructModel
func (sm StructModel) CreateReader() *RowReader {
	rb := make([]sql.RawBytes, len(sm.fields))
	rba := make([]any, len(sm.fields))
	for i := range rb {
		rba[i] = &rb[i]
	}

	return &RowReader{sm, rb, rba, make([]unsafe.Pointer, len(sm.pointers)+1)}
}

// SRErr converts a (*sql.Rows, error) tuple into a single variable to pass to ScanRow*WErr() functions
func SRErr(r *sql.Rows, err error) SRErrStruct { return SRErrStruct{r, err} }

// SRErrStruct is returned from SRErr
type SRErrStruct struct {
	r   *sql.Rows
	err error
}

// ScanRows does an sql.Rows.Scan into the outPointer structure
func (rr *RowReader) ScanRows(rows *sql.Rows, outPointer any) error {
	//Make sure the outPointer type matches
	if err := rr.checkType(outPointer); err != nil {
		return err
	}

	if err := rows.Scan(rr.rawBytesAny...); err != nil {
		return err
	}
	return rr.convert(outPointer)
}

// ScanRow does an sql.Rows.Scan into the outPointer structure for a single row.
func (rr *RowReader) ScanRow(rows *sql.Rows, outPointer any) error {
	defer runSafeCloseRow(rows)

	//Make sure the outPointer type matches
	if err := rr.checkType(outPointer); err != nil {
		return err
	}

	return rr.scanRowReal(rows, outPointer)
}

// ScanRowWErr : See rr.ScanRow and SRErr
func (rr *RowReader) ScanRowWErr(rowsErr SRErrStruct, outPointer any) error {
	if rowsErr.err != nil {
		runSafeCloseRow(rowsErr.r)
		return rowsErr.err
	}
	return rr.ScanRow(rowsErr.r, outPointer)
}

// The real scan single row functionality used by all other singular ScanRow functions
func (rr *RowReader) scanRowReal(rows *sql.Rows, outPointer any) error {
	if !runRowNext(rows) {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	if err := rows.Scan(rr.rawBytesAny...); err != nil {
		return err
	} else if err := rr.convert(outPointer); err != nil {
		return err
	}
	return runCloseRow(rows)
}

/*
ScanRow does an sql.Rows.Scan into the outPointer variable for a single row.

This is essentially the same as:

	ModelStruct(*outPointer).CreateReader().ScanRow(row, outPointer)

If you are scanning a lot of rows it is recommended to use a RowReader as it bypasses a mutex read lock and a few allocations.
In some cases this may even be slower than the native sql.Rows.Scan() method. What speeds this library up so much is the preprocessing done before the ScanRow(s) functions are called and a lot of that is lost in gofastersql.ScanRow() and especially in gofastersql.ScanRowMulti().
*/
func ScanRow[T any](rows *sql.Rows, outPointer *T) error {
	defer runSafeCloseRow(rows)
	if sm, err := ModelStructType(reflect.TypeOf(outPointer).Elem()); err != nil {
		return err
	} else {
		return sm.CreateReader().scanRowReal(rows, outPointer)
	}
}

// ScanRowWErr : See ScanRow and SRErr
func ScanRowWErr[T any](rowsErr SRErrStruct, outPointer *T) error {
	if rowsErr.err != nil {
		runSafeCloseRow(rowsErr.r)
		return rowsErr.err
	}
	return ScanRow[T](rowsErr.r, outPointer)
}

/*
ScanRowMulti does an sql.Rows.Scan into the outPointer variables for a single row. Output variables can be scalar types instead of structs. Output variables must be pointers.

This is essentially the same as:

	ScanRow(row, &struct{*outputType1; *outputType2; ...}{&output1, &output2, ...})

If you are scanning a lot of rows it is recommended to use a RowReader as it bypasses a mutex read lock and a few allocations.
This takes a lot more processing than ScanRow() and may be much slower.
*/
func ScanRowMulti(rows *sql.Rows, output ...any) error {
	defer runSafeCloseRow(rows)
	if sm, outArr, err := getMultipleStructsAsStructModel(output); err != nil {
		return err
	} else {
		return sm.CreateReader().scanRowReal(rows, &outArr[0])
	}
}

// ScanRowMultiWErr : See ScanRowMulti and SRErr
func ScanRowMultiWErr(rowsErr SRErrStruct, output ...any) error {
	if rowsErr.err != nil {
		runSafeCloseRow(rowsErr.r)
		return rowsErr.err
	}
	return ScanRowMulti(rowsErr.r, output...)
}

// Make sure the outPointer type matches
func (rr *RowReader) checkType(outPointer any) error {
	t := reflect.TypeOf(outPointer)
	if t.Kind() != reflect.Pointer || t.Elem() != rr.sm.rType {
		return fmt.Errorf("outPointer type is incorrect (%s)!=(*%s)", reflect.TypeOf(outPointer).String(), rr.sm.rType.String())
	}
	return nil
}

// Convert the read sql data into the output structure(s)
func (rr *RowReader) convert(outPointer any) error {
	//Determine pointer indexes
	var errs []string
	r := *rr //Store locally as we no longer need extensions at this point
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
		if err := sf.converter(r.rawBytesArr[i], upt(p)); err != nil {
			errs = append(errs, fmt.Sprintf("Error on %s: %s", sf.name, err.Error()))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

//------------Row Close/Next functions overwritten during benchmarks------------

func safeRowClose(rows *sql.Rows) {
	if rows != nil {
		_ = rows.Close()
	}
}
func rowClose(rows *sql.Rows) error {
	return rows.Close()
}
func rowNext(rows *sql.Rows) bool {
	return rows.Next()
}

var runSafeCloseRow = safeRowClose
var runCloseRow = rowClose
var runRowNext = rowNext
