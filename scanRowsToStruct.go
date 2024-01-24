//Scan sql rows into structures

/*
Package gofastersql is a tool designed to enhance the efficiency and simplicity of scanning SQL rows into structures.

The flaw in the native library scanning process is its repetitive and time-consuming type determination for each row scan. It must match each field’s type with its native counterpart before converting the data string (byte array). Furthermore, the requirement to specify each individual field for scanning is tedious.

GoFasterSQL instead precalculates string-to-type conversions for each field, utilizing pointers to dedicated conversion functions. This approach eliminates the need for type lookups during scanning, vastly improving performance. The library offers a 2 to 2.5 times speed increase compared to native scan methods (5*+ vs sqlx), a boost that varies with the number of items in each scan. Moreover, its automatic structure determination feature is a significant time-saver during coding.

The library’s ModelStruct function, upon its first invocation for a list of types, determines the structure of those types through recursive reflection. These structures are then cached, allowing for swift reuse in subsequent calls to ModelStruct. This process needs to be executed only once, and its output is concurrency-safe.

ModelStruct flattens all structures and records their flattened member indexes for reading into; so row scanning is by field index, not by name. To match by name, use a RowReaderNamed via StructModel.CreateReaderNamed().

RowReaders, created via StructModel.CreateReader(), are not concurrency safe and can only be used in one goroutine at a time.

Both ScanRow(s) (plural and singular) functions only accept sql.Rows and not sql.Row due to the golang implementation limitations placed upon sql.Row. Non-plural ScanRow functions automatically call Rows.Next() and Rows.Close() like the native implementation.

The SRErr() and *.ScanRowWErr*() helper functions exist to help emulate sql.Row.Scan error handling functionality.

GoFasterSQL supports the following types, including: typedef derivatives, nested use in structures (including pointers to the types), and nullable derivatives (see nulltypes package).
  - string, []byte, sql.RawBytes (RawBytes converted to []byte for singular RowScan functions)
  - bool
  - int, int8, int16, int32, int64
  - uint, uint8, uint16, uint32, uint64
  - float32, float64
  - time.Time (also accepts unix timestamps ; does not currently accept typedef derivatives)
  - struct

Optimization Information:
  - The sole instance of reflection following a ModelStruct call occurs during the ScanRow(s) functions, where a verification ensures that the outPointers types align with the types specified in ModelStruct (the *NC versions [DoScan(runCheck=false)] skip this check).
  - Creating a StructModel from a single structure requires much less overhead than the alternatives.
  - Nested struct pointers add a very tiny bit of extra overhead over nested non-pointers.
  - See https://www.github.com/dakusan/gofastersql/blob/master/benchmarks/benchmarks.png for benchmarks.

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

	var db *sql.DB
	var b []book
	ms, err := gofastersql.ModelStruct(book{})
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
	_ = rows.Close()

So:

	msr.ScanRows(rows, &temp)

is equivalent to:

	rows.Scan(&temp.name, &temp.cardCatalogID, &temp.currentBorrower, &temp.currentBorrowerId, &temp.l.libraryID, &temp.l.loanData)

and is much faster to boot!

It is also equivalent to (but a little faster than):

	ModelStruct(...).CreateReader().ScanRows(rows, &temp.name, &temp.cardCatalogID, &temp.student, temp.l)

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
	rrType      rowReaderType
}

// rowReaderType specifies extensions onto RowReader
type rowReaderType uint8

const (
	rrtStandard rowReaderType = 0               //Standard RowReader
	rrtNamed    rowReaderType = 1 << (iota - 1) //RowReaderNamed (matches against select query column names instead of indexes)
)

// CreateReader creates a RowReader from the StructModel
func (sm StructModel) CreateReader() *RowReader {
	rb := make([]sql.RawBytes, len(sm.fields))
	rba := make([]any, len(sm.fields))
	for i := range rb {
		rba[i] = &rb[i]
	}

	return &RowReader{sm, rb, rba, make([]unsafe.Pointer, len(sm.pointers)+1), rrtStandard}
}

// SRErr converts a (*sql.Rows, error) tuple into a single variable to pass to *.ScanRowWErr*() functions
func SRErr(r *sql.Rows, err error) SRErrStruct { return SRErrStruct{r, err} }

// SRErrStruct is returned from SRErr
type SRErrStruct struct {
	r   *sql.Rows
	err error
}

/*
DoScan is the primary row scanning function that all other row scanning functions call. It does an sql.Rows.Scan() into the outPointers variables.

  - err: If set then the only actions are that rows is closed and the error is returned
  - runCheck: If true then an error is returned if outPointers types do not match the RowReader’s input types. If false then the types are not checked. A check is always performed to make sure the correct number of variables were passed.
  - isSingleRow: If true then rows.Next() is called before the scan and rows.Close() is always called before the function ends
*/
func (rr *RowReader) DoScan(rows *sql.Rows, outPointers []any, err error, runCheck, isSingleRow bool) error {
	//Pass through error
	if err != nil {
		runSafeCloseRow(rows)
		return err
	}

	//If a single row make sure rows.Close() is called
	if isSingleRow {
		defer runSafeCloseRow(rows)
	}

	//Make sure the outPointers types match
	if len(outPointers) != len(rr.sm.rTypes) {
		return fmt.Errorf("outPointers is incorrect length %d!=%d", len(outPointers), len(rr.sm.rTypes))
	}
	if runCheck {
		for i, v := range outPointers {
			t := reflect.TypeOf(v)
			if t.Kind() != reflect.Pointer || t.Elem() != rr.sm.rTypes[i] {
				return fmt.Errorf("outPointers[%d] type is incorrect (%s)!=(*%s)", i, t.String(), rr.sm.rTypes[i].String())
			}
		}
	}

	//If a single row, make sure to open it
	if isSingleRow && !runRowNext(rows) {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	//Nil out all values in rawBytes in case sql attempts to read a non []byte into them (security vulnerability bug in golang sql code)
	for i := range rr.rawBytesArr {
		rr.rawBytesArr[i] = nil
	}

	//Handle extensions
	if rr.rrType != rrtStandard {
		rrn := (*RowReaderNamed)(unsafe.Pointer(rr))
		if !rrn.hasAlreadyMatchedCols || rrn.hasError {
			if err := rrn.initNamed(rows); err != nil {
				return err
			}
		}
	}

	//Run the scan and conversion
	if err := rows.Scan(rr.rawBytesAny...); err != nil {
		return err
	} else if err := rr.convert(outPointers, isSingleRow); err != nil {
		return err
	}

	//If not a single row then nothing more to do
	if !isSingleRow {
		return nil
	}

	//Finish closing a single row
	return runCloseRow(rows)
}

// ScanRows does an sql.Rows.Scan into the outPointers variables.
//
// Just runs: rr.DoScan(rows, outPointers, nil, true, false)
func (rr *RowReader) ScanRows(rows *sql.Rows, outPointers ...any) error {
	return rr.DoScan(rows, outPointers, nil, true, false)
}

// ScanRowsNC does an sql.Rows.Scan into the outPointers variables. No type checks are done on outPointers.
//
// Just runs: rr.DoScan(rows, outPointers, nil, false, false)
func (rr *RowReader) ScanRowsNC(rows *sql.Rows, outPointers ...any) error {
	return rr.DoScan(rows, outPointers, nil, false, false)
}

// ScanRow does an sql.Rows.Scan into the outPointers variables for a single row.
//
// Just runs: rr.DoScan(rows, outPointers, nil, true, true)
func (rr *RowReader) ScanRow(rows *sql.Rows, outPointers ...any) error {
	return rr.DoScan(rows, outPointers, nil, true, true)
}

// ScanRowNC does an sql.Rows.Scan into the outPointers variables for a single row. No type checks are done on outPointers.
//
// Just runs: rr.DoScan(rows, outPointers, nil, false, true)
func (rr *RowReader) ScanRowNC(rows *sql.Rows, outPointers ...any) error {
	return rr.DoScan(rows, outPointers, nil, false, true)
}

// ScanRowWErr : See rr.ScanRow and SRErr
//
// Just runs: rr.DoScan(rowsErr.r, outPointers, rowsErr.err, true, true)
func (rr *RowReader) ScanRowWErr(rowsErr SRErrStruct, outPointers ...any) error {
	return rr.DoScan(rowsErr.r, outPointers, rowsErr.err, true, true)
}

// ScanRowWErrNC : See rr.ScanRowNC and SRErr
//
// Just runs: rr.DoScan(rowsErr.r, outPointers, rowsErr.err, false, true)
func (rr *RowReader) ScanRowWErrNC(rowsErr SRErrStruct, outPointers ...any) error {
	return rr.DoScan(rowsErr.r, outPointers, rowsErr.err, false, true)
}

/*
ScanRow does an sql.Rows.Scan into the outPointers variables for a single row. Output variables must be pointers.

This is essentially the same as:

	ModelStruct(outPointers...).CreateReader().ScanRow(row, outPointers...)

If you are scanning a lot of rows it is recommended to use a RowReader as it bypasses mutex read locks and a few allocations.
In some cases this may even be slower than the native sql.Rows.Scan() method. What speeds this library up so much is the preprocessing done before the ScanRow(s) functions are called and a lot of that is lost in gofastersql.ScanRow().
*/
func ScanRow(rows *sql.Rows, outPointers ...any) error {
	if sm, err := scanRowModelStruct(rows, outPointers); err != nil {
		return err
	} else {
		return sm.CreateReader().DoScan(rows, outPointers, nil, false, true)
	}
}

// Make sure all variables are pointers
func scanRowModelStruct(rows *sql.Rows, outPointers []any) (*StructModel, error) {
	for i, v := range outPointers {
		if reflect.TypeOf(v).Kind() != reflect.Pointer {
			runSafeCloseRow(rows)
			return nil, fmt.Errorf("Parameter #%d is not a pointer", i+1)
		}
	}

	sm, err := ModelStruct(outPointers...)
	if err != nil {
		runSafeCloseRow(rows)
	}
	return &sm, err
}

// ScanRowWErr : See ScanRow and SRErr
func ScanRowWErr(rowsErr SRErrStruct, outPointers ...any) error {
	if rowsErr.err != nil {
		runSafeCloseRow(rowsErr.r)
		return rowsErr.err
	}
	return ScanRow(rowsErr.r, outPointers...)
}

// Convert the read sql data into the output variables
func (rr *RowReader) convert(outPointers []any, isSingleRow bool) error {
	//Get the outputPointer
	r := *rr //Store locally as we no longer need extensions at this point
	var outPointer unsafe.Pointer
	if rr.sm.isSimple {
		outPointer = interface2Pointer(outPointers[0])
	} else {
		//Create an array that holds all the pointers
		outArr := make([]unsafe.Pointer, len(outPointers))
		for i, v := range outPointers {
			outArr[i] = interface2Pointer(v)
		}
		outPointer = unsafe.Pointer(&outArr[0])
	}

	//Determine pointer indexes
	var errs []string
	r.pointers[0] = outPointer
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

		//Get pointer to the output data
		p := unsafe.Add(parentPointer, sf.offset)
		if sf.isPointer {
			if p = *(*unsafe.Pointer)(p); p == nil {
				errs = append(errs, fmt.Sprintf("Error on %s: %s", sf.name, "Pointer not initialized"))
				continue
			}
		}

		//If rawBytes and isSingleRow then change output func to use a byte array instead
		cFunc := sf.converter
		if isSingleRow && (sf.flags&sffIsRawBytes != 0) {
			cFunc = cond(sf.flags&sffIsNullable != 0, cvNBA, convByteArray)
		}

		//Run the conversion function
		if err := cFunc(r.rawBytesArr[i], upt(p)); err != nil {
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
