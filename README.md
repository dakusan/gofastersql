[![Go Report Card](https://goreportcard.com/badge/github.com/dakusan/gofastersql)](https://goreportcard.com/report/github.com/dakusan/gofastersql)
[![GoDoc](https://godoc.org/github.com/dakusan/gofastersql?status.svg)](https://godoc.org/github.com/dakusan/gofastersql)

![GoFasterSQL Logo](logo.jpg)

GoFasterSQL is a tool designed to enhance the efficiency and simplicity of scanning SQL rows into structures.

The flaw in the native library scanning process is its repetitive and time-consuming type determination for each row scan. It must match each field’s type with its native counterpart before converting the data string (byte array). Furthermore, the requirement to specify each individual field for scanning is tedious.

GoFasterSQL instead precalculates string-to-type conversions for each field, utilizing pointers to dedicated conversion functions. This approach eliminates the need for type lookups during scanning, vastly improving performance. The library offers a 2 to 2.5 times speed increase compared to native scan methods, a boost that varies with the number of items in each scan. Moreover, its automatic structure determination feature is a significant time-saver during coding.

The library’s `ModelStruct` function, upon its first invocation for a type, determines the structure of that type through recursive reflection. This structure is then cached, allowing for swift reuse in subsequent calls to `ModelStruct`. This process needs to be executed only once, and its output is concurrency-safe. The sole instance of reflection following a `ModelStruct` call occurs during the `ScanRow(s)` functions, where a verification ensures that the `outPointer` type aligns with the type specified in `ModelStruct`.

`ModelStruct` flattens all structures and records their flattened member indexes for reading into; so row scanning is by field index, not by name.

`RowReader`s, created via `StructModel.CreateReader()`, are not concurrency safe and can only be used in one goroutine at a time.

GoFasterSQL supports the following member types in structures, including typedef derivatives, pointers to any of these types, and nullable derivatives (see nulltypes package).
  - `string`, `[]byte`, `sql.RawBytes`
  - `bool`
  - `int`, `int8`, `int16`, `int32`, `int64`
  - `uint`, `uint8`, `uint16`, `uint32`, `uint64`
  - `float32`, `float64`
  - `time.Time` *(also accepts unix timestamps ; does not currently accept typedef derivatives)*
  - `struct` *(struct pointers add a very tiny bit of extra overhead)*

GoFasterSQL is available under the same style of BSD license as the Go language, which can be found in the LICENSE file.

# Example Usage
## Example #1
```go
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
```
So:<br>
`msr.ScanRows(rows, &temp)`<br>
is equivalent to:<br>
`rows.Scan(&temp.name, &temp.cardCatalogID, &temp.currentBorrower, &temp.currentBorrowerId, &temp.l.libraryID, &temp.l.loanData)`<br>
and is much faster to boot!

## Example #2
Reading a single row directly into multiple structs
```go
type foo struct { bar, baz int }
type moo struct { cow, calf nulltypes.NullInt64 }
var fooVar foo
var mooVar moo

if err := gofastersql.ScanRow(db.QueryRow("SELECT 2, 4, 8, null"), &struct {*foo; *moo}{&fooVar, &mooVar}); err != nil {
	panic(err)
}

//This is equivalent to the above statement but takes a lot more processing than ScanRow() and may be much slower
if err := gofastersql.ScanRowMulti(db.QueryRow("SELECT 2, 4, 8, null"), &fooVar, &mooVar); err != nil {
	panic(err)
}

```
Result:
```go
	fooVar = {2, 4}
	mooVar = {8, NULL}
```

> [!warning]
> If you are scanning a lot of rows it is recommended to use a `RowReader` instead of `gofastersql.ScanRow` as it bypasses a mutex read lock and a few allocations.
> In some cases `gofastersql.ScanRow` may even be slower than the native `sql.Row.Scan()` method. What speeds this library up so much is the preprocessing done before the ScanRow(s) functions are called and a lot of that is lost in `gofastersql.ScanRow` and especially in `gofastersql.ScanRowMulti`.

# Installation
GoFasterSQL is available using the standard go get command.

Install by running:

go get github.com/dakusan/gofastersql
