[![Go Report Card](https://goreportcard.com/badge/github.com/dakusan/gofastersql)](https://goreportcard.com/report/github.com/dakusan/gofastersql)
[![GoDoc](https://godoc.org/github.com/dakusan/gofastersql?status.svg)](https://godoc.org/github.com/dakusan/gofastersql)

![GoFasterSQL Logo](logo.jpg)


GoFasterSQL is a tool designed to significantly enhance the efficiency and simplicity of scanning SQL rows into structures.

Unlike the native MySQL package in GoLang, which is often slow and unwieldy, GoFasterSQL streamlines the process with remarkable speed and ease.

A critical flaw in the native scanning process is its repetitive type determination for each row scan. It laboriously matches each field's type with its native counterpart before converting the data string (byte array), a process that is both time-consuming and cumbersome. Furthermore, the requirement to specify each field for scanning seems unnecessarily tedious.

In contrast, GoFasterSQL precalculates string-to-type conversions for each field, utilizing pointers to dedicated conversion functions. This approach eliminates the need for type lookups during scanning, vastly improving performance. The library offers a 2 to 2.5 times speed increase compared to native scan methods, a boost that varies with the number of items in each scan. Moreover, its automatic structure determination feature is a significant time-saver during coding.

The library’s `ModelStruct` function, upon its first invocation for a type, determines the structure of that type through recursive reflection. This structure is then cached, allowing for swift and efficient reuse in subsequent calls to `ModelStruct`. This process needs to be executed only once, and its output is concurrency-safe.

`RowReader`s, created via `StructModel.CreateReader()`, are not concurrency safe and can only be used in one goroutine at a time.

GoFasterSQL supports the following member types in structures, including typedef derivatives, and pointers to any of these types. This flexibility ensures broad compatibility and ease of integration into diverse projects.
  - `string`, `[]byte`, `sql.RawBytes`
  - `bool`
  - `int`, `int8`, `int16`, `int32`, `int64`
  - `uint`, `uint8`, `uint16`, `uint32`, `uint64`
  - `float32`, `float64`
  - `struct` **(cannot be a pointer)**

Example Usage:
```go
type cardCatalogIdentifier uint
type book struct {
	name string
	cardCatalogID cardCatalogIdentifier
	student
	l loans
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
	var temp book
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