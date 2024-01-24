//go:build ignore

//This file is ignored so sqlx import is not attempted
//A few modifications need to be made elsewhere for all tests to run together too (see customTime)

package test

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"testing"
	"time"
)

var sqlxB *testing.B

func sqlxErr[valT any](val valT, err error) valT {
	if err != nil {
		sqlxB.Fatal(err)
	}
	return val
}

func BenchmarkRowReader_ScanRows_SQLX(b *testing.B) {
	sqlxB = b
	db := sqlxErr(sqlx.Connect("mysql", SQLConnectString))
	tx := sqlxErr(db.Beginx())
	sqlxErr(tx.Exec(`CREATE TEMPORARY TABLE goTest1 (i int) ENGINE=MEMORY`))
	sqlxErr(tx.Exec(`INSERT INTO goTest1 VALUES (0)`))
	rows := sqlxErr(tx.Queryx(`
SELECT
	/*P1 and TestStruct2*/
	CONCAT('P1-', i) as "p1",
	2+i AS "u", (1<<8)-1+i AS "u8", (1<<16)-1+i AS "u16", (1<<32)-1+i AS "u32", 0xFFFFFFFFFFFFFFFF+0 AS "u64", /*Set 1*/
	2+i AS "i", (1<<7)-1+i AS "i8", (1<<15)-1+i AS "i16", (1<<31)-1+i AS "i32", (1<<63)-1+i AS "i64",          /*Set 2*/
	1.1+i AS "f32", 5.5+i AS "f64", CONCAT('str-', i) AS "s", CONCAT('ba-', i) AS "ba", CONCAT('rb-', i) AS "rb", i AS "b",

	/*P2 and TS3*/
	5+i AS "p2",
	20+i  AS "ts3.ts4.u", (1<<8)-2+i  AS "ts3.ts4.u8", (1<<16)-2+i  AS "ts3.ts4.u16", (1<<32)-2+i  AS "ts3.ts4.u32", 0xFFFFFFFFFFFFFFFF+0 AS "ts3.ts4.u64",/*Set 3*/
	20+i  AS "ts3.i", CAST(1<<7 AS INT)*-1-i  AS "ts3.i8", CAST(1<<15 AS INT)*-1-i  AS "ts3.i16", CAST(1<<31 AS INT)*-1-i  AS "ts3.i32", CAST((1<<62)-1 AS SIGNED)*-2-2-0  AS "ts3.i64", /*Set 4*/
	11.11+i  AS "ts3.f32", 12.12+i  AS "ts3.f64", CONCAT('strP-', i)  AS "ts3.ts6.ts7.s", CONCAT('baP-', i)  AS "ts3.ts6.ba", CONCAT('rbP-', i)  AS "ts3.rb", i AS "ts3.b",

	/*TS9*/
	CONCAT('P3-', i) AS "ts9.p3", null AS "ts9.t1", null AS "ts9.t2"
FROM goTest1
`))
	rows.Next()
	b.ResetTimer()

	//Run the benchmark tests
	for i := 0; i < b.N; i++ {
		ts1 := setupTestStruct()
		for n := 0; n < NumBenchmarkScanRowsPasses; n++ {
			sqlxErr(0, rows.StructScan(&ts1))
		}
	}
}

type customTime time.Time //Replace time.Time structs in TestStruct9 with this

func (c customTime) Scan(src any) error {
	return nil
}
