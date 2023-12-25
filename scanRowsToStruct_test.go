package gofastersql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/dakusan/gofastersql/nulltypes"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"testing"
	"time"
)

const (
	SQLConnectString           = "USERNAME@tcp(HOSTNAME:PORT)/DBNAME"
	NumBenchmarkScanRowsPasses = 100_000
)

//-----------Test structures containing all (non-null) readable types-----------

type testStruct1 struct { //0@0
	P1          string       //0
	TestStruct2              //16
	P2          *int         //152
	TS3         TestStruct3  //160
	TS9         *TestStruct9 //248
}
type TestStruct2 struct { //0@16
	U   uint         //16
	U8  uint8        //24
	U16 uint16       //26
	U32 uint32       //28
	U64 uint64       //32
	I   int          //40
	I8  int8         //48
	I16 int16        //50
	I32 int32        //52
	I64 int64        //56
	F32 float32      //64
	F64 *float64     //72
	S   string       //80
	BA  []byte       //96
	RB  sql.RawBytes //120
	B   bool         //144
}
type TestStruct3 struct { //0@160
	TS4         *TestStruct4  //160
	TestStruct5               //168
	F32         *float32      //208
	F64         float64       //216
	TS6         *TestStruct6  //224
	RB          *sql.RawBytes //232
	B           *bool         //240
}
type TestStruct4 struct { //1@0
	U   *uint   //0
	U8  *uint8  //8
	U16 *uint16 //16
	U32 *uint32 //24
	U64 *uint64 //32
}
type TestStruct5 struct { //0@168
	I   *int   //168
	I8  *int8  //176
	I16 *int16 //184
	I32 *int32 //192
	I64 *int64 //200
}
type TestStruct6 struct { //2@0
	TS7         *TestStruct7 //0
	TestStruct8              //8
}
type TestStruct7 struct { //3@0
	S *string //0
}
type TestStruct8 struct { //2@8
	BA *[]byte //8
}
type TestStruct9 struct { //4@0
	P3 []byte     //0
	T1 time.Time  //24
	T2 *time.Time //48
}

//-----------------Database and struct setups for test functions----------------

var sqlConn *sql.DB

func setupSQLConnect() (*sql.Tx, error) {
	//Connect to the database
	if sqlConn == nil {
		if db, err := sql.Open("mysql", SQLConnectString); err != nil {
			return nil, err
		} else if err := db.Ping(); err != nil {
			return nil, err
		} else if _, err := db.Exec(`SET time_zone = ?`, `UTC`); err != nil {
			return nil, err
		} else {
			sqlConn = db
		}
	}

	//Create a transaction
	if _tx, err := sqlConn.Begin(); err != nil {
		return nil, err
	} else {
		return _tx, nil
	}
}

func getTestQueryString(noTimeTesting bool) string {
	//Select values for all the columns
	//Return #1 will have max-values for sets #1,#2 and min-value for set #4
	//Return #2 will have sets #1,#2,#4 overflow (though some of the 64 bit ones cant overflow in SQL for testing)
	return `
SELECT
	/*P1 and TestStruct2*/
	CONCAT('P1-', i),
	2+i, (1<<8)-1+i, (1<<16)-1+i, (1<<32)-1+i, 0xFFFFFFFFFFFFFFFF+0, /*Set 1*/
	2+i, (1<<7)-1+i, (1<<15)-1+i, (1<<31)-1+i, (1<<63)-1+i,          /*Set 2*/
	1.1+i, 5.5+i, CONCAT('str-', i), CONCAT('ba-', i), CONCAT('rb-', i), i,

	/*P2 and TS3*/
	5+i,
	20+i, (1<<8)-2+i, (1<<16)-2+i, (1<<32)-2+i, 0xFFFFFFFFFFFFFFFF+0,/*Set 3*/
	20+i, CAST(1<<7 AS INT)*-1-i, CAST(1<<15 AS INT)*-1-i, CAST(1<<31 AS INT)*-1-i, CAST((1<<62)-1 AS SIGNED)*-2-2-0, /*Set 4*/
	11.11+i, 12.12+i, CONCAT('strP-', i), CONCAT('baP-', i), CONCAT('rbP-', i), i,

	/*TS9*/
	CONCAT('P3-', i), ` + cond(noTimeTesting, `null, null`, `CAST('2001-02-03 05:06:07.21' AS DATETIME(3)), UNIX_TIMESTAMP('2005-08-09 15:16:17.62')`) + `
FROM goTest
	`
}

func getExpectedTestQueryResult() string {
	//goland:noinspection SpellCheckingInspection
	return `{"P1":"P1-0","U":2,"U8":255,"U16":65535,"U32":4294967295,"U64":18446744073709551615,"I":2,"I8":127,"I16":32767,"I32":2147483647,"I64":9223372036854775807,"F32":1.1,"F64":5.5,"S":"str-0","BA":"YmEtMA==","RB":"cmItMA==","B":false,"P2":5,"TS3":{"TS4":{"U":20,"U8":254,"U16":65534,"U32":4294967294,"U64":18446744073709551615},"I":20,"I8":-128,"I16":-32768,"I32":-2147483648,"I64":-9223372036854775808,"F32":11.11,"F64":12.12,"TS6":{"TS7":{"S":"strP-0"},"BA":"YmFQLTA="},"RB":"cmJQLTA=","B":false},"TS9":{"P3":"UDMtMA==","T1":"2001-02-03T05:06:07.21Z","T2":"2005-08-09T15:16:17.62Z"}}`
}

func setupTestQuery(
	usePreparedQuery bool, //If true a prepared statement is used instead of a normal query (used for benchmarking)
	noTimeTesting bool, //time.Time testing is only done for the test runs and not the bench runs since MySQL native lib support seems to not work
) (*sql.Tx, *sql.Rows, error) {
	//Connect to the database and create a transaction
	var tx *sql.Tx
	if _tx, err := setupSQLConnect(); err != nil {
		return nil, nil, err
	} else {
		tx = _tx
	}

	//Create a temporary table and fill it with values 0, 1, 0
	if _, err := tx.Exec(`CREATE TEMPORARY TABLE goTest (i int) ENGINE=MEMORY`); err != nil {
		return tx, nil, err
	} else if _, err := tx.Exec(`INSERT INTO goTest VALUES (0), (1), (0);`); err != nil {
		return tx, nil, err
	}

	if !usePreparedQuery {
		rows, err := tx.Query(getTestQueryString(noTimeTesting))
		return tx, rows, err
	} else if stmt, err := tx.Prepare(getTestQueryString(noTimeTesting)); err != nil {
		return tx, nil, err
	} else {
		rows, err := stmt.Query()
		return tx, rows, err
	}
}

func setupTestStruct() testStruct1 {
	//Create a structure to receive all the valid values with all types represented
	return testStruct1{
		P2: new(int),
		TestStruct2: TestStruct2{
			F64: new(float64),
		},
		TS3: TestStruct3{
			TS4: &TestStruct4{
				U:   new(uint),
				U8:  new(uint8),
				U16: new(uint16),
				U32: new(uint32),
				U64: new(uint64),
			},
			TestStruct5: TestStruct5{
				I:   new(int),
				I8:  new(int8),
				I16: new(int16),
				I32: new(int32),
				I64: new(int64),
			},
			F32: new(float32),
			TS6: &TestStruct6{
				TS7: &TestStruct7{
					S: new(string),
				},
				TestStruct8: TestStruct8{
					BA: new([]byte),
				},
			},
			RB: new(sql.RawBytes),
			B:  new(bool),
		},
		TS9: &TestStruct9{
			T2: new(time.Time),
		},
	}
}

func rollbackTransactionAndRows(tx *sql.Tx, rows *sql.Rows) {
	if rows != nil {
		_ = rows.Close()
	}
	if tx != nil {
		_, _ = tx.Exec(`DROP TEMPORARY TABLE goTest`)
		_ = tx.Rollback()
	}
}

//--------------------------------Test functions--------------------------------

func TestAllTypes(t *testing.T) {
	//Init test data
	var tx *sql.Tx
	var rows *sql.Rows
	if _tx, _rows, err := setupTestQuery(false, false); err != nil {
		rollbackTransactionAndRows(_tx, _rows)
		t.Fatal(err)
	} else {
		tx, rows = _tx, _rows
	}
	defer rollbackTransactionAndRows(tx, rows)
	ts1 := setupTestStruct()

	//Prepare structures for the tests
	var rr RowReader
	var sm StructModel
	if _sm, err := ModelStruct(ts1); err != nil {
		t.Fatal(err)
	} else {
		sm = _sm
		rr = sm.CreateReader()
	}

	//Pass #1: Read into the structure and make sure it comes out correct
	t.Run("Read into structure", func(t *testing.T) {
		rows.Next()
		if err := rr.ScanRows(rows, &ts1); err != nil {
			t.Fatal(err)
		} else if str, err := json.Marshal(ts1); err != nil {
			t.Fatal(err)
		} else if string(str) != getExpectedTestQueryResult() {
			t.Fatal("Structure json marshal did not match: " + string(str))
		}
	})

	//Pass #2: Check for the expected overflow errors
	t.Run("Expected overflow errors", func(t *testing.T) {
		rows.Next()
		if err := rr.ScanRows(rows, &ts1); err == nil {
			t.Fatal("Expected errors not found")
		} else if err.Error() != strings.Join([]string{
			`Error on TestStruct2.U8: strconv.ParseUint: parsing "256": value out of range`,
			`Error on TestStruct2.U16: strconv.ParseUint: parsing "65536": value out of range`,
			`Error on TestStruct2.U32: strconv.ParseUint: parsing "4294967296": value out of range`,
			`Error on TestStruct2.I8: strconv.ParseInt: parsing "128": value out of range`,
			`Error on TestStruct2.I16: strconv.ParseInt: parsing "32768": value out of range`,
			`Error on TestStruct2.I32: strconv.ParseInt: parsing "2147483648": value out of range`,
			`Error on TestStruct2.I64: strconv.ParseInt: parsing "9223372036854775808": value out of range`,
			`Error on TS3.TestStruct5.I8: strconv.ParseInt: parsing "-129": value out of range`,
			`Error on TS3.TestStruct5.I16: strconv.ParseInt: parsing "-32769": value out of range`,
			`Error on TS3.TestStruct5.I32: strconv.ParseInt: parsing "-2147483649": value out of range`,
		}, "\n") {
			t.Fatal("Expected errors not correct:\n" + err.Error())
		}
	})

	//Make sure we get back the same struct on a second attempt
	ts2 := testStruct1{}
	t.Run("Struct model equivalency", func(t *testing.T) {
		if sm2, err := ModelStruct(ts2); err != nil {
			t.Fatal(err)
		} else if !sm2.Equals(sm) {
			t.Fatal("Struct models are not for the same struct")
		}
	})

	//Pass #3: Check for the expected nil pointer errors
	t.Run("Expected nil pointer errors", func(t *testing.T) {
		rows.Next()
		if err := rr.ScanRows(rows, &ts2); err == nil {
			t.Fatal("Expected errors #2 not found")
		} else if err.Error() != strings.Join([]string{
			`Error on TS3.TS4: Pointer not initialized`,
			`Error on TS3.TS6: Pointer not initialized`,
			`Error on TS9: Pointer not initialized`,
			`Error on TestStruct2.F64: Pointer not initialized`,
			`Error on P2: Pointer not initialized`,
			`Error on TS3.TestStruct5.I: Pointer not initialized`,
			`Error on TS3.TestStruct5.I8: Pointer not initialized`,
			`Error on TS3.TestStruct5.I16: Pointer not initialized`,
			`Error on TS3.TestStruct5.I32: Pointer not initialized`,
			`Error on TS3.TestStruct5.I64: Pointer not initialized`,
			`Error on TS3.F32: Pointer not initialized`,
			`Error on TS3.RB: Pointer not initialized`,
			`Error on TS3.B: Pointer not initialized`,
		}, "\n") {
			t.Fatal("Expected errors #2 not correct:\n" + err.Error())
		}
	})
	_ = rows.Close()

	testReadRow(t, tx)
}
func testReadRow(t *testing.T, tx *sql.Tx) {
	//Test ReadRow
	t.Run("ReadRow", func(t *testing.T) {
		type smallTest struct{ a, b int }
		var st smallTest
		if err := ScanRow(tx.QueryRow("SELECT i, i*3 FROM goTest LIMIT 1, 1"), &st); err != nil {
			t.Fatal(err)
		} else if st.a != 1 || st.b != 3 {
			t.Fatal(fmt.Sprintf("smallTest is not the expected value ({%d,%d}!={%d,%d})", st.a, st.b, 1, 3))
		}
	})

	t.Run("ReadRowMulti 1 item", func(t *testing.T) {
		var a int
		if err := ScanRowMulti(tx.QueryRow(`SELECT 6`), &a); err != nil {
			t.Fatal(err)
		} else if a != 6 {
			t.Fatal(fmt.Sprintf("%d!=%d", a, 6))
		}
	})

	//Test ReadRowMulti
	t.Run("ReadRowMulti", func(t *testing.T) {
		ts1 := setupTestStruct()
		if err := ScanRowMulti(tx.QueryRow(getTestQueryString(false)), &ts1.P1, &ts1.TestStruct2, ts1.P2, &ts1.TS3, ts1.TS9); err != nil {
			t.Fatal(err)
		} else if str, err := json.Marshal(ts1); err != nil {
			t.Fatal(err)
		} else if string(str) != getExpectedTestQueryResult() {
			t.Fatal("Structure json marshal for ReadRowMulti did not match: " + string(str))
		}
	})
}

func TestNulls(t *testing.T) {
	//Connect to the database and create a transaction
	var tx *sql.Tx
	if _tx, err := setupSQLConnect(); err != nil {
		t.Fatal(err)
	} else {
		tx = _tx
	}
	defer rollbackTransactionAndRows(tx, nil)

	//Create a temporary table and fill it with values (5, NULL)
	if _, err := tx.Exec(`CREATE TEMPORARY TABLE goTest (i1 int NULL, i2 int NULL) ENGINE=MEMORY`); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Exec(`INSERT INTO goTest VALUES (5, NULL);`); err != nil {
		t.Fatal(err)
	}

	//Run test for putting null onto non-null scalar types
	t.Run("Non-null scalar with null values", func(t *testing.T) {
		ts2 := TestStruct2{F64: new(float64)}
		if err := ScanRow(tx.QueryRow(`SELECT i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2, i2 FROM goTest`), &ts2); err != nil {
			t.Fatal(err)
		} else if str, err := json.Marshal(ts2); err != nil {
			t.Fatal(err)
		} else if string(str) != `{"U":0,"U8":0,"U16":0,"U32":0,"U64":0,"I":0,"I8":0,"I16":0,"I32":0,"I64":0,"F32":0,"F64":0,"S":"","BA":"","RB":null,"B":false}` {
			t.Fatal("Nulled structure json marshal did not match: " + string(str))
		}
	})

	//Run test for nullable scalar types
	t.Run("Null scalars", func(t *testing.T) {
		type TestStructNull struct {
			U8  nulltypes.NullUint8
			U16 nulltypes.NullUint16
			U32 nulltypes.NullUint32
			U64 nulltypes.NullUint64
			I8  nulltypes.NullInt8
			I16 nulltypes.NullInt16
			I32 nulltypes.NullInt32
			I64 nulltypes.NullInt64
			F32 nulltypes.NullFloat32
			F64 *nulltypes.NullFloat64
			S   nulltypes.NullString
			BA  nulltypes.NullByteArray
			RB  nulltypes.NullRawBytes
			B   nulltypes.NullBool
			T   nulltypes.NullTime
		}
		tsn := TestStructNull{F64: new(nulltypes.NullFloat64)}
		tsnToString := func() string {
			list := []any{tsn.U8, tsn.U16, tsn.U32, tsn.U64, tsn.I8, tsn.I16, tsn.I32, tsn.I64, tsn.F32, tsn.F64, tsn.S, tsn.BA, tsn.RB, tsn.B, tsn.T}
			s := make([]string, len(list))
			for i, v := range list {
				s[i] = (v).(fmt.Stringer).String()
			}
			return strings.Join(s, ",")
		}

		if err := ScanRow(tx.QueryRow(`SELECT i1+1, i2, i1+2, i2, i1+3, i2, i1+4, i2, i1+5, i2, i1+6, i2, i1+7, i2, '2001-02-03 05:06:07.21' FROM goTest`), &tsn); err != nil {
			t.Fatal(err)
		} else if tsnToString() != `6,NULL,7,NULL,8,NULL,9,NULL,10,NULL,11,NULL,12,NULL,2001-02-03 05:06:07.21` {
			t.Fatal("Nulled scalar marshal did not match: " + tsnToString())
		}

		if err := ScanRow(tx.QueryRow(`SELECT i2, i1+11, i2, i1+12, i2, i1+13, i2, i1+14, i2, i1+15, i2, i1+16, i2, i1+17, i2 FROM goTest`), &tsn); err != nil {
			t.Fatal(err)
		} else if tsnToString() != `NULL,16,NULL,17,NULL,18,NULL,19,NULL,20,NULL,21,NULL,false,NULL` {
			t.Fatal("Nulled scalar marshal #2 did not match: " + tsnToString())
		}
	})
}

//------------------------------Benchmark ScanRows------------------------------

func BenchmarkRowReader_ScanRows_Faster(b *testing.B) {
	//Init test data
	var rows *sql.Rows
	if _tx, _rows, err := setupTestQuery(false, true); err != nil {
		rollbackTransactionAndRows(_tx, _rows)
		b.Fatal(err)
	} else {
		rows = _rows
		defer rollbackTransactionAndRows(_tx, rows)
	}
	rows.Next()
	b.ResetTimer()

	//Run the benchmark tests
	for i := 0; i < b.N; i++ {
		var rr RowReader
		ts1 := setupTestStruct()
		{
			sm, _ := ModelStruct(ts1)
			rr = sm.CreateReader()
		}

		for n := 0; n < NumBenchmarkScanRowsPasses; n++ {
			if err := rr.ScanRows(rows, &ts1); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkRowReader_ScanRows_Native(b *testing.B) {
	rowReaderScanRowsNative(b, false)
}
func BenchmarkRowReader_ScanRows_NativePrepared(b *testing.B) {
	rowReaderScanRowsNative(b, true)
}
func rowReaderScanRowsNative(b *testing.B, usePreparedQuery bool) {
	//Init test data
	var rows *sql.Rows
	if _tx, _rows, err := setupTestQuery(usePreparedQuery, true); err != nil {
		rollbackTransactionAndRows(_tx, _rows)
		b.Fatal(err)
	} else {
		rows = _rows
		defer rollbackTransactionAndRows(_tx, rows)
	}
	rows.Next()
	b.ResetTimer()

	//Run the benchmark tests
	for i := 0; i < b.N; i++ {
		ts1 := setupTestStruct()
		var timeBuff1, timeBuff2 []byte //Since MySQL time.Time support seems to not work, need to scan into byte buffers
		for n := 0; n < NumBenchmarkScanRowsPasses; n++ {
			if err := rows.Scan(getPointersForTestStruct(&ts1, &timeBuff1, &timeBuff2)...); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func getPointersForTestStruct(ts1 *testStruct1, timeBuff1, timeBuff2 *[]byte) []any {
	return []any{
		&ts1.P1,
		&ts1.U,
		&ts1.U8,
		&ts1.U16,
		&ts1.U32,
		&ts1.U64,
		&ts1.I,
		&ts1.I8,
		&ts1.I16,
		&ts1.I32,
		&ts1.I64,
		&ts1.F32,
		ts1.F64,
		&ts1.S,
		&ts1.BA,
		&ts1.RB,
		&ts1.B,
		&ts1.P2,
		ts1.TS3.TS4.U,
		ts1.TS3.TS4.U8,
		ts1.TS3.TS4.U16,
		ts1.TS3.TS4.U32,
		ts1.TS3.TS4.U64,
		ts1.TS3.TestStruct5.I,
		ts1.TS3.TestStruct5.I8,
		ts1.TS3.TestStruct5.I16,
		ts1.TS3.TestStruct5.I32,
		ts1.TS3.TestStruct5.I64,
		ts1.TS3.F32,
		&ts1.TS3.F64,
		ts1.TS3.TS6.TS7.S,
		ts1.TS3.TS6.BA,
		ts1.TS3.RB,
		ts1.TS3.B,
		&ts1.TS9.P3,
		&timeBuff1,
		&timeBuff2,
	}
}

//-------------------------------Benchmark ScanRow------------------------------
//Unfortunately since Row.Scan() immediately clears its contents upon reading we have to run an SQL query for every test iteration, which basically invalidates the test timing. It is possible to get accurate results though by modifying the sql.go

func BenchmarkRowReader_ScanRow_OneItem_Faster(b *testing.B) {
	//Connect to the database and create a transaction
	var tx *sql.Tx
	if _tx, err := setupSQLConnect(); err != nil {
		b.Fatal(err)
	} else {
		tx = _tx
	}
	defer rollbackTransactionAndRows(tx, nil)

	//Run the benchmark tests
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ts1 struct{ i1 int }
		if err := ScanRow(tx.QueryRow(`SELECT 5`), &ts1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRowReader_ScanRow_OneItem_Native(b *testing.B) {
	//Connect to the database and create a transaction
	var tx *sql.Tx
	if _tx, err := setupSQLConnect(); err != nil {
		b.Fatal(err)
	} else {
		tx = _tx
	}
	defer rollbackTransactionAndRows(tx, nil)

	//Run the benchmark tests
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ts1 struct{ i1 int }
		if err := tx.QueryRow(`SELECT 5`).Scan(&ts1.i1); err != nil {
			b.Fatal(err)
		}
	}
}

//----------------------------Benchmark ScanRowMulti----------------------------
//Unfortunately since Row.Scan() immediately clears its contents upon reading we have to run an SQL query for every test iteration, which basically invalidates the test timing. It is possible to get accurate results though by modifying the sql.go

func BenchmarkRowReader_ScanRowMulti_Faster(b *testing.B) {
	//Init test data
	var tx *sql.Tx
	if _tx, _rows, err := setupTestQuery(false, true); err != nil {
		rollbackTransactionAndRows(_tx, _rows)
		b.Fatal(err)
	} else {
		_ = _rows.Close()
		tx = _tx
		defer rollbackTransactionAndRows(tx, nil)
	}
	queryStr := getTestQueryString(true)

	//Run the benchmark tests
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1 := setupTestStruct()
		if err := ScanRowMulti(tx.QueryRow(queryStr), &ts1.P1, &ts1.TestStruct2, ts1.P2, &ts1.TS3, ts1.TS9); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRowReader_ScanRowMulti_Native(b *testing.B) {
	//Init test data
	var tx *sql.Tx
	if _tx, _rows, err := setupTestQuery(false, true); err != nil {
		rollbackTransactionAndRows(_tx, _rows)
		b.Fatal(err)
	} else {
		_ = _rows.Close()
		tx = _tx
		defer rollbackTransactionAndRows(tx, nil)
	}
	queryStr := getTestQueryString(true)

	//Run the benchmark tests
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1 := setupTestStruct()
		var timeBuff1, timeBuff2 []byte //Since MySQL time.Time support seems to not work, need to scan into byte buffers
		pointers := getPointersForTestStruct(&ts1, &timeBuff1, &timeBuff2)

		//Change RawBytes to []byte since Row.Scan() does not support RawBytes
		for _, n := range []int{15, 32} {
			pointers[n] = (*[]byte)(pointers[n].(*sql.RawBytes))
		}

		if err := tx.QueryRow(queryStr).Scan(pointers...); err != nil {
			b.Fatal(err)
		}
	}
}
