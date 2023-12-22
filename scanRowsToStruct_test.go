package gofastersql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"testing"
)

const (
	SQLConnectString   = "USERNAME@tcp(HOSTNAME:PORT)/DBNAME"
	NumBenchmarkPasses = 100_000
)

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
	P3 []byte //0
}

func setupTestQuery() (*sql.Tx, *sql.Rows, error) {
	//Connect to the database and create a transaction
	var tx *sql.Tx
	if db, err := sql.Open("mysql", SQLConnectString); err != nil {
		return nil, nil, err
	} else if err := db.Ping(); err != nil {
		return nil, nil, err
	} else if _tx, err := db.Begin(); err != nil {
		return nil, nil, err
	} else {
		tx = _tx
	}

	//Create a temporary table and fill it with values 0, 1, 0
	if _, err := tx.Exec(`CREATE TEMPORARY TABLE goTest (i int) ENGINE=MEMORY`); err != nil {
		return nil, nil, err
	} else if _, err := tx.Exec(`INSERT INTO goTest VALUES (0), (1), (0);`); err != nil {
		return nil, nil, err
	}

	//Select values for all the columns
	//Return #1 will have max-values for sets #1,#2 and min-value for set #4
	//Return #2 will have sets #1,#2,#4 overflow (though some of the 64 bit ones cant overflow in SQL for testing)
	rows, err := tx.Query(`
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
	CONCAT('P3-', i)
FROM goTest
	`)

	return tx, rows, err
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
		TS9: &TestStruct9{},
	}
}

func TestAllTypes(t *testing.T) {
	//Init test data
	var tx *sql.Tx
	var rows *sql.Rows
	if _tx, _rows, err := setupTestQuery(); err != nil {
		t.Fatal(err)
	} else {
		tx, rows = _tx, _rows
	}
	ts1 := setupTestStruct()

	//Pass #1: Read into the structure and make sure it comes out correct
	rows.Next()
	var rr RowReader
	var sm StructModel
	if _sm, err := ModelStruct(ts1); err != nil {
		t.Fatal(err)
	} else {
		sm = _sm
		rr = sm.CreateReader()
	}
	if err := rr.ScanRows(rows, &ts1); err != nil {
		t.Fatal(err)
	}
	if str, err := json.Marshal(ts1); err != nil {
		t.Fatal(err)
	} else if //goland:noinspection SpellCheckingInspection
	string(str) != `{"P1":"P1-0","U":2,"U8":255,"U16":65535,"U32":4294967295,"U64":18446744073709551615,"I":2,"I8":127,"I16":32767,"I32":2147483647,"I64":9223372036854775807,"F32":1.1,"F64":5.5,"S":"str-0","BA":"YmEtMA==","RB":"cmItMA==","B":false,"P2":5,"TS3":{"TS4":{"U":20,"U8":254,"U16":65534,"U32":4294967294,"U64":18446744073709551615},"I":20,"I8":-128,"I16":-32768,"I32":-2147483648,"I64":-9223372036854775808,"F32":11.11,"F64":12.12,"TS6":{"TS7":{"S":"strP-0"},"BA":"YmFQLTA="},"RB":"cmJQLTA=","B":false},"TS9":{"P3":"UDMtMA=="}}` {
		t.Fatal("Structure json marshal did not match")
	}

	//Pass #2: Check for the expected overflow errors
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

	//Make sure we get back the same struct on a second attempt
	ts2 := testStruct1{}
	if sm2, err := ModelStruct(ts2); err != nil {
		t.Fatal(err)
	} else if !sm2.Equals(sm) {
		t.Fatal("Struct models are not for the same struct")
	}

	//Pass #3: Check for the expected nil pointer errors
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
	_ = rows.Close()

	//Test ReadRow
	type smallTest struct{ a, b int }
	var st smallTest
	if err := ScanRow(tx.QueryRow("SELECT i, i*3 FROM goTest LIMIT 1, 1"), &st); err != nil {
		t.Fatal(err)
	} else if st.a != 1 || st.b != 3 {
		t.Fatal(fmt.Sprintf("smallTest is not the expected value ({%d,%d}!={%d,%d})", st.a, st.b, 1, 3))
	}
}

func BenchmarkRowReader_ScanRows_Faster(b *testing.B) {
	//Init test data
	var rows *sql.Rows
	if _, _rows, err := setupTestQuery(); err != nil {
		b.Fatal(err)
	} else {
		rows = _rows
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

		for n := 0; n < NumBenchmarkPasses; n++ {
			if err := rr.ScanRows(rows, &ts1); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkRowReader_ScanRows_Native(b *testing.B) {
	//Init test data
	var rows *sql.Rows
	if _, _rows, err := setupTestQuery(); err != nil {
		b.Fatal(err)
	} else {
		rows = _rows
	}
	rows.Next()
	b.ResetTimer()

	//Run the benchmark tests
	for i := 0; i < b.N; i++ {
		ts1 := setupTestStruct()
		for n := 0; n < NumBenchmarkPasses; n++ {
			if err := rows.Scan(
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
			); err != nil {
				b.Fatal(err)
			}
		}
	}
}
