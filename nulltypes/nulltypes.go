// Package nulltypes contains all the scalar types as nullable
package nulltypes

import (
	"database/sql"
	"fmt"
	"time"
	"unsafe"
)

// NullInherit is the structure that all other Null structures inherit from
type NullInherit struct {
	IsNull bool
}
type NullUint8 struct {
	NullInherit
	Val uint8
}
type NullUint16 struct {
	NullInherit
	Val uint16
}
type NullUint32 struct {
	NullInherit
	Val uint32
}
type NullUint64 struct {
	NullInherit
	Val uint64
}
type NullInt8 struct {
	NullInherit
	Val int8
}
type NullInt16 struct {
	NullInherit
	Val int16
}
type NullInt32 struct {
	NullInherit
	Val int32
}
type NullInt64 struct {
	NullInherit
	Val int64
}
type NullFloat32 struct {
	NullInherit
	Val float32
}
type NullFloat64 struct {
	NullInherit
	Val float64
}
type NullBool struct {
	NullInherit
	Val bool
}
type NullString struct {
	NullInherit
	Val string
}
type NullByteArray struct {
	NullInherit
	Val []byte
}
type NullRawBytes struct {
	NullInherit
	Val sql.RawBytes
}
type NullTime struct {
	NullInherit
	Val time.Time
}

func (t NullUint8) String() string     { return getStr(t.IsNull, t.Val) }
func (t NullUint16) String() string    { return getStr(t.IsNull, t.Val) }
func (t NullUint32) String() string    { return getStr(t.IsNull, t.Val) }
func (t NullUint64) String() string    { return getStr(t.IsNull, t.Val) }
func (t NullInt8) String() string      { return getStr(t.IsNull, t.Val) }
func (t NullInt16) String() string     { return getStr(t.IsNull, t.Val) }
func (t NullInt32) String() string     { return getStr(t.IsNull, t.Val) }
func (t NullInt64) String() string     { return getStr(t.IsNull, t.Val) }
func (t NullFloat32) String() string   { return getStr(t.IsNull, t.Val) }
func (t NullFloat64) String() string   { return getStr(t.IsNull, t.Val) }
func (t NullBool) String() string      { return getStr(t.IsNull, t.Val) }
func (t NullString) String() string    { return getStr(t.IsNull, t.Val) }
func (t NullByteArray) String() string { return getStr(t.IsNull, b2s(t.Val)) }
func (t NullRawBytes) String() string  { return getStr(t.IsNull, b2s(t.Val)) }
func (t NullTime) String() string      { return getStr(t.IsNull, t.Val.Format(`2006-01-02 15:04:05.99999`)) }

func getStr[T any](isNull bool, val T) string {
	if isNull {
		return "NULL"
	} else {
		return fmt.Sprintf("%v", val)
	}
}

const nullTimeFormat = `2006-01-02T15:04:05.000Z`

func (t NullUint8) MarshalJSON() ([]byte, error)     { return mj(t.IsNull, t.Val) }
func (t NullUint16) MarshalJSON() ([]byte, error)    { return mj(t.IsNull, t.Val) }
func (t NullUint32) MarshalJSON() ([]byte, error)    { return mj(t.IsNull, t.Val) }
func (t NullUint64) MarshalJSON() ([]byte, error)    { return mj(t.IsNull, t.Val) }
func (t NullInt8) MarshalJSON() ([]byte, error)      { return mj(t.IsNull, t.Val) }
func (t NullInt16) MarshalJSON() ([]byte, error)     { return mj(t.IsNull, t.Val) }
func (t NullInt32) MarshalJSON() ([]byte, error)     { return mj(t.IsNull, t.Val) }
func (t NullInt64) MarshalJSON() ([]byte, error)     { return mj(t.IsNull, t.Val) }
func (t NullFloat32) MarshalJSON() ([]byte, error)   { return mj(t.IsNull, t.Val) }
func (t NullFloat64) MarshalJSON() ([]byte, error)   { return mj(t.IsNull, t.Val) }
func (t NullBool) MarshalJSON() ([]byte, error)      { return mj(t.IsNull, t.Val) }
func (t NullString) MarshalJSON() ([]byte, error)    { return qmj(t.IsNull, t.Val) }
func (t NullByteArray) MarshalJSON() ([]byte, error) { return qmj(t.IsNull, b2s(t.Val)) }
func (t NullRawBytes) MarshalJSON() ([]byte, error)  { return qmj(t.IsNull, b2s(t.Val)) }
func (t NullTime) MarshalJSON() ([]byte, error)      { return qmj(t.IsNull, t.Val.Format(nullTimeFormat)) }

func mj[T any](isNull bool, val T) ([]byte, error) {
	if isNull {
		return []byte("null"), nil
	} else {
		return []byte(fmt.Sprintf("%v", val)), nil
	}
}
func qmj(isNull bool, val string) ([]byte, error) {
	if isNull {
		return []byte("null"), nil
	} else {
		return []byte(`"` + val + `"`), nil
	}
}

// b2s (Unsafe!) converts a byte slice to a string
func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
