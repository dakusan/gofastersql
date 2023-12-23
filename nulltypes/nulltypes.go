// Package nulltypes contains all the scalar types as nullable
package nulltypes

import (
	"database/sql"
	"fmt"
	"time"
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
func (t NullByteArray) String() string { return getStr(t.IsNull, string(t.Val)) }
func (t NullRawBytes) String() string  { return getStr(t.IsNull, string(t.Val)) }
func (t NullTime) String() string      { return getStr(t.IsNull, t.Val.Format(`2006-01-02 15:04:05.99999`)) }

func getStr[T any](isNull bool, val T) string {
	if isNull {
		return "NULL"
	} else {
		return fmt.Sprintf("%v", val)
	}
}
