//Functions to convert to all the different types

package gofastersql

import (
	"database/sql"
	"strconv"
	"unsafe"
)

//--------------------------Generic numeric converters--------------------------

func convUNum[T uint8 | uint16 | uint32 | uint64](in []byte, p unsafe.Pointer, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseUint(b2s(in), 10, bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}
func convINum[T int8 | int16 | int32 | int64](in []byte, p unsafe.Pointer, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseInt(b2s(in), 10, bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}
func convFloat[T float32 | float64](in []byte, p unsafe.Pointer, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseFloat(b2s(in), bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}

//-------------------Conversion function for all scalar types-------------------

func convUint8(in []byte, p unsafe.Pointer) error    { return convUNum[uint8](in, p, 8) }
func convUint16(in []byte, p unsafe.Pointer) error   { return convUNum[uint16](in, p, 16) }
func convUint32(in []byte, p unsafe.Pointer) error   { return convUNum[uint32](in, p, 32) }
func convUint64(in []byte, p unsafe.Pointer) error   { return convUNum[uint64](in, p, 64) }
func convInt8(in []byte, p unsafe.Pointer) error     { return convINum[int8](in, p, 8) }
func convInt16(in []byte, p unsafe.Pointer) error    { return convINum[int16](in, p, 16) }
func convInt32(in []byte, p unsafe.Pointer) error    { return convINum[int32](in, p, 32) }
func convInt64(in []byte, p unsafe.Pointer) error    { return convINum[int64](in, p, 64) }
func convFloat32(in []byte, p unsafe.Pointer) error  { return convFloat[float32](in, p, 32) }
func convFloat64(in []byte, p unsafe.Pointer) error  { return convFloat[float64](in, p, 64) }
func convString(in []byte, p unsafe.Pointer) error   { *(*string)(p) = string(in); return nil }
func convRawBytes(in []byte, p unsafe.Pointer) error { *(*sql.RawBytes)(p) = in; return nil }
func convByteArray(in []byte, p unsafe.Pointer) error {
	out := make([]byte, len(in))
	copy(out, in)
	*(*[]byte)(p) = out
	return nil
}
func convBool(in []byte, p unsafe.Pointer) error {
	if in == nil {
		*(*bool)(p) = false
	} else {
		*(*bool)(p) = in[0] == '1'
	}
	return nil
}
