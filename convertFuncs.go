//Functions to convert to all the different types

package gofastersql

import (
	"database/sql"
	nt "github.com/dakusan/gofastersql/nulltypes"
	"strconv"
	"time"
	"unsafe"
)

type upt unsafe.Pointer

//-------------------Generic numeric converters and (set)null-------------------

func convUNum[T uint8 | uint16 | uint32 | uint64](in []byte, p upt, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseUint(b2s(in), 10, bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}
func convINum[T int8 | int16 | int32 | int64](in []byte, p upt, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseInt(b2s(in), 10, bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}
func convFloat[T float32 | float64](in []byte, p upt, bits int) error {
	if in == nil {
		*(*T)(p) = 0
	} else if n, err := strconv.ParseFloat(b2s(in), bits); err != nil {
		return err
	} else {
		*(*T)(p) = T(n)
	}
	return nil
}
func null(in []byte, p upt) []byte {
	(*nt.NullInherit)(p).IsNull = in == nil
	return in
}

//-------------------Conversion function for all scalar types-------------------

func convUint8(in []byte, p upt) error    { return convUNum[uint8](in, p, 8) }
func convUint16(in []byte, p upt) error   { return convUNum[uint16](in, p, 16) }
func convUint32(in []byte, p upt) error   { return convUNum[uint32](in, p, 32) }
func convUint64(in []byte, p upt) error   { return convUNum[uint64](in, p, 64) }
func convInt8(in []byte, p upt) error     { return convINum[int8](in, p, 8) }
func convInt16(in []byte, p upt) error    { return convINum[int16](in, p, 16) }
func convInt32(in []byte, p upt) error    { return convINum[int32](in, p, 32) }
func convInt64(in []byte, p upt) error    { return convINum[int64](in, p, 64) }
func convFloat32(in []byte, p upt) error  { return convFloat[float32](in, p, 32) }
func convFloat64(in []byte, p upt) error  { return convFloat[float64](in, p, 64) }
func convString(in []byte, p upt) error   { *(*string)(p) = string(in); return nil }
func convRawBytes(in []byte, p upt) error { *(*sql.RawBytes)(p) = in; return nil }
func convByteArray(in []byte, p upt) error {
	if in == nil {
		return nil
	}

	out := make([]byte, len(in))
	copy(out, in)
	*(*[]byte)(p) = out
	return nil
}
func convBool(in []byte, p upt) error {
	if in == nil {
		*(*bool)(p) = false
	} else {
		*(*bool)(p) = in[0] == '1'
	}
	return nil
}
func convTime(in []byte, p upt) error {
	//Null sets to timestamp=0
	if in == nil {
		*(*time.Time)(p) = time.Unix(0, 0).UTC()
		return nil
	}

	//If there are only digits and an optional single decimal place, parse the number as a timestamp (with optional fractional seconds)
	dotLoc, isValidFloat := -1, true
	for loc, r := range in {
		if r >= '0' && r <= '9' {
			continue
		}
		if r != '.' || dotLoc != -1 {
			isValidFloat = false
			break
		}
		dotLoc = loc
	}
	if isValidFloat {
		//Get the fractional part
		fractionalSeconds := int64(0)
		if dotLoc != -1 {
			nanoBuff := []byte{'0', '0', '0', '0', '0', '0', '0', '0', '0'} //Maximum number of nanoseconds in a second is 9 digits
			frac := b2s(in)[dotLoc+1:]
			if len(frac) > len(nanoBuff) {
				frac = frac[0:len(nanoBuff)]
			}
			copy(nanoBuff, frac)
			if _fractionalSeconds, err := strconv.ParseInt(b2s(nanoBuff), 10, 64); err != nil {
				return err
			} else {
				fractionalSeconds = _fractionalSeconds
			}
		} else {
			//Reset the dot location to the end of the number
			dotLoc = len(in)
		}

		//Get the integral part
		if integralSeconds, err := strconv.ParseInt(b2s(in)[0:dotLoc], 10, 64); err != nil {
			return err
		} else {
			*(*time.Time)(p) = time.Unix(integralSeconds, fractionalSeconds).UTC()
		}
		return nil
	}

	//Parse as mysql time
	if t, err := time.Parse(`2006-01-02 15:04:05.99999`, b2s(in)); err != nil {
		return err
	} else {
		*(*time.Time)(p) = t
	}
	return nil
}

// ---------------Conversion function for all NULLABLE scalar types--------------
//I had to get a bit aggressive with name shortening methods below to keep everything on 1 line

func cvNU8(b []byte, p upt) error  { return convUint8(null(b, p), upt(&(*nt.NullUint8)(p).Val)) }
func cvNU16(b []byte, p upt) error { return convUint16(null(b, p), upt(&(*nt.NullUint16)(p).Val)) }
func cvNU32(b []byte, p upt) error { return convUint32(null(b, p), upt(&(*nt.NullUint32)(p).Val)) }
func cvNU64(b []byte, p upt) error { return convUint64(null(b, p), upt(&(*nt.NullUint64)(p).Val)) }
func cvNI8(b []byte, p upt) error  { return convInt8(null(b, p), upt(&(*nt.NullInt8)(p).Val)) }
func cvNI16(b []byte, p upt) error { return convInt16(null(b, p), upt(&(*nt.NullInt16)(p).Val)) }
func cvNI32(b []byte, p upt) error { return convInt32(null(b, p), upt(&(*nt.NullInt32)(p).Val)) }
func cvNI64(b []byte, p upt) error { return convInt64(null(b, p), upt(&(*nt.NullInt64)(p).Val)) }
func cvNF32(b []byte, p upt) error { return convFloat32(null(b, p), upt(&(*nt.NullFloat32)(p).Val)) }
func cvNF64(b []byte, p upt) error { return convFloat64(null(b, p), upt(&(*nt.NullFloat64)(p).Val)) }
func cvNS(b []byte, p upt) error   { return convString(null(b, p), upt(&(*nt.NullString)(p).Val)) }
func cvNRB(b []byte, p upt) error  { return convRawBytes(null(b, p), upt(&(*nt.NullRawBytes)(p).Val)) }
func cvNBA(b []byte, p upt) error  { return convByteArray(null(b, p), upt(&(*nt.NullByteArray)(p).Val)) }
func cvNB(b []byte, p upt) error   { return convBool(null(b, p), upt(&(*nt.NullBool)(p).Val)) }
func cvNT(b []byte, p upt) error   { return convTime(null(b, p), upt(&(*nt.NullTime)(p).Val)) }
