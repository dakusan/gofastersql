//Convert a struct to its model

package gofastersql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

// StructModel holds the model of a structure for processing as a RowReader. StructModel is concurrency safe.
type StructModel struct {
	fields   []structField
	pointers []structPointer
	rType    reflect.Type
}
type structField struct {
	offset       uintptr
	converter    func(in []byte, p unsafe.Pointer) error
	pointerIndex int
	name         string
	isPointer    bool
}
type structPointer struct {
	parentIndex int
	offset      uintptr
	name        string
}

// Store structs for future lookups
var remStructs map[reflect.Type]StructModel
var remLock sync.RWMutex

func init() {
	remStructs = make(map[reflect.Type]StructModel)
}

// ModelStruct extracts the model of a structure for processing as a RowReader.
func ModelStruct(s any) (StructModel, error) {
	//Throw error if a structure is not passed
	t := reflect.TypeOf(s)
	if t.Kind() != reflect.Struct {
		return StructModel{}, fmt.Errorf("Not a %s", reflect.Struct.String())
	}

	//If we already have the structure model cached then return it
	remLock.RLock()
	if s, ok := remStructs[t]; ok {
		remLock.RUnlock()
		return s, nil
	}
	remLock.RUnlock()

	//Do a recursive count of the number of fields
	numFields := 1
	numStructPointers := 0
	{
		var doCount func(reflect.Type)
		doCount = func(v reflect.Type) {
			numFields += v.NumField() - 1
			for i := 0; i < v.NumField(); i++ {
				t := v.Field(i).Type
				if t.Kind() == reflect.Struct {
					doCount(t)
				} else if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct {
					numStructPointers++
					doCount(t.Elem())
				}
			}
		}
		doCount(t)
	}

	//Create the structure model
	ret := StructModel{make([]structField, numFields), make([]structPointer, numStructPointers), t}
	{
		var processStruct func(reflect.Type, uintptr, int, string) []string
		fieldPos := 0
		structPointerPos := 0
		byteArrayType, rawBytesType := reflect.TypeOf([]byte{}), reflect.TypeOf(sql.RawBytes{})
		processStruct = func(v reflect.Type, parentOffset uintptr, parentStructIndex int, parentName string) (retErr []string) {
			for i := 0; i < v.NumField(); i++ {
				//Handle pointers
				fld := v.Field(i)
				fldType := fld.Type
				isPointer := fldType.Kind() == reflect.Pointer
				if isPointer {
					fldType = fld.Type.Elem()
				}

				//Get the function pointer for the type
				var fn func(in []byte, p unsafe.Pointer) error
				switch fldType.Kind() {
				case reflect.String:
					fn = convString
				case reflect.Slice:
					if fldType.AssignableTo(byteArrayType) {
						fn = cond(fldType == rawBytesType, convRawBytes, convByteArray)
					}
				case reflect.Int:
					fn = cond(fldType.Size() == unsafe.Sizeof(int32(0)), convInt32, convInt64)
				case reflect.Uint:
					fn = cond(fldType.Size() == unsafe.Sizeof(uint32(0)), convUint32, convUint64)
				case reflect.Int8:
					fn = convInt8
				case reflect.Int16:
					fn = convInt16
				case reflect.Int32:
					fn = convInt32
				case reflect.Int64:
					fn = convInt64
				case reflect.Uint8:
					fn = convUint8
				case reflect.Uint16:
					fn = convUint16
				case reflect.Uint32:
					fn = convUint32
				case reflect.Uint64:
					fn = convUint64
				case reflect.Float32:
					fn = convFloat32
				case reflect.Float64:
					fn = convFloat64
				case reflect.Bool:
					fn = convBool
				case reflect.Struct:
					//Pointers to structures need to add their StructModel.pointers and redirect appropriately
					offset, structIndex := parentOffset+fld.Offset, parentStructIndex
					if isPointer {
						ret.pointers[structPointerPos] = structPointer{parentStructIndex, parentOffset + fld.Offset, parentName + fld.Name}
						structPointerPos++
						offset, structIndex = 0, structPointerPos //structIndex is +1 what you'd expect because RowReader.pointers[0] is the root struct pointer
					}

					//Recurse on structures
					retErr = append(retErr, processStruct(fldType, offset, structIndex, parentName+fld.Name+".")...)
					continue
				}

				//If there is no function pointer than the type is invalid
				if fn == nil {
					retErr = append(retErr, fmt.Sprintf("%s%s: %s%s", parentName, fld.Name, cond(isPointer, "*", ""), fldType.String()))
				}

				//Store the member
				ret.fields[fieldPos] = structField{parentOffset + fld.Offset, fn, parentStructIndex, parentName + fld.Name, isPointer}
				fieldPos++
			}

			return
		}
		if err := processStruct(t, 0, 0, ""); len(err) != 0 {
			return StructModel{}, fmt.Errorf("Invalid types found for members:\n%s", strings.Join(err, "\n"))
		}
	}

	//Cache the structure model
	remLock.Lock()
	remStructs[t] = ret
	remLock.Unlock()

	//Return success
	return ret, nil
}

// Equals returns if these are from the same struct
func (sm StructModel) Equals(sm2 StructModel) bool {
	return sm.rType == sm2.rType
}
