//Convert a struct to its model

package gofastersql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/dakusan/gofastersql/nulltypes"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

//---------------------------Primary models and cache---------------------------

// StructModel holds the model of a structure for processing as a RowReader. StructModel is concurrency safe.
// If requested to model multiple types (or just a non-struct scalar) then a hacky version is used that emulates the array of variables as a single struct with pointers to each variable.
type StructModel struct {
	fields   []structField   //The flattened list of members from a recursive structure search
	pointers []structPointer //Data for structure pointers (recursive)
	rTypes   []reflect.Type  //The types of the top level structures. Used to confirm RowReader.ScanRow*() function “outPointers” parameters’ types match
	isSimple bool            //If this is modeling a single structure (not a list of variables)
}
type structField struct {
	offset       uintptr          //The offset of the member in structure pointed at by RowReader.pointers[pointerIndex] (which is derived from StructModel.pointers)
	converter    converterFunc    //The conversion function
	pointerIndex int              //The structure index to be used for offset (RowReader.pointers[pointerIndex], which is derived from StructModel.pointers)
	name         string           //The recursed name of the member
	isPointer    bool             //If the member is a pointer
	flags        structFieldFlags //Flags about the member
}
type structPointer struct {
	parentIndex int     //The structure index to be used for offset (RowReader.pointers[parentIndex], which is derived from StructModel.pointers)
	offset      uintptr //The offset of the member in structure pointed at by RowReader.pointers[parentIndex] (which is derived from StructModel.pointers)
	name        string  //The recursed name of the member
}

type structFieldFlags uint8

const (
	sffNoFlags    structFieldFlags = 0
	sffIsRawBytes structFieldFlags = 1 << (iota - 1) //If the member is a RawBytes type
	sffIsNullable                                    //If the member is a nulltypes struct
)

// Store structs for future lookups
var remStructs = make(map[reflect.Type]StructModel)
var remLock sync.RWMutex

//-----------------------Mappings for conversion functions----------------------

type converterFunc func(in []byte, p upt) error

var nullTypeStructConverters = map[reflect.Type]converterFunc{
	reflect.TypeOf(nulltypes.NullUint8{}):     cvNU8,
	reflect.TypeOf(nulltypes.NullUint16{}):    cvNU16,
	reflect.TypeOf(nulltypes.NullUint32{}):    cvNU32,
	reflect.TypeOf(nulltypes.NullUint64{}):    cvNU64,
	reflect.TypeOf(nulltypes.NullInt8{}):      cvNI8,
	reflect.TypeOf(nulltypes.NullInt16{}):     cvNI16,
	reflect.TypeOf(nulltypes.NullInt32{}):     cvNI32,
	reflect.TypeOf(nulltypes.NullInt64{}):     cvNI64,
	reflect.TypeOf(nulltypes.NullFloat32{}):   cvNF32,
	reflect.TypeOf(nulltypes.NullFloat64{}):   cvNF64,
	reflect.TypeOf(nulltypes.NullString{}):    cvNS,
	reflect.TypeOf(nulltypes.NullRawBytes{}):  cvNRB,
	reflect.TypeOf(nulltypes.NullByteArray{}): cvNBA,
	reflect.TypeOf(nulltypes.NullBool{}):      cvNB,
	reflect.TypeOf(nulltypes.NullTime{}):      cvNT,
}
var scalarConverters = make([]converterFunc, reflect.UnsafePointer) //UnsafePointer is the final enum of reflect.Kind
func init() {
	for _, d := range []struct {
		k reflect.Kind
		f converterFunc
	}{
		{reflect.String, convString},
		{reflect.Int, cond(unsafe.Sizeof(0) == unsafe.Sizeof(int32(0)), convInt32, convInt64)},
		{reflect.Uint, cond(unsafe.Sizeof(uint(0)) == unsafe.Sizeof(uint32(0)), convUint32, convUint64)},
		{reflect.Int8, convInt8},
		{reflect.Int16, convInt16},
		{reflect.Int32, convInt32},
		{reflect.Int64, convInt64},
		{reflect.Uint8, convUint8},
		{reflect.Uint16, convUint16},
		{reflect.Uint32, convUint32},
		{reflect.Uint64, convUint64},
		{reflect.Float32, convFloat32},
		{reflect.Float64, convFloat64},
		{reflect.Bool, convBool},
	} {
		if int(d.k) > len(scalarConverters) {
			panic("reflect.UnsafePointer is no longer the highest reflect.Kind?")
		}
		scalarConverters[d.k] = d.f
	}
}

var lookupType = struct{ time, nullInherit, byteArray, rawBytes, nullRawBytes reflect.Type }{
	reflect.TypeOf(time.Time{}),
	reflect.TypeOf(nulltypes.NullInherit{}),
	reflect.TypeOf([]byte{}),
	reflect.TypeOf(sql.RawBytes{}),
	reflect.TypeOf(nulltypes.NullRawBytes{}),
}

//------------------------------Create StructModels-----------------------------

// ModelStruct extracts the model of variables for processing as a RowReader. It can take both pointers and non-pointers.
func ModelStruct(s ...any) (StructModel, error) {
	//If no variables passed return an error
	if len(s) == 0 {
		return StructModel{}, errors.New("At least 1 variable is required")
	}

	//If only 1 variable is passed, and it is a structure, create a simple StructModel
	if len(s) == 1 {
		t := reflect.TypeOf(s[0])
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct && !isScalarStruct(t) {
			//If we already have the structure model cached then return it
			remLock.RLock()
			if s, ok := remStructs[t]; ok {
				remLock.RUnlock()
				return s, nil
			}
			remLock.RUnlock()

			return createStructModelFromStruct(t)
		}
	}

	ret, err := getMultipleStructsAsStructModel(s)
	return ret, err
}

// Function to determine if a struct is considered a scalar type
func isScalarStruct(t reflect.Type) bool {
	return nullTypeStructConverters[t] != nil || t == lookupType.time
}

// Create a StructModel
func createStructModelFromStruct(t reflect.Type) (StructModel, error) {
	//Do a recursive count of the number of fields
	numFields := 1
	numStructPointers := 0
	{
		var doCount func(reflect.Type)
		doCount = func(v reflect.Type) {
			numFields += v.NumField() - 1
			for i := 0; i < v.NumField(); i++ {
				t := v.Field(i).Type
				if t.Kind() == reflect.Struct && !isScalarStruct(t) {
					doCount(t)
				} else if t.Kind() == reflect.Pointer {
					if el := t.Elem(); el.Kind() == reflect.Struct && !isScalarStruct(el) {
						numStructPointers++
						doCount(t.Elem())
					}
				}
			}
		}
		doCount(t)
	}

	//Create the structure model
	ret := StructModel{make([]structField, numFields), make([]structPointer, numStructPointers), []reflect.Type{t}, true}
	{
		var processStruct func(reflect.Type, uintptr, int, string) []string
		fieldPos := 0
		structPointerPos := 0
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
				fn, sff := scalarToConversionFunc(fldType)
				if fn == nil && fldType.Kind() == reflect.Struct {
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
				ret.fields[fieldPos] = structField{parentOffset + fld.Offset, fn, parentStructIndex, parentName + fld.Name, isPointer, sff}
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

// Convert a scalar reflect.Type to its conversion function
func scalarToConversionFunc(fldType reflect.Type) (converterFunc, structFieldFlags) {
	//Handle real scalar types
	k := fldType.Kind()
	cf := scalarConverters[k]
	if cf != nil {
		return cf, sffNoFlags
	}

	//Handle pretend scalar types
	switch k {
	case reflect.Slice:
		if fldType.AssignableTo(lookupType.byteArray) {
			if fldType == lookupType.rawBytes {
				return convRawBytes, sffIsRawBytes
			} else {
				return convByteArray, sffNoFlags
			}
		}
	case reflect.Struct:
		if f := nullTypeStructConverters[fldType]; f != nil {
			return f, sffIsNullable | cond(fldType == lookupType.nullRawBytes, sffIsRawBytes, sffNoFlags)
		} else if fldType == lookupType.time {
			return convTime, sffNoFlags
		}
	}

	//Return no match
	return nil, sffNoFlags
}

// Creates a non-simple StructModel
func getMultipleStructsAsStructModel(vars []any) (StructModel, error) {
	//Pull the StructModels that we already have cached
	errs := make([]string, 0, len(vars))
	varSMs := make([]StructModel, len(vars))
	var newTypes map[reflect.Type]StructModel
	newSM := StructModel{isSimple: false, rTypes: make([]reflect.Type, len(vars))}
	{
		numMissing := len(vars)
		remLock.RLock()
		for i, v := range vars {
			t := reflect.TypeOf(v)
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			newSM.rTypes[i] = t
			if s, ok := remStructs[t]; ok {
				varSMs[i] = s
				numMissing--
			}
		}
		remLock.RUnlock()
		if numMissing != 0 {
			newTypes = make(map[reflect.Type]StructModel, numMissing)
		}
	}

	//Pull the uncached StructModels
	for i, v := range vars {
		//If the type was cached then nothing to do
		if varSMs[i].fields != nil {
			continue
		}

		//Get type pointed to
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}

		//If the new type was already stored in this run then use that
		if newVal, exists := newTypes[t]; exists {
			varSMs[i] = newVal
			continue
		}

		//Pull the StructModel for structs or scalars
		var err error
		var sm StructModel
		if t.Kind() == reflect.Struct && !isScalarStruct(t) {
			sm, err = createStructModelFromStruct(t)
		} else {
			sm, err = createStructModelFromScalar(t)
		}

		//Store either the successful result or the error
		if err != nil {
			errs = append(errs, fmt.Sprintf("Parameter #%d of type “%s” has errors:\n%s", i, t.String(), err.Error()))
		} else {
			varSMs[i] = sm
			newTypes[t] = sm
		}
	}

	//Return errors
	if len(errs) != 0 {
		return StructModel{}, errors.New(strings.Join(errs, "\n\n"))
	}

	//Initialize newSM (get number of pointers and fields)
	{
		numPointers, numFields := 0, 0
		for _, sm := range varSMs {
			numPointers += len(sm.pointers) + 1
			numFields += len(sm.fields)
		}
		newSM.fields = make([]structField, numFields)
		newSM.pointers = make([]structPointer, numPointers)
	}

	//Create a StructModel for return
	pointerSize := unsafe.Sizeof((*int)(nil))
	curPointerIndex, curFieldIndex := 0, 0
	for smIndex, sm := range varSMs {
		//Store the variable as a pointer
		newSM.pointers[curPointerIndex] = structPointer{0, pointerSize * uintptr(smIndex), "Param#" + strconv.Itoa(smIndex)}
		curPointerIndex++

		//Copy over its members
		for fieldIndex, field := range sm.fields {
			tempField := field
			tempField.pointerIndex += curPointerIndex
			//While I could update the name field here, to include the parameter number, I feel that is a waste of processing
			newSM.fields[curFieldIndex+fieldIndex] = tempField
		}
		curFieldIndex += len(sm.fields)

		//Copy over its pointers
		for pointerIndex, pointer := range sm.pointers {
			tempPointer := pointer
			tempPointer.parentIndex += curPointerIndex
			newSM.pointers[curPointerIndex+pointerIndex] = tempPointer
		}
		curPointerIndex += len(sm.pointers)
	}

	return newSM, nil
}

func createStructModelFromScalar(t reflect.Type) (StructModel, error) {
	convFunc, sff := scalarToConversionFunc(t)
	if convFunc == nil {
		return StructModel{}, errors.New("Invalid scalar type")
	}

	sm := StructModel{
		[]structField{{0, convFunc, 0, "Scalar-" + t.Name(), false, sff}},
		nil, []reflect.Type{t}, false,
	}

	//Cache the structure model
	remLock.Lock()
	remStructs[t] = sm
	remLock.Unlock()

	return sm, nil
}

//-------------------------------------Misc-------------------------------------

// Equals returns if these are from the same struct
func (sm StructModel) Equals(sm2 StructModel) bool {
	if len(sm.rTypes) != len(sm2.rTypes) {
		return false
	}
	for i, t := range sm.rTypes {
		if t != sm2.rTypes[i] {
			return false
		}
	}
	return true
}
