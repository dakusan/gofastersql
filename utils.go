//Utility functions

package gofastersql

import (
	"unsafe"
)

// b2s (Unsafe!) converts a byte slice to a string
func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// interface2Pointer (Unsafe!) converts an interface with a pointer to its pointer
func interface2Pointer(v any) unsafe.Pointer {
	return (*(*struct{ _, Data unsafe.Pointer })(unsafe.Pointer(&v))).Data
}

// cond is basically the conditional operator
func cond[T any](isTrue bool, ifTrue, ifFalse T) T {
	if isTrue {
		return ifTrue
	}
	return ifFalse
}
