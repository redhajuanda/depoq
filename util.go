package depoq

import "reflect"

// isStruct checks if the given interface is a struct or not
func isStruct(i interface{}) bool {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem() // Dereference the pointer
	}
	return t.Kind() == reflect.Struct
}
