package core

/*
	Utility functions used in the implementations.
*/

import (
	"fmt"
	"os"
)

// Creates a new array for the passed data type.
func arrayForType(type_ DataTypes, len int) interface{} {
	if type_ == INT {
		return make([]int, len)
	} else if type_ == FLOAT {
		return make([]float64, len)
	} else {
		return make([]string, len)
	}
}

// Returns a comparator function using the passed comparison with the passed value.
func comparator[T int | float64 | string](comp Comparison, compVal T) func(T) bool {
	if comp == EQ {
		return func(a T) bool { return a == compVal }
	} else if comp == NEQ {
		return func(a T) bool { return a != compVal }
	} else if comp == LT {
		return func(a T) bool { return a < compVal }
	} else if comp == GT {
		return func(a T) bool { return a > compVal }
	} else if comp == LE {
		return func(a T) bool { return a <= compVal }
	} else if comp == GE {
		return func(a T) bool { return a >= compVal }
	}
	return nil
}

func getRelevantRows[T int | string | float64](comp Comparison, cmpVal T, cols []Column, relCol int) []Column {
	comparator := comparator(comp, cmpVal)  // function to compare the values
	relevantData := cols[relCol].Data.([]T) // the data of the colum to use for selecting
	resultCols := make([]Column, len(cols)) // the resulting (filtered) columns
	// init the columns
	for idx := range cols {
		resultCols[idx].Signature = cols[idx].Signature
		resultCols[idx].Data = arrayForType(cols[idx].Signature.Type, 0)
	}

	for row, elm := range relevantData {
		// checks for every element inside the relevant column whether it meets the condition
		if comparator(elm) {
			// if so -> copy the data inside this row
			for col_idx, column := range cols {
				if column.Signature.Type == INT {
					resultCols[col_idx].Data = append(resultCols[col_idx].Data.([]int), column.Data.([]int)[row])
				} else if column.Signature.Type == STRING {
					resultCols[col_idx].Data = append(resultCols[col_idx].Data.([]string), column.Data.([]string)[row])
				} else if column.Signature.Type == FLOAT {
					resultCols[col_idx].Data = append(resultCols[col_idx].Data.([]float64), column.Data.([]float64)[row])
				}
			}
		}
	}

	return resultCols
}

// Casts the passed value to an interger and exits if the cast fails.
func asInt(t interface{}) int {
	val, ok := t.(int)
	if !ok {
		error_("Could not convert '%T' to integer.", t)
	}
	return val
}

// Casts the passed value to an float and exits if the cast fails.
func asFloat(t interface{}) float64 {
	val, ok := t.(float64)
	if !ok {
		error_("Could not convert '%T' to float64.", t)
	}
	return val
}

// Casts the passed value to an string and exits if the cast fails.
func asString(t interface{}) string {
	val, ok := t.(string)
	if !ok {
		error_("Could not convert '%T' to string.", t)
	}
	return val
}

// Simple log function for information.
func info(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "[INFO]: "+format+"\n", args...)
}

// Simple log function for warnigs.
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "[WARN]: "+format+"\n", args...)
}

// Simple log function for errors. Exits the program.
func error_(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[ERR]: "+format+"\n", args...)
	os.Exit(1)
}

// Checks if the passed error is nil and exits if this is the case.
func checkError(err error) {
	if err != nil {
		error_("%s", err)
	}
}

// Checks if the passed predicate returns true for all values inside the array.
func allMatch[T any](array []T, predicate func(T) bool) bool {
	for _, elm := range array {
		if !predicate(elm) {
			return false
		}
	}
	return true
}
