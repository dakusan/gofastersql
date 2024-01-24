//Scan columns by name instead of index

package gofastersql

import (
	"database/sql"
	"errors"
	"fmt"
)

/*
RowReaderNamed is a RowReader that scans sql rows into a struct by column name instead of index. See RowReader for more information.
Columns names are only matched on the first row scan that this RowReaderNamed is used in. Errors due to missing or ambiguous names are returned on this first row scan.
Do not scan subsequent rows that contain columns in a different order.

Column names must match either the full member name path with dots for nested structures, or just the name of the member. Top level scalars can be matched by “Param”+Base0Index.
If a conflict arises due to requesting an ambiguous member name, and there is no top level member with the name, an error is returned. A field cannot also be matched to more than one column name. See TODO note in readme for more information.
*/
type RowReaderNamed struct {
	RowReader
	hasAlreadyMatchedCols, hasError bool
}

// CreateReaderNamed creates a RowReaderNamed from the StructModel
func (sm StructModel) CreateReaderNamed() *RowReader {
	rr := &RowReaderNamed{
		RowReader: *sm.CreateReader(),
	}
	rr.rrType = rrtNamed
	return &rr.RowReader
}

func (rrn *RowReaderNamed) initNamed(rows *sql.Rows) error {
	//Quick exit conditions
	if rrn.rrType != rrtNamed {
		return errors.New("Not a RowReaderNamed")
	}
	if rrn.hasError {
		return errors.New("RowReaderNamed previously returned an error due to column name problems")
	}
	if rrn.hasAlreadyMatchedCols {
		return nil
	}

	//Get the column names
	var colNames []string
	if _colNames, err := rows.Columns(); err != nil {
		rrn.hasError, rrn.hasAlreadyMatchedCols = true, true
		return err
	} else if len(_colNames) != len(rrn.sm.fields) {
		rrn.hasError, rrn.hasAlreadyMatchedCols = true, true
		return fmt.Errorf("Number of columns in row (%d) does not match number of expected fields (%d)", len(_colNames), len(rrn.sm.fields))
	} else {
		colNames = _colNames
	}

	//Make a list of the base names and names (fix the names on top level scalar parameters)
	fieldNames := make([]string, len(colNames))
	fieldBaseNames := make([]string, len(colNames))
	{
		fields := rrn.sm.fields
		for i := range fieldNames {
			basename := fields[i].baseName
			fieldBaseNames[i] = basename
			if len(basename) == 0 {
				fieldNames[i] = rrn.sm.pointers[fields[i].pointerIndex-1].name
			} else {
				fieldNames[i] = fields[i].name
			}
		}
	}

	//Match the columns with the RowReader members
	//TODO: This process could be greatly enhanced, but this takes care of the base use cases
	fieldAlreadyUsed := make([]bool, len(fieldNames))
	colIndexToFieldIndex := make([]int, len(fieldNames))
nextCol:
	for colIndex, colName := range colNames {
		partialMatchFieldIndex, numPartialMatches := -1, 0
		for fieldIndex, fieldName := range fieldNames {
			if fieldAlreadyUsed[fieldIndex] {
				continue
			}
			if fieldName == colName {
				fieldAlreadyUsed[fieldIndex] = true
				colIndexToFieldIndex[colIndex] = fieldIndex
				continue nextCol
			}
			if fieldBaseNames[fieldIndex] == colName {
				partialMatchFieldIndex = fieldIndex
				numPartialMatches++
			}
		}
		if numPartialMatches != 1 {
			rrn.hasError, rrn.hasAlreadyMatchedCols = true, true
			return fmt.Errorf("%d matches found for column “%s”", numPartialMatches, colName)
		}
		fieldAlreadyUsed[partialMatchFieldIndex] = true
		colIndexToFieldIndex[colIndex] = partialMatchFieldIndex
	}

	//Reorganize the fields in the RowReader
	rrn.hasAlreadyMatchedCols = true
	oldFieldsList := rrn.sm.fields
	newFieldsList := make([]structField, len(oldFieldsList))
	for colIndex, fieldIndex := range colIndexToFieldIndex {
		newFieldsList[colIndex] = oldFieldsList[fieldIndex]
	}
	rrn.sm.fields = newFieldsList

	return nil
}

/*
ScanRowNamed does an sql.Rows.Scan into the outPointers variables for a single row using column names. Output variables must be pointers.

This is essentially the same as:

	ModelStruct(outPointers...).CreateReaderNamed().ScanRow(row, outPointers...)

If you are scanning a lot of rows it is recommended to use a RowReaderNamed as it bypasses mutex read locks, a few allocations, and column name matching.
In some cases this may even be slower than the native sql.Rows.Scan() method. What speeds this library up so much is the preprocessing done before the ScanRow(s) functions are called and a lot of that is lost in gofastersql.ScanRowNamed().
*/
func ScanRowNamed(rows *sql.Rows, outPointers ...any) error {
	if sm, err := scanRowModelStruct(rows, outPointers); err != nil {
		return err
	} else {
		return sm.CreateReaderNamed().DoScan(rows, outPointers, nil, false, true)
	}
}

// ScanRowNamedWErr : See ScanRowNamed and SRErr
func ScanRowNamedWErr(rowsErr SRErrStruct, outPointers ...any) error {
	if rowsErr.err != nil {
		runSafeCloseRow(rowsErr.r)
		return rowsErr.err
	}
	return ScanRowNamed(rowsErr.r, outPointers...)
}
