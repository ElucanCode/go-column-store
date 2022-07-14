package core

import (
	"os"

	"github.com/jedib0t/go-pretty/v6/table" // rendering of tables
	"github.com/jedib0t/go-pretty/v6/text"  // customization of text inside the tables
)

func (col *Column) CreateIndex() bool {
    if col.Index != nil {
        return false
    }
    col.Index = make(map[interface{}][]int)
    return true
}

func (col *Column) IndexInsert(key interface{}, i int) {
    if col.Index[key] == nil {
        col.Index[key] = make([]int, 1)
        col.Index[key][0] = i
    } else {
        col.Index[key] = append(col.Index[key], i)
    }
}

func (col *Column) IndexLookup(key interface{}) []int {
    return col.Index[key]
}

func (col *Column) isInt() bool {
    return col.Signature.Type == INT
}

func (col *Column) isFloat() bool {
    return col.Signature.Type == FLOAT
}

func (col *Column) isString() bool {
    return col.Signature.Type == STRING
}

func (col *Column) intAt(i int) int {
    return col.Data.([]int)[i]
}

func (col *Column) floatAt(i int) float64 {
    return col.Data.([]float64)[i]
}

func (col *Column) stringAt(i int) string {
    return col.Data.([]string)[i]
}

func (rel *Relation) Scan(colList []AttrInfo) Relationer {
	var rs *Relation = new(Relation)

	for _, sig := range colList {
		var col_idx = rel.findColumn(sig)
		if col_idx != -1 {
			rs.Columns = append(rs.Columns, rel.Columns[col_idx])
		} else {
			warn("Unable to find column '%s'; skipping this column.", sig.Name)
		}
	}

	return rs
}

func (rel *Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer {
	relevantCol := rel.findColumn(col)

	rs := new(Relation)
	rs.Name = "select from " + rel.Name
	// gets the columns for the created relation
	if rel.columns()[relevantCol].isInt() {
		rs.Columns = getRelevantRows(comp, asInt(compVal), rel.columns(), relevantCol)
	} else if rel.columns()[relevantCol].isFloat() {
		rs.Columns = getRelevantRows(comp, asFloat(compVal), rel.columns(), relevantCol)
	} else if rel.columns()[relevantCol].isString() {
		rs.Columns = getRelevantRows(comp, asString(compVal), rel.columns(), relevantCol)
	} else {
		return nil // unknown type
	}

	return rs
}

func (rel *Relation) Print() {
	// create configs for the table
	configs := make([]table.ColumnConfig, len(rel.Columns))
	for i := range configs {
		configs[i] = table.ColumnConfig{
			Number:      i + 1,
			Align:       text.AlignRight,
			AlignHeader: text.AlignCenter,
			AutoMerge:   false,
		}
	}

	// setup the table writer
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	t.SetColumnConfigs(configs)

	// insert table data
	t.AppendHeader(rel.getHeader())
	t.AppendRows(rel.getRows())

	t.Render()
}

func (rel *Relation) MakeIndex(indexCol AttrInfo) Relationer {
	colIdx := rel.findColumn(indexCol)

    if colIdx == -1 {
        error_("Unknown column name ", indexCol.Name)
    }

    if !rel.columns()[colIdx].CreateIndex() {
        return rel
    }


    for i := 0; i < rel.rowCount(); i++ {
		if rel.columns()[colIdx].isInt() {
            data := rel.Columns[colIdx].intAt(i)
            rel.Columns[colIdx].IndexInsert(data, i)
		} else if rel.columns()[colIdx].isFloat() {
            data := rel.Columns[colIdx].floatAt(i)
            rel.Columns[colIdx].IndexInsert(data, i)
		} else if rel.columns()[colIdx].isString() {
            data := rel.Columns[colIdx].stringAt(i)
            rel.Columns[colIdx].IndexInsert(data, i)
		}
    }
	return rel
}

func (rel *Relation) IndexScan(col AttrInfo, key interface{}) Relationer {
    colIdx := rel.findColumn(col)

    if colIdx == -1 {
        error_("Unknown column name ", col.Name)
    }

    rel.Columns[colIdx].CreateIndex()

    result := new(Relation)
    result.Name = "IndexScan on " + rel.Name
    result.Columns = make([]Column, len(rel.Columns))
    for i := range result.Columns {
        result.Columns[i].Signature = rel.columns()[i].Signature
        if result.Columns[i].isInt() {
            result.Columns[i].Data = make([]int, 0)
        } else if result.Columns[i].isFloat() {
            result.Columns[i].Data = make([]float64, 0)
        } else if result.Columns[i].isString() {
            result.Columns[i].Data = make([]string, 0)
        }
    }

    for _, rowIdx := range rel.Columns[colIdx].IndexLookup(key) {
        for i := 0; i < len(result.Columns); i++ {
            if rel.columns()[i].isInt() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), rel.Columns[i].intAt(rowIdx))
            } else if rel.columns()[i].isFloat() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), rel.Columns[i].floatAt(rowIdx))
            } else if rel.columns()[i].isString() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), rel.Columns[i].stringAt(rowIdx))
            }
        }
    }

	return result
}

func (rel *Relation) columns() []Column {
	return rel.Columns
}

// Returns the index of the column with the passed name.
func (rel *Relation) findColumn(attr AttrInfo) int {
	for idx, col := range rel.Columns {
		if col.Signature.Name == attr.Name {
			return idx
		}
	}
	return -1
}

func (rel *Relation) rowCount() int {
	if rel.Columns[0].isInt() {
		return len(rel.Columns[0].Data.([]int))
	} else if rel.Columns[0].isFloat() {
		return len(rel.Columns[0].Data.([]float64))
	} else if rel.Columns[0].isString() {
		return len(rel.Columns[0].Data.([]string))
	} else {
        error_("Unknown or unset column type.")
        return -1 // Dead code ...
    }
}

/*
-------------------------------------------------
Relation intern helper functions
-------------------------------------------------
*/

// Helper for getting the column names
func (rel *Relation) getHeader() table.Row {
	header := make(table.Row, len(rel.Columns))

	for idx, col := range rel.Columns {
		header[idx] = col.Signature.Name
	}

	return header
}

// Helper for creating Row objects needed to print a table
func (rel *Relation) getRows() []table.Row {
	col_nums := len(rel.Columns)
	// no colums -> empty return
	if col_nums <= 0 {
		info("'%s' is an empty table.", rel.Name)
		return make([]table.Row, 0)
	}

	row_num := rel.rowCount()
	rows := make([]table.Row, row_num)

	// iterate for each row
	for i := 0; i < row_num; i++ {
		row := make(table.Row, col_nums)
		// iterate over each column and get the entry at the current row
		for j, col := range rel.Columns {
			if col.isInt() {
				row[j] = col.intAt(i)
			} else if col.isFloat() {
				row[j] = col.floatAt(i)
			} else if col.isString() {
				row[j] = col.stringAt(i)
			} else {
                error_("Unknown or unset column type.")
            }
		}
		rows[i] = row
	}

	return rows
}
