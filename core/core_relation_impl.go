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
	if rel.columns()[relevantCol].Signature.Type == INT {
		rs.Columns = getRelevantRows(comp, asInt(compVal), rel.columns(), relevantCol)
	} else if rel.columns()[relevantCol].Signature.Type == FLOAT {
		rs.Columns = getRelevantRows(comp, asFloat(compVal), rel.columns(), relevantCol)
	} else if rel.columns()[relevantCol].Signature.Type == STRING {
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

	colsig := rel.columns()[colIdx].Signature

    for i := 0; i < rel.rowCount(); i++ {
		if colsig.Type == INT {
            data := rel.Columns[colIdx].Data.([]int)[i]
            rel.Columns[colIdx].IndexInsert(data, i)
		} else if colsig.Type == FLOAT {
            data := rel.Columns[colIdx].Data.([]float64)[i]
            rel.Columns[colIdx].IndexInsert(data, i)
		} else if colsig.Type == STRING {
            data := rel.Columns[colIdx].Data.([]string)[i]
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
        if result.Columns[i].Signature.Type == INT {
            result.Columns[i].Data = make([]int, 0)
        } else if result.Columns[i].Signature.Type == FLOAT {
            result.Columns[i].Data = make([]float64, 0)
        } else if result.Columns[i].Signature.Type == STRING {
            result.Columns[i].Data = make([]string, 0)
        }
    }

    for _, rowIdx := range rel.Columns[colIdx].IndexLookup(key) {
        for i := 0; i < len(result.Columns); i++ {
            if rel.columns()[i].Signature.Type == INT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), rel.Columns[i].Data.([]int)[rowIdx])
            } else if rel.columns()[i].Signature.Type == FLOAT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), rel.Columns[i].Data.([]float64)[rowIdx])
            } else if rel.columns()[i].Signature.Type == STRING {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), rel.Columns[i].Data.([]string)[rowIdx])
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
	if rel.Columns[0].Signature.Type == INT {
		return len(rel.Columns[0].Data.([]int))
	} else if rel.Columns[0].Signature.Type == FLOAT {
		return len(rel.Columns[0].Data.([]float64))
	} else {
		return len(rel.Columns[0].Data.([]string))
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
			if col.Signature.Type == INT {
				row[j] = col.Data.([]int)[i]
			} else if col.Signature.Type == FLOAT {
				row[j] = col.Data.([]float64)[i]
			} else {
				row[j] = col.Data.([]string)[i]
			}

		}
		rows[i] = row
	}

	return rows
}
