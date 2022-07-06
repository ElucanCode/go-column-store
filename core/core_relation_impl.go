package core

import (
	"os"

	"github.com/jedib0t/go-pretty/v6/table" // rendering of tables
	"github.com/jedib0t/go-pretty/v6/text"  // customization of text inside the tables
)

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

    return nil
}

func (rel *Relation) IndexScan(key interface{}) Relationer {

    return nil
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
