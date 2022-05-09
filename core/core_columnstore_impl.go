package core

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

func (cs *ColumnStore) Load(csvFile string, separator rune) Relationer {
	file, err := os.Open(csvFile)
	checkError(err)
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = separator
	// Relation header
	header, err := reader.Read()
	checkError(err)
	// Relation data
	data, err := reader.ReadAll()
	checkError(err)

	// regexes to check if the column contains an interger or a float
	intRegex := regexp.MustCompile("^[0-9]+$")
	floatRegex := regexp.MustCompile("^[0-9]+.[0-9]*$")

	attrInfos := make([]AttrInfo, len(header))
	for idx := range attrInfos { // set the column signatures
		attrInfos[idx].Name = header[idx]
		attrInfos[idx].Enc = NOCOMP
		if allMatch(data, func(row []string) bool { return intRegex.Match([]byte(row[idx])) }) {
			// every entry in this column is an interger
			attrInfos[idx].Type = INT
		} else if allMatch(data, func(row []string) bool { return floatRegex.Match([]byte(row[idx])) }) {
			// ervery entry in this column is a float
			attrInfos[idx].Type = FLOAT
		} else {
			// every thing else is just a string
			attrInfos[idx].Type = STRING
		}
	}

	tableName := csvFile[:len(csvFile)-len(filepath.Ext(csvFile))]
	rel := cs.CreateRelation(tableName, attrInfos)

	for colIdx, col := range rel.columns() {
		// create column data array
		rel.columns()[colIdx].Data = arrayForType(col.Signature.Type, len(data))

		// store column data
		for rowIdx, row := range data {
			if col.Signature.Type == INT {
				val, err := strconv.Atoi(row[colIdx])
				checkError(err)
				rel.columns()[colIdx].Data.([]int)[rowIdx] = val
			} else if col.Signature.Type == FLOAT {
				val, err := strconv.ParseFloat(row[colIdx], 64)
				checkError(err)
				rel.columns()[colIdx].Data.([]float64)[rowIdx] = val
			} else {
				rel.columns()[colIdx].Data.([]string)[rowIdx] = row[colIdx]
			}
		}
	}

	return rel
}

func (cs *ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	// initialize a new map when no one exists
	if cs.relations == nil {
		cs.relations = make(map[string]Relationer)
	}

	// create an appropriate number of columns and asign the signatures
	var cols []Column = make([]Column, len(sig))
	for i, s := range sig {
		cols[i].Signature = s
	}

	// create a new relation and asign the columns
	var rs *Relation = new(Relation)
	rs.Name = tabName
	rs.Columns = cols

	// save and return the relation
	cs.relations[tabName] = rs
	return rs
}

func (cs *ColumnStore) GetRelation(relName string) Relationer {
	rel, ok := cs.relations[relName]
	if !ok {
		error_("No relation with name '%s'", relName)
	}
	return rel
}
