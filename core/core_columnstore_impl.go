package core

import (
	"encoding/csv"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
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
			if col.isInt() {
				val, err := strconv.Atoi(row[colIdx])
				checkError(err)
				rel.columns()[colIdx].Data.([]int)[rowIdx] = val
			} else if col.isFloat() {
				val, err := strconv.ParseFloat(row[colIdx], 64)
				checkError(err)
				rel.columns()[colIdx].Data.([]float64)[rowIdx] = val
			} else if col.isString() {
				rel.columns()[colIdx].Data.([]string)[rowIdx] = row[colIdx]
			} else {
                error_("Unknown or unset column type.")
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

func (cs *ColumnStore) NestedLoopJoin(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, comp Comparison) Relationer {
    // Basic setup
    leftRel := cs.GetRelation(leftRelation)
    rightRel := cs.GetRelation(rightRelation)
    lidx := leftRel.findColumn(leftColumn)
    ridx := rightRel.findColumn(rightColumn)
    lcol := leftRel.columns()[lidx]

    if lcol.Signature.Type != rightRel.columns()[ridx].Signature.Type {
        error_("Not matching types for nested loop join.")
    }

    result := prepareJoinResult("NestedLoopJoin", leftRel, rightRel, lidx, ridx)

    // Perform the join
    for i := 0; i < leftRel.rowCount(); i++ {
        var predicate interface{}
        if lcol.isInt() {
            predicate = comparator(comp, lcol.intAt(i))
        } else if lcol.isFloat() {
            predicate = comparator(comp, lcol.floatAt(i))
        } else if lcol.isString() {
            predicate = comparator(comp, lcol.stringAt(i))
        } else {
            error_("Unknown or unset column type.")
        }
        for j := 0; j < rightRel.rowCount(); j++ {
            if lcol.isInt() && predicate.(func(int) bool)(rightRel.columns()[ridx].intAt(j)) {
                join(leftRel, rightRel, result, i, j)
            } else if lcol.isFloat() && predicate.(func(float64) bool)(rightRel.columns()[ridx].floatAt(j)) {
                join(leftRel, rightRel, result, i, j)
            } else if lcol.isString() && predicate.(func(string) bool)(rightRel.columns()[ridx].stringAt(j)) {
                join(leftRel, rightRel, result, i, j)
            }
        }
    }

    return &result
}

func (cs *ColumnStore) IndexNestedLoopJoin(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo) Relationer {
    leftRel := cs.GetRelation(leftRelation)
    rightRel := cs.GetRelation(rightRelation)
    lidx := leftRel.findColumn(leftColumn)
    ridx := rightRel.findColumn(rightColumn)
    lcol := leftRel.columns()[lidx]

    if lcol.Signature.Type != rightRel.columns()[ridx].Signature.Type {
        error_("Not matching types for Index nested loop join.")
    }

    rightRel.MakeIndex(rightColumn)

    result := prepareJoinResult("IndexNestedLoopJoin", leftRel, rightRel, lidx, ridx)

    for i := 0; i < leftRel.rowCount(); i++ {
        var value interface{}
        if lcol.isInt() {
            value = lcol.intAt(i)
        } else if lcol.isFloat() {
            value = lcol.floatAt(i)
        } else if lcol.isString() {
            value = lcol.stringAt(i)
        } else {
            error_("Unknown or unset column type.")
        }
        for _, row := range rightRel.columns()[ridx].IndexLookup(value) {
            join(leftRel, rightRel, result, i, row)
        }
    } 

    return &result
}

func (cs *ColumnStore) HashJoin(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, comp Comparison) Relationer {
    s := cs.hashJoinSetup(leftRelation, leftColumn, rightRelation, rightColumn, "HashJoin")

    // Perform the join
    for i := 0; i < s.secondRel.rowCount(); i++ {
        var hashed int
        if s.scol.isInt() {
            hashed = s.hash(s.scol.intAt(i))
        } else if s.scol.isFloat() {
            hashed = s.hash(s.scol.floatAt(i))
        } else if s.scol.isString() {
            hashed = s.hash(s.scol.stringAt(i))
        } else {
            error_("Unknown or unset column type.")
        }
        hashed = abs(hashed % len(s.hashTable))
        for _, j := range s.hashTable[hashed] {
            if s.fcol.isInt() {
                predicate := comparator(comp, s.fcol.intAt(j))
                if predicate(s.scol.intAt(i)) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
            } else if s.fcol.isFloat() {
                predicate := comparator(comp, s.scol.floatAt(j))
                if predicate(s.scol.floatAt(i)) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
            } else if s.fcol.isString() {
                predicate := comparator(comp, s.fcol.stringAt(j))
                if predicate(s.scol.stringAt(i)) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
            } else {
                error_("Unknown or unset column type.")
            }
        }
    }

    return &s.result
}

func (cs *ColumnStore) ParallelHashJoin(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, comp Comparison) Relationer {
    s := cs.hashJoinSetup(leftRelation, leftColumn, rightRelation, rightColumn, "HashJoin")

    type ij struct {
        i int
        j int
    }
    n := s.secondRel.rowCount()

    rows := make(chan []ij, n)
    defer close(rows)

    wg := new(sync.WaitGroup)
    wg.Add(n)

    // Perform the join
    for i := 0; i < s.secondRel.rowCount(); i++ {
        go func(i int, wg *sync.WaitGroup) {
            var hashed int
            found := make([]ij, 0)

            if s.scol.isInt() {
                hashed = s.hash(s.scol.intAt(i))
            } else if s.scol.isFloat() {
                hashed = s.hash(s.scol.floatAt(i))
            } else if s.scol.isString() {
                hashed = s.hash(s.scol.stringAt(i))
            } else {
                error_("Unknown or unset column type.")
            }
            hashed = abs(hashed % len(s.hashTable))
            for _, j := range s.hashTable[hashed] {
                if s.fcol.isInt() {
                    predicate := comparator(comp, s.fcol.intAt(j))
                    if predicate(s.scol.intAt(i)) {
                        found = append(found, ij { i: i, j: j })
                    }
                } else if s.fcol.isFloat() {
                    predicate := comparator(comp, s.scol.floatAt(j))
                    if predicate(s.scol.floatAt(i)) {
                        found = append(found, ij { i: i, j: j })
                    }
                } else if s.fcol.isString() {
                    predicate := comparator(comp, s.fcol.stringAt(j))
                    if predicate(s.scol.stringAt(i)) {
                        found = append(found, ij { i: i, j: j })
                    }
                } else {
                    error_("Unknown or unset column type.")
                }
            }

            rows <- found
            wg.Wait()
        }(i, wg)
    }

    for i := 0; i < n; i++ {
        for _, ij := range <- rows {
            join(s.firstRel, s.secondRel, s.result, ij.j, ij.i)
        }
        wg.Done()
    }

    return &s.result
}

/*
-------------------------------------------------
ColumnStore intern helper functions
-------------------------------------------------
*/

type setup struct {
    firstRel    Relationer
    secondRel   Relationer
    result      Relation
    hash        func(interface{}) int
    hashTable   [][]int
    fcol        Column
    scol        Column
}

func (cs *ColumnStore) hashJoinSetup(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, resultName string) setup {
    // Basic setup
    leftRel := cs.GetRelation(leftRelation)
    rightRel := cs.GetRelation(rightRelation)
    lidx := leftRel.findColumn(leftColumn)
    ridx := rightRel.findColumn(rightColumn)
    lcol := leftRel.columns()[lidx]
    rcol := rightRel.columns()[ridx]

    if lcol.Signature.Type != rcol.Signature.Type {
        error_("Not matching types for hash join.")
    }

    // Get the smaller relation
    var firstRel, secondRel Relationer
    var fidx, sidx int
    var fcol, scol Column
    if leftRel.rowCount() < rightRel.rowCount() {
        firstRel, secondRel = leftRel, rightRel
        fidx, sidx = lidx, ridx
        fcol, scol = lcol, rcol
    } else {
        firstRel, secondRel = rightRel, leftRel
        fidx, sidx = ridx, lidx
        fcol, scol = rcol, lcol
    }

    // create the hash table, the length should be done differently
    var hashTable = make([][]int, firstRel.rowCount())
    hash := createHashFunction(fcol.Signature)
    for i := 0; i < firstRel.rowCount(); i++ {
        var hashed int
        if fcol.isInt() {
            hashed = hash(fcol.intAt(i))
        } else if fcol.isFloat() {
            hashed = hash(fcol.floatAt(i))
        } else if fcol.isString() {
            hashed = hash(fcol.stringAt(i))
        } else {
            error_("Unknown or unset column type.")
        }
        hashed = abs(hashed % len(hashTable))
        hashTable[hashed] = append(hashTable[hashed], i)
    }

    result := prepareJoinResult(resultName, firstRel, secondRel, fidx, sidx)
    return setup {
        firstRel: firstRel,
        secondRel: secondRel,
        result: result,
        hash: hash,
        hashTable: hashTable,
        fcol: fcol,
        scol: scol,
    }
}

func createHashFunction(info AttrInfo) func(interface{}) int {
    switch info.Type {
    case INT:
        return func(in interface{}) int {
            return asInt(in)
        }
    case FLOAT:
        return func(in interface{}) int {
            return int(math.Round(asFloat(in)))
        }
    default:
        return func(in interface{}) int {
            h := fnv.New64a()
            h.Write([]byte(asString(in)))
            return int(h.Sum64())
        }
    }
}

func prepareJoinResult(name string, first, second Relationer, fidx, sidx int) Relation {
    // create relation
    var result Relation
    result.Name = name
    result.Columns = make([]Column, len(first.columns()) + len(second.columns()))

    // Setup columns
    for i := 0; i < len(result.Columns); i++ {
        if i < len(first.columns()) {
            result.Columns[i].Signature = first.columns()[i].Signature
            if i == fidx {
                result.Columns[i].Signature.Name += " (first)"
            }
        } else {
            i2 := i - len(first.columns())
            result.Columns[i].Signature = second.columns()[i2].Signature
            if i2 == sidx {
                result.Columns[i].Signature.Name += " (second)"
            }
        }
        if result.Columns[i].isInt() {
            result.Columns[i].Data = make([]int, 0)
        } else if result.Columns[i].isFloat() {
            result.Columns[i].Data = make([]float64, 0)
        } else if result.Columns[i].isString() {
            result.Columns[i].Data = make([]string, 0)
        } else {
            error_("Unknown or unset column type.")
        }
    }

    return result
}

func join(firstRel, secondRel Relationer, result Relation, firstIndex, secondIndex int) {
    for i := 0; i < len(result.Columns); i++ {
        if i < len(firstRel.columns()) {
            if result.Columns[i].isInt() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), firstRel.columns()[i].intAt(firstIndex))
            } else if result.Columns[i].isFloat() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), firstRel.columns()[i].floatAt(firstIndex))
            } else if result.Columns[i].isString() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), firstRel.columns()[i].stringAt(firstIndex))
            } else {
                error_("Unknown or unset column type.")
            }
        } else {
            i2 := i - len(firstRel.columns())
            if result.Columns[i].isInt() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), secondRel.columns()[i2].intAt(secondIndex))
            } else if result.Columns[i].isFloat() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), secondRel.columns()[i2].floatAt(secondIndex))
            } else if result.Columns[i].isString() {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), secondRel.columns()[i2].stringAt(secondIndex))
            } else {
                error_("Unknown or unset column type.")
            }
        }
    }
}
