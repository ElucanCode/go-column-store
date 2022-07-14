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

func (cs *ColumnStore) NestedLoopJoin(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, comp Comparison) Relationer {
    // Basic setup
    leftRel := cs.GetRelation(leftRelation)
    rightRel := cs.GetRelation(rightRelation)
    lidx := leftRel.findColumn(leftColumn)
    ridx := rightRel.findColumn(rightColumn)
    lsig := leftRel.columns()[lidx].Signature

    if lsig.Type != rightRel.columns()[ridx].Signature.Type {
        error_("Not matching types for nested loop join.")
    }

    result := prepareJoinResult("NestedLoopJoin", leftRel, rightRel, lidx, ridx)

    // Perform the join
    for i := 0; i < leftRel.rowCount(); i++ {
        var predicate interface{}
        if lsig.Type == INT {
            predicate = comparator(comp, leftRel.columns()[lidx].Data.([]int)[i])
        } else if lsig.Type == FLOAT {
            predicate = comparator(comp, leftRel.columns()[lidx].Data.([]float64)[i])
        } else {
            predicate = comparator(comp, leftRel.columns()[lidx].Data.([]string)[i])
        }
        for j := 0; j < rightRel.rowCount(); j++ {
            if lsig.Type == INT && predicate.(func(int) bool)(rightRel.columns()[ridx].Data.([]int)[j]) {
                join(leftRel, rightRel, result, i, j)
            } else if lsig.Type == FLOAT && predicate.(func(float64) bool)(rightRel.columns()[ridx].Data.([]float64)[j]) {
                join(leftRel, rightRel, result, i, j)
            } else if lsig.Type == STRING && predicate.(func(string) bool)(rightRel.columns()[ridx].Data.([]string)[j]) {
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
    lsig := leftRel.columns()[lidx].Signature

    if lsig.Type != rightRel.columns()[ridx].Signature.Type {
        error_("Not matching types for Index nested loop join.")
    }

    rightRel.MakeIndex(rightColumn)

    result := prepareJoinResult("IndexNestedLoopJoin", leftRel, rightRel, lidx, ridx)

    for i := 0; i < leftRel.rowCount(); i++ {
        var value interface{}
        if lsig.Type == INT {
            value = leftRel.columns()[lidx].Data.([]int)[i]
        } else if lsig.Type == FLOAT {
            value = leftRel.columns()[lidx].Data.([]float64)[i]
        } else {
            value = leftRel.columns()[lidx].Data.([]string)[i]
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
        if s.ssig.Type == INT {
            hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]int)[i])
        } else if s.ssig.Type == FLOAT {
            hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]float64)[i])
        } else {
            hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]string)[i])
        }
        hashed = abs(hashed % len(s.hashTable))
        for _, j := range s.hashTable[hashed] {
            if s.fsig.Type == INT {
                predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]int)[j])
                if predicate(s.secondRel.columns()[s.sidx].Data.([]int)[i]) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
            } else if s.fsig.Type == FLOAT {
                predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]float64)[j])
                if predicate(s.secondRel.columns()[s.sidx].Data.([]float64)[i]) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
            } else {
                predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]string)[j])
                if predicate(s.secondRel.columns()[s.sidx].Data.([]string)[i]) {
                    join(s.firstRel, s.secondRel, s.result, j, i)
                }
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

            if s.ssig.Type == INT {
                hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]int)[i])
            } else if s.ssig.Type == FLOAT {
                hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]float64)[i])
            } else {
                hashed = s.hash(s.secondRel.columns()[s.sidx].Data.([]string)[i])
            }
            hashed = abs(hashed % len(s.hashTable))
            for _, j := range s.hashTable[hashed] {
                if s.fsig.Type == INT {
                    predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]int)[j])
                    if predicate(s.secondRel.columns()[s.sidx].Data.([]int)[i]) {
                        found = append(found, ij { i: i, j: j })
                    }
                } else if s.fsig.Type == FLOAT {
                    predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]float64)[j])
                    if predicate(s.secondRel.columns()[s.sidx].Data.([]float64)[i]) {
                        found = append(found, ij { i: i, j: j })
                    }
                } else {
                    predicate := comparator(comp, s.firstRel.columns()[s.fidx].Data.([]string)[j])
                    if predicate(s.secondRel.columns()[s.sidx].Data.([]string)[i]) {
                        found = append(found, ij { i: i, j: j })
                    }
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
    fidx        int
    sidx        int
    fsig        AttrInfo
    ssig        AttrInfo
}

func (cs *ColumnStore) hashJoinSetup(leftRelation string, leftColumn AttrInfo, rightRelation string, rightColumn AttrInfo, resultName string) setup {
    // Basic setup
    leftRel := cs.GetRelation(leftRelation)
    rightRel := cs.GetRelation(rightRelation)
    lidx := leftRel.findColumn(leftColumn)
    ridx := rightRel.findColumn(rightColumn)

    if leftRel.columns()[lidx].Signature.Type != rightRel.columns()[ridx].Signature.Type {
        error_("Not matching types for hash join.")
    }

    // Get the smaller relation
    var firstRel, secondRel Relationer
    var fidx, sidx int
    var fsig, ssig AttrInfo
    if leftRel.rowCount() < rightRel.rowCount() {
        firstRel, secondRel = leftRel, rightRel
        fidx, sidx = lidx, ridx
        fsig, ssig = leftRel.columns()[lidx].Signature, rightRel.columns()[ridx].Signature
    } else {
        firstRel, secondRel = rightRel, leftRel
        fidx, sidx = ridx, lidx
        fsig, ssig = rightRel.columns()[ridx].Signature, leftRel.columns()[lidx].Signature
    }

    // create the hash table, the length should be done differently
    var hashTable = make([][]int, firstRel.rowCount())
    hash := createHashFunction(fsig)
    for i := 0; i < firstRel.rowCount(); i++ {
        var hashed int
        if fsig.Type == INT {
            hashed = hash(firstRel.columns()[fidx].Data.([]int)[i])
        } else if fsig.Type == FLOAT {
            hashed = hash(firstRel.columns()[fidx].Data.([]float64)[i])
        } else {
            hashed = hash(firstRel.columns()[fidx].Data.([]string)[i])
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
        fidx: fidx,
        sidx: sidx,
        fsig: fsig,
        ssig: ssig,
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
        if result.Columns[i].Signature.Type == INT {
            result.Columns[i].Data = make([]int, 0)
        } else if result.Columns[i].Signature.Type == FLOAT {
            result.Columns[i].Data = make([]float64, 0)
        } else {
            result.Columns[i].Data = make([]string, 0)
        }
    }

    return result
}

func join(firstRel, secondRel Relationer, result Relation, firstIndex, secondIndex int) {
    for i := 0; i < len(result.Columns); i++ {
        if i < len(firstRel.columns()) {
            if result.Columns[i].Signature.Type == INT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), firstRel.columns()[i].Data.([]int)[firstIndex])
            } else if result.Columns[i].Signature.Type == FLOAT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), firstRel.columns()[i].Data.([]float64)[firstIndex])
            } else {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), firstRel.columns()[i].Data.([]string)[firstIndex])
            }
        } else {
            i2 := i - len(firstRel.columns())
            if result.Columns[i].Signature.Type == INT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]int), secondRel.columns()[i2].Data.([]int)[secondIndex])
            } else if result.Columns[i].Signature.Type == FLOAT {
                result.Columns[i].Data = append(result.Columns[i].Data.([]float64), secondRel.columns()[i2].Data.([]float64)[secondIndex])
            } else {
                result.Columns[i].Data = append(result.Columns[i].Data.([]string), secondRel.columns()[i2].Data.([]string)[secondIndex])
            }
        }
    }
}
