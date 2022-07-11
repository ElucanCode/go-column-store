package core

/*
	The comparison operators for filter operators.
*/
type Comparison string

const (
	EQ  Comparison = "=="
	NEQ Comparison = "!="
	LT  Comparison = "<"
	GT  Comparison = ">"
	LE  Comparison = "<="
	GE  Comparison = ">="
)

/*
	The supported data types of the Column Store.
*/
type DataTypes int

const (
	INT DataTypes = iota
	FLOAT
	STRING
)

/*
	The AttrInfo structure stores metainformation about a column, e.g., the name, type and encryption used in a column.
*/
type AttrInfo struct {
	Name string
	Type DataTypes
}

/*
	The column structure stores the actual data of a column in a relation.
*/
type Column struct {
	Signature AttrInfo
	Data      interface{}
	Index     map[interface{}][]int
}

/*
	The actual structure for a Relation. It contains the name of the relation and a collection
	of the columns.
*/
type Relation struct {
	Name    string
	Columns []Column
}

/*
	The Relationer interface defines an interface for operations that can be executed on a Relation.
	These methods implements in- and output and query operators.
*/
type Relationer interface {
	Scan(colList []AttrInfo) Relationer
	Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer
	Print()
	MakeIndex(indexCol AttrInfo) Relationer
	IndexScan(col AttrInfo, key interface{}) Relationer
	// Package intern possibility to get the columns from a Relationer
	columns() []Column
	// Package intern helper to get the index of a specific column
	findColumn(col AttrInfo) int
	// Package intern helper to get the number of rows
	rowCount() int
}

/*
	ColumnStore is the main structure that contains a map of relations with their names as key.
*/
type ColumnStore struct {
	// Made private so it can't be accessed from outside.
	relations map[string]Relationer
}

/*
	ColumnStorer is the main interface in order to create relations with given name and attributes and
	to get a relation by name.
*/
type ColumnStorer interface {
	/*
		Loads a .csv file into a relation. The name of the relation corresponds to the name of the given file.
		A given seperator is used to delimit the columns.
		Moved to ColumnStorer so the Relation is created automatically instead of manually.
	*/
	Load(csvFile string, separator rune) Relationer

	/*
		Creates a new relation with given name and a signature list of the columns.
	*/
	CreateRelation(tabName string, sig []AttrInfo) Relationer

	/*
		Returns a relation by name.
	*/
	GetRelation(relName string) Relationer

	NestedLoopJoin(leftRelation string, leftCol AttrInfo, rightRelation string, rightCol AttrInfo, comp Comparison) Relationer

	IndexNestedLoopJoin(leftRelation string, leftCol AttrInfo, rightRelation string, rightCol AttrInfo) Relationer

	HashJoin(leftRelation string, leftCol AttrInfo, rightRelation string, rightCol AttrInfo, comp Comparison) Relationer
}
