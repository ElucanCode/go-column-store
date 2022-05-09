package main

import (
	"ColumnStore/core"
	"fmt"
)

func main() {
	var cs = new(core.ColumnStore)

	students_rel := cs.Load("students.csv", ',')

	fmt.Println("Ganze Relation")
	students_rel.Print()

	fmt.Println("ID, Durchschnitt und Nachname")
	students_rel.Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Durchschnitt"}, {Name: "Nachname"}}).Print()

	fmt.Println("Studenten mit ID < 10")
	students_rel.Select(core.AttrInfo{Name: "ID"}, core.LT, 10).Print()

	fmt.Println("Studenten mit Durchschnitt < 2.0")
	students_rel.Select(core.AttrInfo{Name: "Durchschnitt"}, core.LT, 2.0).Print()

	fmt.Println("ID und Durchschnitt mit Nachname == Meyer")
	students_rel.Select(core.AttrInfo{Name: "Nachname"}, core.EQ, "Meyer").Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Durchschnitt"}}).Print()

	fmt.Println("Studenten mit 1.2 <= Durchschnitt < 2.5 und 21 < Alter <= 24")
	students_rel.
		Select(core.AttrInfo{Name: "Durchschnitt"}, core.GE, 1.2).
		Select(core.AttrInfo{Name: "Durchschnitt"}, core.LT, 2.5).
		Select(core.AttrInfo{Name: "Alter"}, core.GT, 21).
		Select(core.AttrInfo{Name: "Alter"}, core.LE, 24).
		Print()

	fmt.Println("ID und Name mit 1.2 <= Durchschnitt < 2.5 und 21 < Alter <= 24")
	students_rel.
		Select(core.AttrInfo{Name: "Durchschnitt"}, core.GE, 1.2).
		Select(core.AttrInfo{Name: "Durchschnitt"}, core.LT, 2.5).
		Select(core.AttrInfo{Name: "Alter"}, core.GT, 21).
		Select(core.AttrInfo{Name: "Alter"}, core.LE, 24).
		Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Nachname"}, {Name: "Vorname"}}).
		Print()

}
