package main

import (
	"ColumnStore/core"
	"fmt"
)

func main() {
	var cs = new(core.ColumnStore)

	students_rel := cs.Load("students.csv", ',')

	fmt.Println("Ganze Studenten-Relation")
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

    fmt.Println("Ganze Noten Relation")
    cs.Load("noten.csv", ',').Print()

    fmt.Println("Studenten, bei denen die Noten aufgerundet wurden (HashJoin)")
    cs.HashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname"}, {Name: "Nachname"}, {Name: "Beschreibung"}}).
        Print()

    fmt.Println("Häufige Namen")
    cs.Load("haeufige_namen.csv", ',').Print()

    fmt.Println("Studenten mit häufigen namen (NestedLoopJoin)")
    cs.NestedLoopJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ).Print()

    fmt.Println("Studenten mit häufigen namen (HashJoin)")
    cs.HashJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ).Print()

    fmt.Println("============================================================")

    cs.Load("test1.csv", ',')
    cs.Load("test2.csv", ',')
    cs.HashJoin("test1", core.AttrInfo{Name:"ID"}, "test2", core.AttrInfo{Name:"ID"}, core.EQ).Print()
    cs.NestedLoopJoin("test1", core.AttrInfo{Name:"ID"}, "test2", core.AttrInfo{Name:"ID"}, core.EQ).Print()
    cs.IndexNestedLoopJoin("test1", core.AttrInfo{Name:"ID"}, "test2", core.AttrInfo{Name: "ID"}).Print()

    fmt.Println("============================================================")

    students_rel.MakeIndex(core.AttrInfo{Name: "Alter"}).IndexScan(core.AttrInfo{Name: "Alter"}, 19).Print()
    cs.HashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname"}, {Name: "Nachname"}, {Name: "Beschreibung"}}).
        MakeIndex(core.AttrInfo{Name: "Beschreibung"}).
        IndexScan(core.AttrInfo{Name: "Beschreibung"}, "gut").
        Print()
}
