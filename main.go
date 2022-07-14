package main

import (
	"ColumnStore/core"
	"fmt"
)

func test_session_1(cs *core.ColumnStore) {
	students_rel := cs.GetRelation("students")
    fmt.Println("========================= SESSION 1 =========================")

	fmt.Println("ID, Durchschnitt und Nachname")
	students_rel.
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Durchschnitt"}, {Name: "Nachname"}}).
        Print()

	fmt.Println("Studenten mit ID < 10")
	students_rel.
        Select(core.AttrInfo{Name: "ID"}, core.LT, 10).
        Print()

	fmt.Println("Studenten mit Durchschnitt < 2.0")
	students_rel.
        Select(core.AttrInfo{Name: "Durchschnitt"}, core.LT, 2.0).
        Print()

	fmt.Println("ID und Durchschnitt mit Nachname == Meyer")
	students_rel.
        Select(core.AttrInfo{Name: "Nachname"}, core.EQ, "Meyer").
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Durchschnitt"}}).
        Print()

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

func test_session_2(cs *core.ColumnStore) {
    fmt.Println("========================= SESSION 2 =========================")

    fmt.Println("Studenten, bei denen die Noten aufgerundet wurden (HashJoin)")
    cs.
        HashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname"}, {Name: "Nachname"}, {Name: "Beschreibung"}}).
        Print()

    fmt.Println("Studenten mit häufigen namen (NestedLoopJoin)")
    cs.
        NestedLoopJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname (first)"}, {Name: "Nachname"}, {Name: "Alter"}, {Name: "Geschlaecht"}}).
        Print()

    fmt.Println("Studenten mit häufigen namen (HashJoin)")
    cs.
        HashJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname (first)"}, {Name: "Nachname"}, {Name: "Alter"}, {Name: "Geschlaecht"}}).
        Print()

    fmt.Println("Vornamen NestedLoopJoin Nachnamen")
    cs.
        NestedLoopJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name:"ID"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID (first)"}, {Name: "Vorname"}, {Name: "Nachname"}}).
        Print()

    fmt.Println("Vornamen HashJoin Nachnamen")
    cs.
        HashJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name:"ID"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID (first)"}, {Name: "Vorname"}, {Name: "Nachname"}}).
        Print()
}

func test_session_3(cs *core.ColumnStore) {
	students_rel := cs.GetRelation("students")

    fmt.Println("========================= SESSION 3 =========================")

    fmt.Println("Vornamen IndexNestedLoopJoin Nachnamen")
    cs.
        IndexNestedLoopJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name: "ID"}).
        Scan([]core.AttrInfo{{Name: "ID (first)"}, {Name: "Vorname"}, {Name: "Nachname"}}).
        Print()

    fmt.Println("IndexScan auf Studenten/Alter mit 19")
    students_rel.
        MakeIndex(core.AttrInfo{Name: "Alter"}).
        IndexScan(core.AttrInfo{Name: "Alter"}, 19).
        Print()

    fmt.Println("Vornamen, Nachnamen, Beschreibung von Studenten HashJoin Noten IndexScan auf Beschreibung mit \"gut\"")
    cs.
        HashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname"}, {Name: "Nachname"}, {Name: "Beschreibung"}}).
        MakeIndex(core.AttrInfo{Name: "Beschreibung"}).
        IndexScan(core.AttrInfo{Name: "Beschreibung"}, "gut").
        Print()

    fmt.Println("Studenten, bei denen die Noten aufgerundet wurden (ParallelHashJoin)")
    cs.
        ParallelHashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname"}, {Name: "Nachname"}, {Name: "Beschreibung"}}).
        Print()

    fmt.Println("Studenten mit häufigen namen (ParallelHashJoin)")
    cs.
        ParallelHashJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID"}, {Name: "Vorname (first)"}, {Name: "Nachname"}, {Name: "Alter"}, {Name: "Geschlaecht"}}).
        Print()

    fmt.Println("Vornamen ParallelHashJoin Nachnamen")
    cs.
        ParallelHashJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name:"ID"}, core.EQ).
        Scan([]core.AttrInfo{{Name: "ID (first)"}, {Name: "Vorname"}, {Name: "Nachname"}}).
        Print()
}

func main() {
	var cs = new(core.ColumnStore)
    fmt.Println("Studentend Relation")
    cs.Load("students.csv", ',').Print()
    fmt.Println("Noten Relation")
    cs.Load("noten.csv", ',').Print()
    fmt.Println("Haeufige Namen Relation")
    cs.Load("haeufige_namen.csv", ',').Print()
    fmt.Println("Vornamen Relation")
    cs.Load("vornamen.csv", ',').Print()
    fmt.Println("Nachnamen Relation")
    cs.Load("nachnamen.csv", ',').Print()

    test_session_1(cs)
    test_session_2(cs)
    test_session_3(cs)
}
