package main

import (
	"ColumnStore/core"
	"testing"
)

func BenchmarkHashJoin_StudentsNoten(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("students.csv", ',')
    cs.Load("noten.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.HashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE)
    }
}

func BenchmarkHashJoin_StudentsNamen(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("students.csv", ',')
    cs.Load("haeufige_namen.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.HashJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ)
    }
}

func BenchmarkHashJoin_VornamenNachnamen(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("vornamen.csv", ',')
    cs.Load("nachnamen.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.HashJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name:"ID"}, core.EQ)
    }
}

func BenchmarkParallelHashJoin_StudentsNoten(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("students.csv", ',')
    cs.Load("noten.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.ParallelHashJoin("students", core.AttrInfo{Name: "Durchschnitt"}, "noten", core.AttrInfo{Name: "Note"}, core.LE)
    }
}

func BenchmarkParallelHashJoin_StudentsNamen(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("students.csv", ',')
    cs.Load("haeufige_namen.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.ParallelHashJoin("students", core.AttrInfo{Name: "Vorname"}, "haeufige_namen", core.AttrInfo{Name: "Vorname"}, core.EQ)
    }
}

func BenchmarkParallelHashJoin_VornamenNachnamen(b *testing.B) {
	var cs = new(core.ColumnStore)
    cs.Load("vornamen.csv", ',')
    cs.Load("nachnamen.csv", ',')
    for i := 0; i < b.N; i++ {
        cs.ParallelHashJoin("vornamen", core.AttrInfo{Name:"ID"}, "nachnamen", core.AttrInfo{Name:"ID"}, core.EQ)
    }
}
