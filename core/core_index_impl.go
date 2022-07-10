package core

func NewIndex() *Index {
    index := new(Index)
    index.entries = make(map[interface{}][]int)
    return index
}

func (idx *Index) Insert(key interface{}, i int) {
    if idx.entries[key] == nil {
        idx.entries[key] = make([]int, 1)
        idx.entries[key][0] = i
    } else {
        idx.entries[key] = append(idx.entries[key], i)
    }
}

func (idx *Index) Find(key interface{}) []int {
    return idx.entries[key]
}
