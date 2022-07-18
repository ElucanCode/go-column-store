.PHONY: run

run:
	go mod tidy
	go build .
	@echo ""
	./ColumnStore

bench:
	go mod tidy
	go test -bench=. -benchtime=1000000x

clean:
	rm ColumnStore
