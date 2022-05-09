.PHONY: run

run:
	go mod tidy
	go build .
	@echo ""
	./ColumnStore

clean:
	rm ColumnStore
