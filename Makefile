.PHONY: run

run:
	go build .
	@echo ""
	./ColumnStore

clean:
	rm ColumnStore
