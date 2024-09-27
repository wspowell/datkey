build:
	go build ./...

test: build
	go test -race -count=1 ./...

lint:
	golangci-lint cache clean
	golangci-lint run 

bench:
	go test -benchmem -bench . ./lib/errors
	go test -benchmem -bench . ./