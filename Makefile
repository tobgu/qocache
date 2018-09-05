
test:
	go test github.com/tobgu/qocache/...

fmt:
	go fmt ./...

vet:
	go vet ./...

build:
	go build github.com/tobgu/qocache/cmd/qocache
