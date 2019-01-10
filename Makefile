GO=go

test:
	$(GO) test github.com/tobgu/qocache/...

bench:
	$(GO) test -bench=.

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build github.com/tobgu/qocache/cmd/qocache
