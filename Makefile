GO=go

test:
	$(GO) test github.com/tobgu/qocache/...

lint:
	~/go/bin/golangci-lint run ./...

ci: test lint

bench:
	$(GO) test -bench=.

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build github.com/tobgu/qocache/cmd/qocache

dev-deps:
	mkdir -p ~/go/bin
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ~/go/bin v1.55.2
