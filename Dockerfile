from golang:1.11.4

RUN go get github.com/tobgu/qocache/cmd/qocache

CMD ["sh", "-c", "qocache"]
