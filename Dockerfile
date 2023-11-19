FROM golang:1.21.4

WORKDIR /qocache
COPY . .
RUN make build

CMD ["sh", "-c", "/qocache/qocache"]
