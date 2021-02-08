FROM golang:1.14 AS builder

COPY . /hoard

WORKDIR /hoard

RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux go build cmd/hoard.go

ENTRYPOINT ["/hoard/hoard"]
