FROM golang:1.15 AS builder

COPY . /hoard
WORKDIR /hoard

RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux go build cmd/hoard.go

FROM ubuntu:20.04

COPY --from=builder /hoard/hoard /hoard/

RUN mkdir /hoard/workspace

ENTRYPOINT ["/hoard/hoard"]
