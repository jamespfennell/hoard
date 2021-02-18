FROM golang:1.16 AS builder

WORKDIR /hoard

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux go build cmd/hoard.go

# Only build the image if the tests pass
RUN go test ./...

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:buster-curl

COPY --from=builder /hoard/hoard /hoard/

WORKDIR /hoard

ENTRYPOINT ["/hoard/hoard"]
