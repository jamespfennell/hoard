FROM golang:1.24 AS builder

WORKDIR /hoard

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN go build \
    -ldflags "-X github.com/jamespfennell/hoard/internal/server.buildTimeUnix=$(date +'%s')" \
    cmd/hoard.go

# Only build the image if the tests pass
RUN go test ./...

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:stable-curl

COPY --from=builder /hoard/hoard /usr/bin

WORKDIR /hoard

ENTRYPOINT ["hoard"]
