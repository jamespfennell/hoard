FROM golang:1.24 AS builder

WORKDIR /hoard

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

# The .git directory is not in the build context, so the commit is passed in as a build arg
ARG GIT_COMMIT

RUN go build \
    -ldflags "-X github.com/jamespfennell/hoard/internal/server.buildTimeUnix=$(date +'%s') -X github.com/jamespfennell/hoard/internal/server.gitCommit=${GIT_COMMIT}" \
    cmd/hoard.go

# Only build the image if the tests pass
RUN go test ./...

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:stable-curl

COPY --from=builder /hoard/hoard /usr/bin

WORKDIR /hoard

ENV MALLOC_ARENA_MAX=1

ENTRYPOINT ["hoard"]
