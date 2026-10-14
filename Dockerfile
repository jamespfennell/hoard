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

# Tune glibc malloc so that memory freed by the xz and zstd C libraries is returned to the OS.
# Go's own allocator is unaffected; this only applies to allocations made through cgo.
#
# - arena_max=1: use a single malloc arena, so there is only one pool of retained memory.
#   Contention is negligible because the C libraries make few, large allocations.
# - mmap_threshold: allocations at least this size get their own mmap and are unmapped when
#   freed. By default glibc raises this threshold dynamically (up to 32 MiB) after large
#   frees, which puts the multi-MiB xz buffers on the heap where they are retained; setting
#   it explicitly disables that adjustment.
# - trim_threshold: free memory at the top of the heap beyond this size is returned to the OS.
#
# 131072 bytes is 128 KiB, glibc's default starting value for both thresholds. Any value well
# below the compression buffer sizes works; much smaller values send small allocations to mmap.
ENV GLIBC_TUNABLES=glibc.malloc.arena_max=1:glibc.malloc.mmap_threshold=131072:glibc.malloc.trim_threshold=131072

ENTRYPOINT ["hoard"]
