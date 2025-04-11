# Hoard integration tests

This directory contains the Hoard integration tests.
These test Hoard using a real feed server and a real object storage service.
By default, the feed server and object storage are run in the test binary's process,
and the tests can simply be run using `go test`.

In the default set up, the tests interact with Hoard using the Go API.
The tests can also be run using the Hoard command line interface
    by passing the `--hoard-cmd` flag.
For example, assuming Hoard is compiled to the current directory:

    go test ./tests --hoard-cmd "./hoard" --hoard-working-dir=$PWD

To run the integration tests without compiling Hoard first, the `./hoard`
    flag value can be replaced with `go run`:

    go test ./tests --hoard-cmd "go run cmd/hoard.go" --hoard-working-dir=$PWD

Note that this is very slow as `go run` is invoked for every single API intertask.
Finally, to run the tests using the Hoard Docker image:

    go test ./tests 
        --hoard-cmd "docker run --volume /tmp/hoard_tests:/tmp/hoard_tests --network host jamespfennell/hoard:latest"
        --hoard-cleanup-optional

For the Docker case, we need to mount the tempory directory in the image
    so that we can pass in Hoard config files and inspect the local results of Hoard.
We use the host network so that Hoard-in-Docker can contact the feed server
    and object storage server running inside the test binary.
We pass `--hoard-cleanup-optional` as in the Docker case, the test runner
    is unable to delete any files created inside the image.

