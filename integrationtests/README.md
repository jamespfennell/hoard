# Hoard integration tests

The Hoard integration tests test Hoard using a real feed server
and a real object storage service.
By default, the feed server and object storage are run in-process,
and means the tests can be run using `go test`