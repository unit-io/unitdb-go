module github.com/unit-io/unitdb-go

go 1.16

require (
	github.com/golang/protobuf v1.5.2
	github.com/unit-io/unitdb v0.1.1
	google.golang.org/grpc v1.39.0
)

replace github.com/unit-io/unitdb => /src/github.com/unit-io/unitdb
