# Unitdb go client [![GoDoc](https://godoc.org/github.com/unit-io/unitdb-go?status.svg)](https://godoc.org/github.com/unit-io/unitdb-go)

## The Unitdb messaging system is an open source messaging system for microservice, and real-time internet connected devices. The Unitdb messaging API is built for speed and security.

The Unitdb is a real-time messaging system for microservices, and real-tme internet connected devices, it is based on GRPC communication. The Unitdb messaging system satisfy the requirements for low latency and binary messaging, it is perfect messaging system for internet connected devices.

## Quick Start
To build [unitdb](https://github.com/unit-io/unitdb) from source code use go get command and copy unitdb.conf to the path unitdb binary is placed.

> go get -u github.com/unit-io/unitdb/server

The server needs an encryption key of its own: set `encryption_config`'s `key` in unitdb.conf, or the `UNITDB_ENCRYPTION_KEY` environment variable, to 32 random characters, for example the output of `openssl rand -base64 24`.

### Usage
Detailed API documentation is available using the [godoc.org](https://godoc.org/github.com/unit-io/unitdb-go) service.

Make use of the client by importing it in your Go client source code. For example,

import "github.com/unit-io/unitdb-go"

Samples are available in the examples directory for reference. To build unitdb server from latest source code use "replace" in go.mod to point to your local module.

```golang
go mod edit -replace github.com/unit-io/unitdb=$GOPATH/src/github.com/unit-io/unitdb
```

### Topic keys and the insecure flag
A client publishes and subscribes with topic keys: it prefixes a topic with a key, as in `key/teams.alpha.ch1`. A client with a primary client ID requests keys from the server by publishing `[{"topic":"teams.alpha.ch1","type":"rw"}]` to `unitdb/keygen`, and receives them on the same topic (see `examples/sample`, `-a keygen`).

`WithInsecure()` connects with the insecure flag, which skips topic keys. Since unitdb v0.6.0 a server refuses it, and `Connect` returns the refusal (return code 4), unless the server's config sets `"allow_insecure": true`, which is for development only and which a cluster node refuses to start with. Use it only for tests and debugging against such a standalone server.

A trusted backend, such as an API server acting for its users, needs no topic keys either: give it a service client ID, which only the server's `mintid` command issues, with the server's key (`go run ./server/cmd/mintid -config unitdb.conf -service` in the unitdb repository), and connect with it without `WithInsecure()`. A connection the backend opens for a user, with the user's client ID, skips topic keys once it publishes `{"client_id": "<the service's client ID>"}` to `unitdb/service`. Keep service IDs on servers, never on clients or devices. Topics whose first part starts with `$` are reserved for the server.

### Reconnecting
By default a client closes when its connection is lost, and calls the handler set with `WithConnectionLostHandler`. With `WithAutoReconnect()` it connects again by itself instead, trying each server in turn, for example the other nodes of a cluster:

```golang
client, err := udb.NewClient(
	"tcp://node-one:6060",
	clientID,
	udb.AddServer("tcp://node-two:6060"),
	udb.WithSessionKey(sessionKey),
	udb.WithAutoReconnect(),
	udb.WithConnectionLostHandler(func(_ udb.Client, err error) { log.Println("connection lost:", err) }),
	udb.WithConnectionHandler(func(udb.Client) { log.Println("connected") }),
)
```

It resumes its session, resends what the server had not acknowledged, and subscribes again to its topics. Calls made while it reconnects wait for the connection, up to the write timeout. A message in flight when the connection drops can be delivered twice. `WithMaxReconnectInterval` caps the pause between attempts (10 seconds by default).

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
This project is licensed under [MIT License](https://github.com/unit-io/unitdb-go/blob/master/LICENSE).
