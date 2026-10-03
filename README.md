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
A client publishes and subscribes with topic keys: it prefixes a topic with a key, as in `key/teams.alpha.ch1`. A client with a primary client ID requests keys from the server with `Keygen`, which publishes `[{"topic":"teams.alpha.ch1","type":"rw"}]` to `unitdb/keygen` and completes with the server's answer on the same topic:

```golang
r := client.Keygen(udb.KeyRequest{Topic: "teams.alpha.ch1", Type: "rw", TTL: 24 * time.Hour})
if _, err := r.Get(ctx, 10*time.Second); err != nil {
	log.Fatal(err) // a *udb.RequestError if the server refused
}
key := r.(*udb.KeygenResult).Keys()[0] // key.Key, and key.UUID to revoke it with
client.Subscribe(key.Key + "/teams.alpha.ch1")
```

Without a TTL a key lasts the server's `topic_key_ttl`, for ever by default. Client IDs and keys are opaque: the server's v2 client IDs are 94 characters, and its v2 keys 48, of base64url, which includes `-` and `_`. Since unitdb v0.7.0 the server issues and takes v2 IDs and keys only: it refuses a v1 client ID (52 characters) at connect with return code 2, and a v1 signed key or an unsigned one with an error notice of status 401 on `unitdb/error/`. A v0.6.0 server still takes them, and renews a v1 ID (see below), so move clients to v2 IDs and keys on v0.6.0 before upgrading.

`WithInsecure()` connects with the insecure flag, which skips topic keys. Since unitdb v0.6.0 a server refuses it, and `Connect` returns the refusal (return code 4), unless the server's config sets `"allow_insecure": true`, which is for development only and which a cluster node refuses to start with. Use it only for tests and debugging against such a standalone server.

A trusted backend, such as an API server acting for its users, needs no topic keys either: give it a service client ID, which only the server's `mintid` command issues, with the server's key (`go run ./server/cmd/mintid -config unitdb.conf -service` in the unitdb repository), and connect with it without `WithInsecure()`. A connection the backend opens for a user, with the user's client ID, skips topic keys once it vouches for it with `client.Vouch(serviceID)`, which publishes `{"client_id": "<the service's client ID>"}` to `unitdb/service`; vouch again after a reconnect. Keep service IDs on servers, never on clients or devices. Topics whose first part starts with `$` are reserved for the server.

### Client ID renewal
A server with v2 client IDs renews a client's ID when it connects with one sealed with a key being retired, or with one past 80% of its lifetime (a v0.6.0 server also renews a v1 ID, which v0.7.0 refuses instead): it sends the same ID sealed again, with a new expiry, on `unitdb/clientid/`. The client adopts it, and connects and reconnects with it from then on. Persist it, and create the client with it next time, as an expired ID is refused (`Connect` returns a `*udb.ConnectError` with return code 2, `udb.ConnRefusedIDRejected`):

```golang
udb.WithClientIDHandler(func(_ udb.Client, clientID string) { saveClientID(clientID) })
```

The client's local store, where it keeps what it has not finished sending and receiving, lives in a directory named after the client ID under the store path (`WithStorePath`). A renewed ID keeps it: the client records, in a small file named after the new ID, the directory of the store, so a client created with the renewed ID opens the same store and resumes the same session.

### Revocation
A primary client revokes a client ID or topic key of its contract by its uuid, as `Keygen` and `RequestClientID` give it, for ever or until a time, or every ID and key its contract was issued so far, its own ID included:

```golang
client.Revoke(key.UUID, time.Time{})                      // for ever
client.Revoke(idResult.UUID(), time.Now().Add(time.Hour)) // until then
client.RevokeAll()
```

Each completes with the server's answer: `Get` returns a `*udb.RequestError` with status 400, 403 (not a primary client) or, from a v0.6.0 server, 503 (a cluster with nodes that don't read v2 IDs and keys yet) if the server refused; a v0.7.0 server always takes them. A revoked client ID is refused at connect with return code 2, a revoked key with an error notice of status 401 on `unitdb/error/`; connections and subscriptions already open stay.

### Return codes
`Connect` fails with a `*udb.ConnectError` holding the server's return code: 0x01 unacceptable proto version, 0x02 identifier rejected (also an expired or revoked ID, and a v1 ID since v0.7.0), 0x03 identifier not allowed access, 0x04 not authorized (a refused key, or the insecure flag on a server without `allow_insecure`), 0x05 server error, 0x06 authentication failed, 0x07 forbidden, 0x08 session in use, 0x09 unknown epoch. The server's `utp` package names 0x04 `ErrRefusedServerUnavailable`; the client's `ConnRefusedNotAuthorized` names it as the server uses it.

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
