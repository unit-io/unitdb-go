# unitd [![GoDoc](https://godoc.org/github.com/unit-io/unitd-go?status.svg)](https://godoc.org/github.com/unit-io/unitd-go)

## Unitd is an open source messaging broker for IoT and other real-time messaging service. Unitd messaging API is built for speed and security.

Unitd is a real-time messaging service for IoT connected devices, it is based on MQTT protocol. Unitd is blazing fast and secure messaging infrastructure and APIs for IoT, gaming, apps and real-time web.

Unitd can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. Unitd is perfect for the internet of things and internet connected devices.

## Quick Start
To build [unitd](https://github.com/unit-io/unitd) from source code use go get command and copy unitd.conf to the path unitd binary is placed.

> go get -u github.com/unit-io/unitd && unitd

## Unitd Client
[unitd-go](https://github.com/unit-io/unitd-go) is GRPC client to pubsub messages over protobuf using GRPC.

### Usage
Detailed API documentation is available using the [godoc.org](https://godoc.org/github.com/unit-io/unitd-go) service.

Make use of the client by importing it in your Go client source code. For example,

import "github.com/unit-io/unitd-go"

Samples are available in the cmd directory for reference.

Note:

The library supports using pubsub over protobug by using the grpc:// prefix in the URI.

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [MIT](https://github.com/unit-io/unitd-go/blob/master/LICENSE).
