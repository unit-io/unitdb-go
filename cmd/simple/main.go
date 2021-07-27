package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	udb "github.com/unit-io/unitdb-go"
)

var f udb.MessageHandler = func(client udb.Client, pubMsg udb.PubMessage) {
	for _, msg := range pubMsg.Messages() {
		fmt.Printf("TOPIC: %s\n", msg.Topic)
		fmt.Printf("MSG: %s\n", msg.Payload)
	}
}

func main() {
	client, err := udb.NewClient(
		//"tcp://localhost:6060",
		// "ws://localhost:6080",
		"grpc://localhost:6080",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		udb.WithCleanSession(),
		udb.WithInsecure(),
		udb.WithBatchDuration(1*time.Second),
		udb.WithKeepAlive(2*time.Second),
		udb.WithPingTimeout(1*time.Second),
		udb.WithDefaultMessageHandler(f),
	)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	ctx := context.Background()
	err = client.ConnectContext(ctx)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	var r udb.Result

	r = client.Relay([]string{"ADcABeFRBDJKe/groups.private.673651407196578720.message"}, udb.WithLast("10m"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	r = client.Subscribe("ADcABeFRBDJKe/groups.private.673651407196578720.message", udb.WithSubDeliveryMode(0))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < 2; i++ {
		msg := fmt.Sprintf("Hi #%d time!", i)
		r := client.Publish("ADcABeFRBDJKe/groups.private.673651407196578720.message", []byte(msg), udb.WithTTL("1m"), udb.WithPubDeliveryMode(0))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	wait := time.NewTicker(5 * time.Second)
	<-wait.C
	r = client.Unsubscribe("ADcABeFRBDJKe/groups.private.673651407196578720.message")
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wait.Stop()
	client.Disconnect()
}
