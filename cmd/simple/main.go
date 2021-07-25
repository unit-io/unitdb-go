package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	unitdb "github.com/unit-io/unitdb-go"
)

var f unitdb.MessageHandler = func(client unitdb.Client, pubMsg unitdb.PubMessage) {
	for _, msg := range pubMsg.Messages() {
		fmt.Printf("TOPIC: %s\n", msg.Topic)
		fmt.Printf("MSG: %s\n", msg.Payload)
	}
}

func main() {
	client, err := unitdb.NewClient(
		//"tcp://localhost:6060",
		// "ws://localhost:6080",
		"grpc://localhost:6080",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		unitdb.WithCleanSession(),
		unitdb.WithInsecure(),
		unitdb.WithBatchDuration(1*time.Second),
		unitdb.WithKeepAlive(2*time.Second),
		unitdb.WithPingTimeout(1*time.Second),
		unitdb.WithDefaultMessageHandler(f),
	)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	ctx := context.Background()
	err = client.ConnectContext(ctx)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	var r unitdb.Result

	r = client.Relay("ADcABeFRBDJKe/groups.private.673651407196578720.message", unitdb.WithLast("10m"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	r = client.Subscribe("ADcABeFRBDJKe/groups.private.673651407196578720.message", unitdb.WithSubDeliveryMode(0))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < 2; i++ {
		msg := fmt.Sprintf("Hi #%d time!", i)
		r := client.Publish("ADcABeFRBDJKe/groups.private.673651407196578720.message", []byte(msg), unitdb.WithTTL("1m"), unitdb.WithPubDeliveryMode(0))
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
