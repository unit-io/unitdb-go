package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	udb "github.com/unit-io/unitdb-go"
)

var messageHandler = func(ctx context.Context, topicFilter *udb.TopicFilter) {
	for {
		select {
		case <-ctx.Done():
			return
		case messages := <-topicFilter.Updates():
			for _, msg := range messages {
				fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", msg.Topic, msg.Payload)
			}
		}
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

	privateGroupFilter, err := client.TopicFilter("groups.private...")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	go messageHandler(ctx, privateGroupFilter)

	r = client.Relay([]string{"groups.private.673651407196578720.message"}, udb.WithLast("10m"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	r = client.Subscribe("groups.private.673651407196578720.message", udb.WithSubDeliveryMode(0))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < 2; i++ {
		msg := fmt.Sprintf("Hi #%d time!", i)
		r := client.Publish("groups.private.673651407196578720.message", []byte(msg), udb.WithTTL("1m"), udb.WithPubDeliveryMode(0))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	wait := time.NewTicker(5 * time.Second)
	<-wait.C
	r = client.Unsubscribe("groups.private.673651407196578720.message")
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wait.Stop()
	client.DisconnectContext(ctx)
}
