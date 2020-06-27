package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	unitd "github.com/unit-io/unitd-go"
)

var f unitd.MessageHandler = func(client unitd.ClientConn, msg unitd.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func main() {
	client, err := unitd.NewClient(
		"grpc://localhost:6061",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		unitd.WithInsecure(),
		unitd.WithKeepAlive(2*time.Second),
		unitd.WithPingTimeout(1*time.Second),
		unitd.WithDefaultMessageHandler(f),
	)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	ctx := context.Background()
	err = client.ConnectContext(ctx)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	r := client.Subscribe([]byte("teams.alpha.user1"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("Hi #%d time!", i)
		r := client.Publish([]byte("teams.alpha.user1"), []byte(msg))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	wait := time.NewTicker(1 * time.Second)
	<-wait.C
	r = client.Unsubscribe([]byte("teams.alpha.user1"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wait.Stop()
	client.Disconnect()
}
