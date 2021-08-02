package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	udb "github.com/unit-io/unitdb-go"
)

/*
Options:
 [-help]                      Display help
 [-a pub|sub|unsub|keygen]    Action pub (publish) or sub (subscribe) or unsub (unsubscribe) or keygen (key generation)
 [-m <message>]               Payload to send
 [-n <number>]                Number of messages to send or receive
 [-clean]                     CleanSession (true if -clean is present)
 [-id <clientid>]             CliendID
 [-user <user>]               User
 [-password <password>]       Password
 [-server <uri>]              Server URI
 [-topic <topic>]             Topic
*/

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
	topic := flag.String("topic", "groups.private.673651407196578720.message", "The topic name to/from which to publish/subscribe")
	server := flag.String("server", "grpc://localhost:6080", "The server URI. ex: grpc://127.0.0.1:6080")
	password := flag.String("password", "", "The password (optional)")
	user := flag.String("user", "", "The User (optional)")
	id := flag.String("id", "UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ", "The ClientID (optional)")
	num := flag.Int("number", 1, "The number of messages to publish or subscribe (default 1)")
	payload := flag.String("message", "Hello team alpha channel1!", "The message text to publish (default empty)")
	action := flag.String("action", "relay", "Action publish, relay or subscribe (required)")
	flag.Parse()

	if *action != "pub" && *action != "relay" && *action != "sub" && *action != "unsub" && *action != "keygen" {
		fmt.Println("Invalid setting for -action, must be pub or sub")
		return
	}

	if *topic == "" {
		fmt.Println("Invalid setting for -topic, must not be empty")
		return
	}

	fmt.Printf("Client Info:\n")
	fmt.Printf("\taction:    %s\n", *action)
	fmt.Printf("\tserver:    %s\n", *server)
	fmt.Printf("\tclientid:  %s\n", *id)
	fmt.Printf("\tuser:      %s\n", *user)
	fmt.Printf("\tpassword:  %s\n", *password)
	fmt.Printf("\ttopic:     %s\n", *topic)
	fmt.Printf("\tmessage:   %s\n", *payload)
	fmt.Printf("\tnum:       %d\n", *num)

	if *action == "keygen" {
		recv := make(chan [2][]byte)

		client, err := udb.NewClient(
			*server,
			*id,
			// udb.WithInsecure(),
			udb.WithUserNamePassword(*user, []byte(*password)),
			udb.WithCleanSession(),
			udb.WithConnectionLostHandler(func(client udb.Client, err error) {
				if err != nil {
					log.Fatal(err)
				}
				close(recv)
			}),
		)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		ctx := context.Background()
		err = client.ConnectContext(ctx)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		fmt.Println("Keygen Started")
		req := []struct {
			Topic string `json:"topic"`
			Type  string `json:"type"`
		}{{
			*topic,
			"rw",
		},
		}
		keyReq, err := json.Marshal(req)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		r := client.Publish("unitdb/keygen", keyReq)
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
		for {
			select {
			case <-ctx.Done():
				client.DisconnectContext(ctx)
				fmt.Println("Subscriber Disconnected")
				return
			case incoming := <-recv:
				fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
				return
			}
		}
	}

	if *action == "sub" {
		recv := make(chan [2][]byte)

		client, err := udb.NewClient(
			*server,
			*id,
			udb.WithInsecure(),
			// udb.WithSessionKey(2339641922),
			udb.WithUserNamePassword(*user, []byte(*password)),
			// udb.WithCleanSession(),
			udb.WithKeepAlive(2*time.Second),
			udb.WithPingTimeout(1*time.Second),
			udb.WithConnectionLostHandler(func(client udb.Client, err error) {
				if err != nil {
					log.Fatal(err)
				}
				close(recv)
			}),
			udb.WithBatchDuration(10*time.Second),
		)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		ctx := context.Background()
		err = client.ConnectContext(ctx)
		if err != nil {
			log.Fatalf("err: %s", err)
		}

		topicFilter, err := client.TopicFilter(*topic)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		go messageHandler(ctx, topicFilter)

		r := client.Subscribe(*topic, udb.WithSubDeliveryMode(1) /*, udb.WithSubDelay(1*time.Second)*/)
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		wait := time.NewTicker(30 * time.Second)
		<-wait.C
		r = client.Unsubscribe(*topic)
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		wait.Stop()
		client.DisconnectContext(ctx)

	}

	if *action == "pub" {

		client, err := udb.NewClient(
			*server,
			*id,
			udb.WithInsecure(),
			// udb.WithSessionKey(2339641921),
			udb.WithUserNamePassword(*user, []byte(*password)),
			udb.WithConnectionLostHandler(func(client udb.Client, err error) {
				if err != nil {
					log.Fatal(err)
				}
			}),
			// udb.WithCleanSession(),
		)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		ctx := context.Background()
		err = client.ConnectContext(ctx)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		fmt.Println("Publisher Started")
		for i := 0; i < *num; i++ {
			r := client.Publish(*topic, []byte(*payload), udb.WithPubDeliveryMode(1))
			if _, err := r.Get(ctx, 1*time.Second); err != nil {
				log.Fatalf("err: %s", err)
			}
		}

		time.Sleep(1 * time.Second)
		client.DisconnectContext(ctx)
		fmt.Println("Publisher Disconnected")

	} else {
		recv := make(chan [2][]byte)

		client, err := udb.NewClient(
			*server,
			*id,
			udb.WithInsecure(),
			// udb.WithSessionKey(2339641922),
			udb.WithUserNamePassword(*user, []byte(*password)),
			// udb.WithCleanSession(),
			udb.WithKeepAlive(2*time.Second),
			udb.WithPingTimeout(1*time.Second),
			udb.WithConnectionLostHandler(func(client udb.Client, err error) {
				if err != nil {
					log.Fatal(err)
				}
				close(recv)
			}),
			udb.WithBatchDuration(10*time.Second),
		)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		ctx := context.Background()
		err = client.ConnectContext(ctx)
		if err != nil {
			log.Fatalf("err: %s", err)
		}

		topicFilter, err := client.TopicFilter(*topic)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		go messageHandler(ctx, topicFilter)

		r := client.Relay([]string{*topic}, udb.WithLast("1m"))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		wait := time.NewTicker(30 * time.Second)
		<-wait.C
		wait.Stop()
		client.DisconnectContext(ctx)
	}
}
