package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	unitd "github.com/unit-io/unitd-go"
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

func main() {
	topic := flag.String("topic", "AbYANcAPRKPNa/teams.alpha.ch1", "The topic name to/from which to publish/subscribe")
	server := flag.String("server", "grpc://localhost:6061", "The server URI. ex: grpc://127.0.0.1:6061")
	password := flag.String("password", "", "The password (optional)")
	user := flag.String("user", "", "The User (optional)")
	id := flag.String("id", "UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ", "The ClientID (optional)")
	num := flag.Int("num", 1, "The number of messages to publish or subscribe (default 1)")
	payload := flag.String("message", "Hello team alpha channel1!", "The message text to publish (default empty)")
	action := flag.String("action", "sub", "Action publish or subscribe (required)")
	flag.Parse()

	if *action != "pub" && *action != "sub" && *action != "unsub" && *action != "keygen" {
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

		client, err := unitd.NewClient(
			*server,
			*id,
			// unitd.WithInsecure(),
			unitd.WithUserNamePassword(*user, *password),
			unitd.WithCleanSession(),
			unitd.WithConnectionLostHandler(func(client unitd.ClientConn, err error) {
				if err != nil {
					log.Fatal(err)
				}
				close(recv)
			}),
			unitd.WithDefaultMessageHandler(func(client unitd.ClientConn, msg unitd.Message) {
				recv <- [2][]byte{msg.Topic(), msg.Payload()}
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
		req := struct {
			Topic string `json:"topic"`
			Type  string `json:"type"`
		}{
			*topic,
			"rw",
		}
		keyReq, err := json.Marshal(req)
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		r := client.Publish([]byte("unitd/keygen"), keyReq)
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

	if *action == "pub" {
		client, err := unitd.NewClient(
			*server,
			*id,
			// unitd.WithInsecure(),
			unitd.WithUserNamePassword(*user, *password),
			unitd.WithCleanSession(),
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
			r := client.Publish([]byte(*topic), []byte(*payload), unitd.WithPubQos(2))
			if _, err := r.Get(ctx, 1*time.Second); err != nil {
				log.Fatalf("err: %s", err)
			}
		}

		time.Sleep(1 * time.Second)
		client.DisconnectContext(ctx)
		fmt.Println("Publisher Disconnected")
	} else {
		recv := make(chan [2][]byte)

		client, err := unitd.NewClient(
			*server,
			*id,
			// unitd.WithInsecure(),
			unitd.WithUserNamePassword(*user, *password),
			unitd.WithCleanSession(),
			unitd.WithConnectionLostHandler(func(client unitd.ClientConn, err error) {
				if err != nil {
					log.Fatal(err)
				}
				close(recv)
			}),
			unitd.WithDefaultMessageHandler(func(client unitd.ClientConn, msg unitd.Message) {
				recv <- [2][]byte{msg.Topic(), msg.Payload()}
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
		r := client.Subscribe([]byte(*topic), unitd.WithSubQos(2))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		for {
			select {
			case <-ctx.Done():
				client.DisconnectContext(ctx)
				fmt.Println("Subscriber Disconnected")
				return
			case incoming := <-recv:
				fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
			}
		}
	}
}
