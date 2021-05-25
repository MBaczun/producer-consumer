package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"google.golang.org/grpc"

	pb "github.com/MBaczun/producer-consumer/prodcon"
)

func produceSingleString(client pb.ConsumerClient, s string) {
	ack, err := client.ConsumeSingleString(context.Background(), &pb.String{Value: s})
	if err != nil {
		log.Fatalf("client error in string consumption: %s", err)
	}
	fmt.Printf("(single) Ack: %v\n", ack.Value)
}

func produceStreamStrings(client pb.ConsumerClient, strings []string) {
	//make stream
	stream, err := client.ConsumeStream(context.Background())
	if err != nil {
		log.Fatalf("%v.RecordRoute(_) = _, %v", client, err)
	}

	//stream strings
	for _, s := range strings {
		if err := stream.Send(&pb.String{Value: s}); err != nil {
			log.Fatalf("%v.Send(%v) = %v", stream, s, err)
		}
	}

	//end transaction
	ack, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	fmt.Printf("(stream) Ack: %v\n", ack.Value)
}

func main() {
	address := flag.String("addr", "localhost", "Server IP address")
	port := flag.Int("p", 3030, "Server Port")
	flag.Parse()

	fmt.Printf("CLient using address: %v\n", *address)

	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", *address, *port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %s", err)
	}
	defer conn.Close()

	client := pb.NewConsumerClient(conn)

	produceSingleString(client, "hello")

	strings := []string{"Hello", "World", "one", "two", "three"}
	produceStreamStrings(client, strings)

}
