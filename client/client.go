package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"

	pb "github.com/MBaczun/producer-consumer/prodcon"
)

const (
	port = 3030
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

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %s", err)
	}
	defer conn.Close()

	client := pb.NewConsumerClient(conn)

	produceSingleString(client, "hello")

	strings := []string{"Hello", "World", "one", "two", "three"}
	produceStreamStrings(client, strings)

}
