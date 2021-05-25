package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "github.com/MBaczun/producer-consumer/prodcon"
)

type consumerServer struct {
	//embeding??
	pb.UnimplementedConsumerServer
}

func (s *consumerServer) ConsumeSingleString(ctx context.Context, str *pb.String) (*pb.Ack, error) {
	fmt.Printf("Consumed %v\n", str.Value)
	return &pb.Ack{Value: true}, nil
}

func (s *consumerServer) ConsumeStream(stream pb.Consumer_ConsumeStreamServer) error {
	for {
		str, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Ack{Value: true})
		}
		if err != nil {
			return err
		}
		fmt.Printf("Consumed %v\n", str.Value)
	}
}

func main() {
	port := flag.Int("p", 3030, "Port")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	s := consumerServer{}
	pb.RegisterConsumerServer(grpcServer, &s)

	fmt.Println("Server Started")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}
