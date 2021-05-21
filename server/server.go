package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "github.com/MBaczun/producer-consumer/prodcon"
)

const (
	port = 3030
)

type consumerServer struct {
	//embeding??
	pb.UnimplementedConsumerServer
}

func (s *consumerServer) ConsumeSingleString(ctx context.Context, str *pb.String) (*pb.Ack, error) {
	fmt.Printf("Consumed %v", str.Value)
	return &pb.Ack{Value: true}, nil
}

func (s *consumerServer) ConsumeStream(stream pb.Consumer_ConsumeStreamServer) error {
	for {
		str, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("Consumed %v", str.Value)
	}
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	s := consumerServer{}
	pb.RegisterConsumerServer(grpcServer, &s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}
