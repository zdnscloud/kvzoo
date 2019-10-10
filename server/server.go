package server

import (
	"net"

	"github.com/zdnscloud/kvzoo"
	pb "github.com/zdnscloud/kvzoo/proto"
	"google.golang.org/grpc"
)

type KVGRPCServer struct {
	server   *grpc.Server
	listener net.Listener
}

func New(addr string, db kvzoo.DB) (*KVGRPCServer, error) {
	server := grpc.NewServer()

	service := newKVService(db)
	pb.RegisterKVSServer(server, service)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &KVGRPCServer{
		server:   server,
		listener: listener,
	}, nil
}

func (s *KVGRPCServer) Start() error {
	return s.server.Serve(s.listener)
}

func (s *KVGRPCServer) Stop() error {
	s.server.GracefulStop()
	return nil
}
