package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"code.teambition.com/soa/go-lib/grpc/grpclb"
	"code.teambition.com/soa/go-lib/grpc/grpclb/examples/helloworld"
	"github.com/coreos/etcd/clientv3"
	etcdnaming "github.com/coreos/etcd/clientv3/naming"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	addr    = flag.String("addr", "", "server address")
	weight  = flag.Int("weight", int(grpclb.Level1), "the weight of this server")
	etcd    = flag.String("etcd", "127.0.0.1:2379", "the ectd listener")
	service = flag.String("service", "helloworld", "the service name")
)

func etcdAdd(c *clientv3.Client, service, addr string, weight int) error {
	r := &etcdnaming.GRPCResolver{Client: c}
	return r.Update(c.Ctx(), service, grpclb.AddServer(addr, grpclb.WeightLvl(weight)))
}

func etcdDelete(c *clientv3.Client, service, addr string) error {
	r := &etcdnaming.GRPCResolver{Client: c}
	return r.Update(c.Ctx(), service, grpclb.DeleteServer(addr))
}

func etcdLeaseAdd(c *clientv3.Client, lid clientv3.LeaseID, service, addr string, weight int) error {
	r := &etcdnaming.GRPCResolver{Client: c}
	return r.Update(c.Ctx(), service, grpclb.AddServer(addr, grpclb.WeightLvl(weight)), clientv3.WithLease(lid))
}

func main() {
	flag.Parse()
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*etcd, ",")})
	if err != nil {
		log.Panic(err)
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Panic(err)
	}
	s := grpc.NewServer()
	helloworld.RegisterGreeterServer(s, hw{})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	etcdAdd(etcdClient, *service, *addr, *weight)

	go func() {
		log.Printf("Server start at %s", *addr)
		if err := s.Serve(lis); err != nil {
			log.Println(err)
		}
	}()

	<-signals
	etcdDelete(etcdClient, *service, *addr)
}

type hw struct{}

func (h hw) SayHello(context.Context, *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{Message: *addr}, nil
}
