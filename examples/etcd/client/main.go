package main

import (
	"flag"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	etcdnaming "github.com/coreos/etcd/clientv3/naming"
	"github.com/teambition/grpclb"
	"github.com/teambition/grpclb/examples/helloworld"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/mgo.v2/bson"
)

var (
	etcd    = flag.String("etcd servers", "127.0.0.1:2379", "the ectd listener")
	service = flag.String("service", "helloworld", "the service name")
)

func main() {
	flag.Parse()
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*etcd, ",")})
	if err != nil {
		panic(err)
	}
	r := &etcdnaming.GRPCResolver{Client: etcdClient}
	b := grpclb.NewKetamaBalance(r)
	conn, err := grpc.Dial(*service, grpc.WithBalancer(b), grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.FailFast(false)))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := helloworld.NewGreeterClient(conn)
	count := map[string]int{}
	for i := 0; i <= 10000; i++ {
		timeoutCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		id := bson.NewObjectId().Hex()
		res, err := c.SayHello(grpclb.StrOrNumToContext(timeoutCtx, id), &helloworld.HelloRequest{Name: strconv.Itoa(i)})
		if err != nil {
			log.Println(err)
			continue
		}
		count[res.Message]++
	}
	for k, v := range count {
		log.Printf("server: %s, count: %d\n", k, v)
	}
}
