package mq

import (
	"encoding/json"
	"github.com/pebbe/zmq4"
	"github.com/tylertreat/mq-benchmarking/benchmark"
	"strings"
	"time"
)

type LightQ struct {
	handler      benchmark.MessageHandler
	sender       *zmq4.Socket
	receiver     *zmq4.Socket
	send_counter int64
}

func lightqReceive(lightq *LightQ) {
	for {
		// TODO: Some messages come back empty. Is this a slow-consumer problem?
		// Should DONTWAIT be used?
		message, _ := lightq.receiver.RecvBytes(zmq4.DONTWAIT)
		if lightq.handler.ReceiveMessage(message) {
			break
		}
	}
}

type JoinResp struct {
	bind_uri string `json:"bind_uri"`
	cmd      string `json:"cmd"`
	status   string `json:"status"`
	topic    string `json:"topic"`
}

var (
	stats_cmd string = `{"cmd" :"stats", "topic": "testfile",  "user_id": "test_admin", "password": "T0p$3cr31"}`
)

func createTopic(admin *zmq4.Socket) (producer_bind_uri string, consumer_bind_uri string) {

	str := `{"cmd": "create_topic", "admin_password": "T0p$3cr31", "admin_user_id": "lightq_admin", "broker_type": "file", "topic": "testfile", "user_id": "test_admin", "password": "T0p$3cr31"}`
	//log.Print("Sending create topic command %s", str)
	admin.SendMessage(str)
	reply, err := admin.RecvMessage(0)
	if err != nil {
		panic(err)
	}
	//log.Print(reply)

	//get response producer
	str = `{"cmd" :"join", "topic": "testfile", "type": "pub", "user_id": "test_admin", "password": "T0p$3cr31", "connection_type": "zmq"}`
	//log.Print("Sending join pub command %s", str)
	admin.SendMessage(str)
	reply, err = admin.RecvMessage(0)
	if err != nil {
		panic(err)
	}
	//log.Print(reply[0])
	res := &JoinResp{}
	json.Unmarshal([]byte(reply[0]), &res)

	producer_bind_uri = res.bind_uri
	//log.Print("producer_bind_uri %s", producer_bind_uri)

	producer_bind_uri = strings.Replace(producer_bind_uri, "*", "127.0.0.1", -1)

	str = `{"cmd" :"join", "topic": "testfile", "type": "pull", "user_id": "test_admin", "password": "T0p$3cr31", "connection_type": "zmq"}`
	//log.Print("Sending join pull  command %s", str)
	admin.SendMessage(str)
	reply, err = admin.RecvMessage(0)
	if err != nil {
		panic(err)
	}
	//log.Print(reply[0])
	res = &JoinResp{}
	json.Unmarshal([]byte(reply[0]), &res)

	consumer_bind_uri = res.bind_uri
	//	log.Print("consumer_bind_uri: %s", consumer_bind_uri)
	consumer_bind_uri = strings.Replace(consumer_bind_uri, "*", "127.0.0.1", -1)
	admin.Close()

	//return producer_bind_uri, consumer_bind_uri
	return "tcp://127.0.0.1:5001", "tcp://127.0.0.1:5003"

}

func NewLightQ(numberOfMessages int, testLatency bool) *LightQ {
	ctx, _ := zmq4.NewContext()
	admin, err := ctx.NewSocket(zmq4.REQ)
	if err != nil {
		panic(err)
	}
	admin.Connect("tcp://127.0.0.1:5000")
	producer_bind_uri, consumer_bind_uri := createTopic(admin)

	//log.Print("producer_bind_uri %s", producer_bind_uri)
	//log.Print("consumer_bind_uri: %s", consumer_bind_uri)
	pub, _ := ctx.NewSocket(zmq4.PUSH)
	pub.Connect(producer_bind_uri)
	sub, _ := ctx.NewSocket(zmq4.PULL)
	sub.Connect(consumer_bind_uri)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &LightQ{
		handler:  handler,
		sender:   pub,
		receiver: sub,
	}
}

func (lightq *LightQ) Setup() {
	// Sleep is needed to avoid race condition with receiving initial messages.
	time.Sleep(3 * time.Second)
	go lightqReceive(lightq)
}

func (lightq *LightQ) Teardown() {
	lightq.sender.Close()
	lightq.receiver.Close()
}

func (lightq *LightQ) Send(message []byte) {
	// TODO: Should DONTWAIT be used? Possibly overloading consumer.
	lightq.sender.SendBytes(message, zmq4.DONTWAIT)
	lightq.send_counter++
	if lightq.send_counter%100000 == 0 {
		time.Sleep(10 * time.Millisecond)
	}
}

func (lightq *LightQ) MessageHandler() *benchmark.MessageHandler {
	return &lightq.handler
}
