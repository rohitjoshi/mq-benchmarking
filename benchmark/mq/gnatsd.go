package mq

import (
	"fmt"
	"time"

	"github.com/apcera/nats"
	"github.com/rohitjoshi/mq-benchmarking/benchmark"
)

type Gnatsd struct {
	handler     	benchmark.MessageHandler
	connPub        *nats.Conn
	connSub        *nats.Conn
	subject        string
	testLatency    bool
}

func NewGnatsd(numberOfMessages int, testLatency bool) *Gnatsd {
	connPub, _ := nats.Connect(nats.DefaultURL)
	connSub, _ := nats.Connect(nats.DefaultURL)

	// We want to be alerted if we get disconnected, this will
	// be due to Slow Consumer.
	connPub.Opts.AllowReconnect = false
	connSub.Opts.AllowReconnect = false

	// Report async errors.
	connPub.Opts.AsyncErrorCB = func(nc *nats.Conn, sub *nats.Subscription, err error) {
		panic(fmt.Sprintf("NATS: Received an async error! %v\n", err))
	}
	// Report async errors.
	connSub.Opts.AsyncErrorCB = func(nc *nats.Conn, sub *nats.Subscription, err error) {
		panic(fmt.Sprintf("NATS: Received an async error! %v\n", err))
	}

	// Report a disconnect scenario.
	connPub.Opts.DisconnectedCB = func(nc *nats.Conn) {
		fmt.Printf("Getting behind! %d\n", nc.OutMsgs-nc.InMsgs)
		panic("NATS: Got disconnected!")
	}

	// Report a disconnect scenario.
	connSub.Opts.DisconnectedCB = func(nc *nats.Conn) {
		fmt.Printf("Getting behind! %d\n", nc.OutMsgs-nc.InMsgs)
		panic("NATS: Got disconnected!")
	}

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Gnatsd{
		handler:     handler,
		subject:     "test",
		connPub:        connPub,
		connSub:        connSub,
		testLatency: testLatency,
	}
}

func (g *Gnatsd) Setup() {
	g.connSub.Subscribe(g.subject, func(message *nats.Msg) {
		g.handler.ReceiveMessage(message.Data)
	})
}

func (g *Gnatsd) Teardown() {
	g.connPub.Close()
	g.connSub.Close()
}

const (
	// Maximum bytes we will get behind before we start slowing down publishing.
	maxBytesBehind = 1024 * 1024 // 1MB

	// Maximum msgs we will get behind before we start slowing down publishing.
	maxMsgsBehind = 65536 // 64k

	// Maximum msgs we will get behind when testing latency
	maxLatencyMsgsBehind = 10 // 10

	// Time to delay publishing when we are behind.
	delay = 1 * time.Millisecond
)

func (g *Gnatsd) Send(message []byte) {
	// Check if we are behind by >= 1MB bytes
	bytesDeltaOver := g.connPub.OutBytes-g.connSub.InBytes >= maxBytesBehind
	// Check if we are behind by >= 65k msgs
	msgsDeltaOver := g.connPub.OutMsgs-g.connSub.InMsgs >= maxMsgsBehind
	// Override for latency test.
	if g.testLatency {
		msgsDeltaOver = g.connPub.OutMsgs-g.connSub.InMsgs >= maxLatencyMsgsBehind
	}

	// If we are behind on either condition, sleep a bit to catch up receiver.
	if bytesDeltaOver || msgsDeltaOver {
	//	time.Sleep(delay)
	}

	if(g.connPub.OutMsgs % 100000 == 0) {
		time.Sleep(10 * time.Millisecond)
		//log.Print("waiting..")
	}

	g.connPub.Publish(g.subject, message)
}

func (g *Gnatsd) MessageHandler() *benchmark.MessageHandler {
	return &g.handler
}
