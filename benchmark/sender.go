package benchmark

import (
	"encoding/binary"
	"log"
	"time"
    "math/rand"
)

type MessageSender interface {
	Send([]byte)
}

type SendEndpoint struct {
	MessageSender MessageSender
}




func randSeq(n int, letters []rune) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func (endpoint SendEndpoint) TestThroughput(messageSize int, numberToSend int) {
	//message := make([]byte, messageSize)
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	message := []byte(randSeq(messageSize, letters))
	
	start := time.Now().UnixNano()
	for i := 0; i < numberToSend; i++ {
		endpoint.MessageSender.Send(message)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}

func (endpoint SendEndpoint) TestLatency(messageSize int, numberToSend int) {
	start := time.Now().UnixNano()
	b := make([]byte, 9)
	for i := 0; i < numberToSend; i++ {
		binary.PutVarint(b, time.Now().UnixNano())
		endpoint.MessageSender.Send(b)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}
