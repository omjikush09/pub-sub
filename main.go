package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
)

type Message struct {
	Topic   string
	Payload []byte
}

type Subscriber struct {
	id    uint64
	topic string
	c     chan Message
}

type subReq struct {
	topic string
	reply chan *Subscriber
}

type unsubReq struct {
	sub  *Subscriber
	done chan struct{}
}

var ErrorClose = errors.New("broker closed")

type Broker struct {
	subCh   chan subReq
	unsubCh chan unsubReq
	pubCh   chan Message
	quit    chan struct{}
	done    chan struct{}
	nextID  atomic.Uint64
	once    sync.Once
}

func NewBroker() *Broker {
	b := &Broker{
		subCh:   make(chan subReq),
		unsubCh: make(chan unsubReq),
		pubCh:   make(chan Message, 64),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *Broker) run() {
	defer close(b.done)
	subs := map[string]map[uint64]*Subscriber{}
	for {
		select {
		case r := <-b.subCh:
			s := &Subscriber{
				id:    b.nextID.Add(1),
				topic: r.topic,
				c:     make(chan Message, 16),
			}
			if subs[r.topic] == nil {
				subs[r.topic] = map[uint64]*Subscriber{}
			}
			subs[r.topic][s.id] = s
			r.reply <- s
		case r := <-b.pubCh:

			s := subs[r.Topic]
			for _, v := range s {
				select {
				case v.c <- r:
				default:

				}
			}

		case r := <-b.unsubCh:
			if subs[r.sub.topic] != nil {
				topicSubs := subs[r.sub.topic]
				delete(topicSubs, r.sub.id)
				if len(topicSubs) == 0 {
					delete(subs, r.sub.topic)
				}
			}
			close(r.sub.c)
			r.done <- struct{}{}
		}
	}

}

func (b *Broker) Subscribe(topic string) (*Subscriber, error) {
	//
	s := subReq{topic: topic, reply: make(chan *Subscriber, 1)}

	select {
	case b.subCh <- s:
		return <-s.reply, nil
	case <-b.done:
		return nil, fmt.Errorf("Error is done")
	}
}

func (b *Broker) Unsubscribe(subscriber *Subscriber) {
	if subscriber == nil {
		return
	}
	un := unsubReq{sub: subscriber, done: make(chan struct{})}

	select {
	case b.unsubCh <- un:
		<-un.done
	case <-b.done:
	}

}

func (b *Broker) Publish(topic string, payload []byte) {
	message := Message{Topic: topic, Payload: payload}
	select {
	case b.pubCh <- message:
	case <-b.done:
	}

}

func main() {
	broker := NewBroker()

	http.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Methond not allowed", http.StatusBadRequest)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "Topic not found", http.StatusBadRequest)
			return
		}

		payload, _ := io.ReadAll(r.Body)
		broker.Publish(topic, payload)
		fmt.Fprintln(w, "ok")
	})

	http.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusBadRequest)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "Topic not found", http.StatusBadRequest)
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Internal Server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		s, err := broker.Subscribe(topic)
		if err != nil {
			return
		}

		log.Printf("Subscriber connected %q", topic)

		for {
			select {
			case msg, ok := <-s.c:
				if !ok {
					return
				}
				fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
				flusher.Flush()
			case <-r.Context().Done():
				log.Printf("User disconnected %q", topic)
				broker.Unsubscribe(s)
				return
			}
		}

	})

	PORT := os.Getenv("PORT")
	if PORT == "" {
		PORT = "8000"
	}

	log.Println("listen on :" + PORT)
	log.Fatal(http.ListenAndServe(":8000", nil))
}
