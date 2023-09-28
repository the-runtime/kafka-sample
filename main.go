package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

var (
	producer     sarama.SyncProducer
	consumer     sarama.Consumer
	topicCounter int
)

func init() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	var err error
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	//Initialize Kafka consumer
	consumer, err = sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalf("failed to create Kafka consumer: %v", err)
	}

}

func main() {
	http.HandleFunc("/create-topic", createKafkaTopic)
	http.HandleFunc("/push-message", pushkafkaMessage)
	http.HandleFunc("/read-message", readKafkaMessage)

	log.Println("Starting web server on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

}

func createKafkaTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		http.Error(w, "Topic name is required", http.StatusBadRequest)
		return
	}

	prodMessage := []*sarama.ProducerMessage{{
		Topic: topicName,
		Value: sarama.StringEncoder(fmt.Sprintf("Topic %s created at %s", topicName, time.Now().UTC())),
	}}
	err := producer.SendMessages(prodMessage)

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create topic %s: %v", topicName, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Topic %s created successfully", topicName)

}

func pushkafkaMessage(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		http.Error(w, "Topic name is required", http.StatusBadRequest)
		return
	}

	//simulate message creation with random delay
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	message := fmt.Sprintf("(%s, %d)", time.Now().UTC().Format(time.RFC3339), topicCounter)
	topicCounter++

	err := producer.SendMessages([]*sarama.ProducerMessage{{
		Topic: topicName,
		Value: sarama.StringEncoder(message),
	}})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to push message to topic %s, %v", topicName, err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Message pushed to topic %s: %s", topicName, message)
}

func readKafkaMessage(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		println("error parsing topic or topic name not given")
		http.Error(w, "Topic name required", http.StatusBadRequest)

	}

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		println("Error getting partition =consumer")
		return
	}

	lastOffset := partitionConsumer.HighWaterMarkOffset()

	//defer partitionConsumer.AsyncClose()

	println("Go routine starts")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-partitionConsumer.Messages():
				if !ok {
					fmt.Println("Channel Closed. Exiting")
					return
				}
				fmt.Fprintf(w, string(msg.Value))
				fmt.Println("Message Received", string(msg.Value))
				if msg.Offset == lastOffset-1 {
					partitionConsumer.AsyncClose()
				}
			}
		}
	}()
	wg.Wait()
	println("Go routine ends here")
}
