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
		log.Fatalf("Failed to create Kafka producer %v", err)
	}

	//Initialize kafka consumer
	consumer, err = sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalf("failed to create kafka consumer %v", err)
	}

}

func main() {
	http.HandleFunc("/create-topic", createKafkaTopic)
	http.HandleFunc("/messages", kafkaMessage)

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

	err := producer.SendMessages([]*sarama.ProducerMessage{{
		Topic: topicName,
		Value: sarama.StringEncoder(fmt.Sprintf("Topic %s created at %s", topicName, time.Now().UTC())),
	}})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create topic %s", topicName), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Topic %s created successfully", topicName)

}
func kafkaMessage(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		println("error parsing topic or topic name not given")
		http.Error(w, "Topic nmae required", http.StatusBadRequest)
	}
	var wgM sync.WaitGroup
	wgM.Add(1)
	go func() {
		defer wgM.Done()
		partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
		if err != nil {
			println("Error problem partion from consumer: %v", err)
			return
		}

		lastOffset := partitionConsumer.HighWaterMarkOffset()
		var wgm sync.WaitGroup
		wgm.Add(1)
		go func() {
			defer wgm.Done()
			for {
				select {
				case msg, ok := <-partitionConsumer.Messages():
					if !ok {
						fmt.Println("Channel Closed. Exiting")
						return
					}
					fmt.Fprintf(w, "%s\n", string(msg.Value))
					if msg.Offset == lastOffset-1 {
						partitionConsumer.AsyncClose()
					}
				}
			}
		}()
		wgm.Wait()
		println("Message reading completed")
	}()
	wgM.Add(1)
	go func() {
		defer wgM.Done()

		//simulate message processing with random delay
		time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

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
		//w.WriteHeader(http.StatusCreated)

	}()

	wgM.Wait()

}
