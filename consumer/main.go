package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type User struct {
	Name           string `json:"name"`
	Lastname       string `json:"lastname"`
	PassportNumber string `json:"passport_number"`
}

type AnonymizedUser struct {
	Name           string `json:"name"`
	Lastname       string `json:"lastname"`
	PassportNumber string `json:"passport_number"`
}

func hashString(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	groupID := "anonymizer-group"
	rawTopic := "raw-users"
	outputTopic := "anonymized-users"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    rawTopic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   outputTopic,
	})

	log.Println("Consumer started")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var user User
		if err := json.Unmarshal(m.Value, &user); err != nil {
			log.Printf("Invalid message: %v", err)
			continue
		}
		anonymized := AnonymizedUser{
			Name:           hashString(user.Name),
			Lastname:       hashString(user.Lastname),
			PassportNumber: hashString(user.PassportNumber),
		}
		data, err := json.Marshal(anonymized)
		if err != nil {
			log.Printf("Marshal error: %v", err)
			continue
		}
		if err := writer.WriteMessages(context.Background(),
			kafka.Message{Value: data},
		); err != nil {
			log.Printf("Kafka write error: %v", err)
			continue
		}
		log.Printf("Anonymized user: %+v", anonymized)
	}
}