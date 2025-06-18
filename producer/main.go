package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/segmentio/kafka-go"
)

type User struct {
	Name           string `json:"name"`
	Lastname       string `json:"lastname"`
	PassportNumber string `json:"passport_number"`
}

func main() {
	kafkaURL := os.Getenv("KAFKA_BROKER")
	if kafkaURL == "" {
		kafkaURL = "kafka:9092"
	}
	topic := "raw-users"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   topic,
	})

	http.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var user User
		if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		data, err := json.Marshal(user)
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}

		if err := writer.WriteMessages(context.Background(),
			kafka.Message{Value: data},
		); err != nil {
			log.Printf("Error writing to kafka: %v", err)
			http.Error(w, "kafka error", http.StatusInternalServerError)
			return
		}
		log.Printf("Produced user: %+v", user)
		w.Write([]byte("ok"))
	})

	log.Println("Producer listening on :8080")
	http.ListenAndServe(":8080", nil)
}
