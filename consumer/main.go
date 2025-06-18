package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"os"
	"strconv"
	"time"
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
func createKafkaTopic(broker, topic string) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	return controllerConn.CreateTopics(topicConfig)
}

func waitForKafka(broker string, maxAttempts int) error {
	for i := 0; i < maxAttempts; i++ {
		conn, err := kafka.Dial("tcp", broker)
		if err == nil {
			conn.Close()
			return nil
		}
		log.Printf("Attempt %d: Kafka not ready, waiting...", i+1)
		time.Sleep(time.Second * time.Duration(i+1))
	}
	return fmt.Errorf("failed to connect to Kafka after %d attempts", maxAttempts)
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	groupID := "anonymizer-group"
	rawTopic := "raw-users"
	outputTopic := "anonymized-users"

	// Ждем готовности Kafka
	if err := waitForKafka(broker, 10); err != nil {
		log.Fatalf("Kafka connection error: %v", err)
	}

	// Создаем выходной топик если его нет
	if err := createKafkaTopic(broker, outputTopic); err != nil {
		log.Printf("Warning: could not create topic %s: %v", outputTopic, err)
	}

	// Инициализация reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       rawTopic,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset,
		MaxWait:     10 * time.Second,
	})
	defer reader.Close()

	// Инициализация writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        outputTopic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  5,
		BatchSize:    10,
		BatchTimeout: 50 * time.Millisecond,
	}
	defer writer.Close()

	log.Println("Consumer started successfully")

	for {
		// Чтение сообщения
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Десериализация
		var user User
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}

		// Анонимизация
		anonymized := AnonymizedUser{
			Name:           hashString(user.Name),
			Lastname:       hashString(user.Lastname),
			PassportNumber: hashString(user.PassportNumber),
		}

		// Сериализация
		data, err := json.Marshal(anonymized)
		if err != nil {
			log.Printf("Error encoding message: %v", err)
			continue
		}

		// Запись в Kafka
		err = writer.WriteMessages(context.Background(),
			kafka.Message{Value: data},
		)
		if err != nil {
			log.Printf("Error writing message: %v", err)
			continue
		}

		log.Printf("Processed message: %+v", anonymized)
	}
}
