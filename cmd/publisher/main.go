package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const queueName = "requests"

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/appdb?sslmode=disable"
	}

	// Connect to RabbitMQ
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://app:app@localhost:5672/"
	}

	var mqConn *amqp.Connection
	var err error
	for i := 0; i < 30; i++ {
		mqConn, err = amqp.Dial(rabbitURL)
		if err == nil {
			break
		}
		log.Printf("waiting for rabbitmq... (%v)", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatalf("could not connect to rabbitmq: %v", err)
	}
	defer mqConn.Close()

	mqChan, err := mqConn.Channel()
	if err != nil {
		log.Fatalf("could not open channel: %v", err)
	}
	defer mqChan.Close()

	_, err = mqChan.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("could not declare queue: %v", err)
	}

	// Listen to PostgreSQL NOTIFY
	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("listener error: %v", err)
		}
	})

	err = listener.Listen("hr_employee")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	defer listener.Close()

	log.Println("publisher: listening on pg channel 'hr_employee', publishing to rabbitmq queue 'requests'")

	for {
		select {
		case notification := <-listener.Notify:
			if notification == nil {
				continue
			}

			log.Printf("publisher: received pg notify: %s", notification.Extra)

			// Validate JSON
			var payload json.RawMessage
			if json.Valid([]byte(notification.Extra)) {
				payload = json.RawMessage(notification.Extra)
			} else {
				escaped, _ := json.Marshal(notification.Extra)
				payload = escaped
			}

			err := mqChan.Publish("", queueName, false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        payload,
			})
			if err != nil {
				log.Printf("publisher: publish error: %v", err)
			} else {
				log.Printf("publisher: published to queue '%s'", queueName)
			}

		case <-time.After(90 * time.Second):
			go listener.Ping()
		}
	}
}
