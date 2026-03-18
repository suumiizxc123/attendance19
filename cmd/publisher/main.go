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
		dsn = "postgres://odoo:odoo@localhost:5432/odoo?sslmode=disable"
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://app:app@localhost:5672/"
	}

	mqConn := connectRabbitMQ(rabbitURL)
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

	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("listener error: %v", err)
		}
	})

	if err := listener.Listen("hr_employee"); err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	defer listener.Close()

	log.Println("publisher: listening on pg channel 'hr_employee', publishing to rabbitmq queue 'requests'")

	for {
		select {
		case n := <-listener.Notify:
			if n == nil {
				continue
			}
			handleNotify(n, mqChan)
		case <-time.After(90 * time.Second):
			go listener.Ping()
		}
	}
}

func connectRabbitMQ(url string) *amqp.Connection {
	var conn *amqp.Connection
	var err error
	for i := 0; i < 30; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			return conn
		}
		log.Printf("waiting for rabbitmq... (%v)", err)
		time.Sleep(time.Second)
	}
	log.Fatalf("could not connect to rabbitmq: %v", err)
	return nil
}

func handleNotify(n *pq.Notification, mqChan *amqp.Channel) {
	log.Println("========== PG NOTIFY RECEIVED ==========")
	log.Printf("channel: %s", n.Channel)

	var payload []byte
	if json.Valid([]byte(n.Extra)) {
		payload = []byte(n.Extra)
		printEmployeeInfo(payload)
	} else {
		escaped, _ := json.Marshal(n.Extra)
		payload = escaped
		log.Printf("raw: %s", n.Extra)
	}

	err := mqChan.Publish("", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        payload,
	})
	if err != nil {
		log.Printf("PUBLISH FAILED: %v", err)
	} else {
		log.Printf("PUBLISHED to queue '%s' (%d bytes)", queueName, len(payload))
	}
	log.Println("=========================================")
}

func printEmployeeInfo(data []byte) {
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return
	}
	if name, ok := parsed["name"]; ok {
		log.Printf("employee: %v", name)
	}
	if id, ok := parsed["id"]; ok {
		log.Printf("id: %v", id)
	}
	if v, ok := parsed["last_check_in"]; ok {
		log.Printf("last_check_in: %v", v)
	}
	if v, ok := parsed["last_check_out"]; ok {
		log.Printf("last_check_out: %v", v)
	}
}
