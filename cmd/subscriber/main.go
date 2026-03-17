package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var db *sql.DB

const (
	queueName  = "requests"
	numWorkers = 5
)

func main() {
	// Connect to PostgreSQL
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/subdb?sslmode=disable"
	}

	var err error
	for i := 0; i < 30; i++ {
		db, err = sql.Open("postgres", dsn)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		log.Printf("waiting for db... (%v)", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatalf("could not connect to db: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)

	// Connect to RabbitMQ
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://app:app@localhost:5672/"
	}

	var mqConn *amqp.Connection
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

	// Start concurrent consumers
	msgs, err := mqChan.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("could not start consumer: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumeWorker(id, msgs)
		}(i)
	}

	log.Printf("subscriber: consuming queue '%s' with %d workers", queueName, numWorkers)

	// HTTP server for listing saved data
	http.HandleFunc("/requests", handleList)

	log.Println("subscriber: listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

type requestPayload struct {
	ID        int             `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Headers   json.RawMessage `json:"headers"`
	Body      json.RawMessage `json:"body"`
	CreatedAt string          `json:"created_at"`
}

func consumeWorker(id int, msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		var payload requestPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			log.Printf("worker %d: invalid json: %v — saving raw", id, err)
			raw, _ := json.Marshal(string(msg.Body))
			_, dbErr := db.Exec(
				`INSERT INTO requests (method, path, headers, body) VALUES ($1, $2, $3, $4)`,
				"MQTT", msg.RoutingKey, "{}", raw,
			)
			if dbErr != nil {
				log.Printf("worker %d: insert error: %v", id, dbErr)
				msg.Nack(false, true)
				continue
			}
			msg.Ack(false)
			continue
		}

		headers := payload.Headers
		if len(headers) == 0 {
			headers = json.RawMessage("{}")
		}
		body := payload.Body
		if len(body) == 0 {
			body = json.RawMessage("null")
		}

		_, err := db.Exec(
			`INSERT INTO requests (source_id, method, path, headers, body) VALUES ($1, $2, $3, $4, $5)`,
			payload.ID, payload.Method, payload.Path, headers, body,
		)
		if err != nil {
			log.Printf("worker %d: insert error: %v", id, err)
			msg.Nack(false, true)
			continue
		}

		msg.Ack(false)
		log.Printf("worker %d: saved source_id=%d path=%s", id, payload.ID, payload.Path)
	}
}

type record struct {
	ID        int             `json:"id"`
	SourceID  *int            `json:"source_id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Headers   json.RawMessage `json:"headers"`
	Body      json.RawMessage `json:"body"`
	CreatedAt time.Time       `json:"created_at"`
}

func handleList(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT id, source_id, method, path, headers, body, created_at FROM requests ORDER BY id DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var records []record
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.ID, &rec.SourceID, &rec.Method, &rec.Path, &rec.Headers, &rec.Body, &rec.CreatedAt); err != nil {
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		records = append(records, rec)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}
