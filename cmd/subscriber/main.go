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

	http.HandleFunc("/employees", handleList)

	log.Println("subscriber: listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func consumeWorker(id int, msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		var emp map[string]any
		if err := json.Unmarshal(msg.Body, &emp); err != nil {
			log.Printf("worker %d: invalid json: %v", id, err)
			msg.Nack(false, false)
			continue
		}

		rawData := msg.Body

		// Extract known fields
		var employeeID *int
		if v, ok := emp["id"].(float64); ok {
			i := int(v)
			employeeID = &i
		}

		name, _ := emp["name"].(string)
		workEmail, _ := emp["work_email"].(string)
		barcode, _ := emp["barcode"].(string)

		var lastCheckIn, lastCheckOut *time.Time
		if v, ok := emp["last_check_in"].(string); ok && v != "" {
			if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
				lastCheckIn = &t
			}
		}
		if v, ok := emp["last_check_out"].(string); ok && v != "" {
			if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
				lastCheckOut = &t
			}
		}

		_, err := db.Exec(
			`INSERT INTO hr_employee (employee_id, name, work_email, barcode, last_check_in, last_check_out, raw_data)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			employeeID, name, workEmail, barcode, lastCheckIn, lastCheckOut, rawData,
		)
		if err != nil {
			log.Printf("worker %d: insert error: %v", id, err)
			msg.Nack(false, true)
			continue
		}

		msg.Ack(false)
		log.Printf("worker %d: saved employee_id=%v name=%s", id, employeeID, name)
	}
}

type record struct {
	ID           int             `json:"id"`
	EmployeeID   *int            `json:"employee_id"`
	Name         string          `json:"name"`
	WorkEmail    string          `json:"work_email"`
	Barcode      string          `json:"barcode"`
	LastCheckIn  *time.Time      `json:"last_check_in"`
	LastCheckOut *time.Time      `json:"last_check_out"`
	RawData      json.RawMessage `json:"raw_data"`
	CreatedAt    time.Time       `json:"created_at"`
}

func handleList(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT id, employee_id, name, work_email, barcode, last_check_in, last_check_out, raw_data, created_at
		FROM hr_employee ORDER BY id DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var records []record
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.ID, &rec.EmployeeID, &rec.Name, &rec.WorkEmail, &rec.Barcode,
			&rec.LastCheckIn, &rec.LastCheckOut, &rec.RawData, &rec.CreatedAt); err != nil {
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		records = append(records, rec)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}
