package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/appdb?sslmode=disable"
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

	http.HandleFunc("/", handleAny)
	http.HandleFunc("/requests", handleList)

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleAny(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/requests" {
		handleList(w, r)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Store body as raw JSON; if not valid JSON, wrap as string
	var bodyJSON json.RawMessage
	if len(bodyBytes) == 0 {
		bodyJSON = json.RawMessage("null")
	} else if json.Valid(bodyBytes) {
		bodyJSON = bodyBytes
	} else {
		escaped, _ := json.Marshal(string(bodyBytes))
		bodyJSON = escaped
	}

	// Collect headers as JSON
	headersJSON, _ := json.Marshal(r.Header)

	var id int
	err = db.QueryRow(
		`INSERT INTO requests (method, path, headers, body) VALUES ($1, $2, $3, $4) RETURNING id`,
		r.Method, r.URL.RequestURI(), headersJSON, bodyJSON,
	).Scan(&id)
	if err != nil {
		log.Printf("insert error: %v", err)
		http.Error(w, "failed to save request", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "saved",
		"id":     id,
	})
}

type record struct {
	ID        int              `json:"id"`
	Method    string           `json:"method"`
	Path      string           `json:"path"`
	Headers   json.RawMessage  `json:"headers"`
	Body      json.RawMessage  `json:"body"`
	CreatedAt time.Time        `json:"created_at"`
}

func handleList(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT id, method, path, headers, body, created_at FROM requests ORDER BY id DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var records []record
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.ID, &rec.Method, &rec.Path, &rec.Headers, &rec.Body, &rec.CreatedAt); err != nil {
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		records = append(records, rec)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}
