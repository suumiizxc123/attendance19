package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/appdb?sslmode=disable"
	}

	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		panic("cannot open db: " + err.Error())
	}
	if err = db.Ping(); err != nil {
		panic("cannot ping db: " + err.Error())
	}

	// ensure table exists
	db.Exec(`CREATE TABLE IF NOT EXISTS requests (
		id SERIAL PRIMARY KEY,
		method TEXT NOT NULL,
		path TEXT NOT NULL,
		headers JSONB,
		body JSONB,
		created_at TIMESTAMPTZ DEFAULT now()
	)`)

	// clean before tests
	db.Exec(`DELETE FROM requests`)

	os.Exit(m.Run())
}

func TestHandleAny_JSONBody(t *testing.T) {
	body := `{"name":"alice","age":30}`
	req := httptest.NewRequest(http.MethodPost, "/test-path", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleAny(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["status"] != "saved" {
		t.Fatalf("expected status=saved, got %v", resp["status"])
	}
	if resp["id"] == nil || resp["id"].(float64) < 1 {
		t.Fatalf("expected valid id, got %v", resp["id"])
	}
}

func TestHandleAny_EmptyBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/empty", nil)
	w := httptest.NewRecorder()

	handleAny(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "saved" {
		t.Fatalf("expected status=saved, got %v", resp["status"])
	}
}

func TestHandleAny_NonJSONBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/plain", bytes.NewBufferString("hello world"))
	w := httptest.NewRecorder()

	handleAny(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "saved" {
		t.Fatalf("expected status=saved, got %v", resp["status"])
	}
}

func TestHandleAny_DifferentMethods(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			body := `{"method":"` + method + `"}`
			req := httptest.NewRequest(method, "/method-test", bytes.NewBufferString(body))
			w := httptest.NewRecorder()

			handleAny(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d", method, w.Code)
			}
		})
	}
}

func TestHandleList(t *testing.T) {
	// insert a known record first
	body := `{"list_test":true}`
	req := httptest.NewRequest(http.MethodPost, "/for-list", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	handleAny(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("setup insert failed: %d", w.Code)
	}

	// now list
	req = httptest.NewRequest(http.MethodGet, "/requests", nil)
	w = httptest.NewRecorder()
	handleList(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var records []record
	if err := json.NewDecoder(w.Body).Decode(&records); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected at least 1 record")
	}

	// most recent should be our insert
	found := false
	for _, r := range records {
		if r.Path == "/for-list" {
			found = true
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			break
		}
	}
	if !found {
		t.Error("did not find /for-list record in list response")
	}
}

func TestHandleAny_NestedJSON(t *testing.T) {
	body := `{"user":{"name":"bob","tags":["a","b"]},"count":5}`
	req := httptest.NewRequest(http.MethodPost, "/nested", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	handleAny(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleAny_ArrayBody(t *testing.T) {
	body := `[1, 2, 3, "four"]`
	req := httptest.NewRequest(http.MethodPost, "/array", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	handleAny(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}
