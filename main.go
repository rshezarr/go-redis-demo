package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"time"
)

var (
	ctx = context.Background()
	rdb *redis.Client
	db  *sql.DB
)

func main() {
	var err error

	// Initialize Redis client
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis server address
		Password: "",               // No password set
		DB:       0,                // Use default DB
	})

	// Initialize SQLite database
	db, err = sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer db.Close()

	// Create table and insert sample data
	createTableAndInsertData(db)

	// Set up HTTP server and routes
	r := mux.NewRouter()
	r.HandleFunc("/get-info/{id}", getInfoHandler).Methods("GET")

	http.Handle("/", r)
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func createTableAndInsertData(db *sql.DB) {
	// Create table
	query := `
    CREATE TABLE IF NOT EXISTS info (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT
    );`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert 100 rows of sample data
	for i := 1; i <= 100; i++ {
		reqId := uuid.New().String()
		_, err := db.Exec("INSERT INTO info (id, data) VALUES (?, ?) ON CONFLICT(id) DO NOTHING;", reqId)
		if err != nil {
			log.Fatalf("Failed to insert sample data: %v", err)
		}
	}
}

// Handler for /get-info/{id} endpoint
func getInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Check Redis cache first
	cacheKey := fmt.Sprintf("info:%s", id)
	cachedData, err := rdb.Get(ctx, cacheKey).Result()
	if errors.Is(err, redis.Nil) {
		// Cache miss, retrieve data from SQLite
		var data string
		err := db.QueryRow("SELECT data FROM info WHERE id = ?", id).Scan(&data)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "Data not found", http.StatusNotFound)
				return
			}
			http.Error(w, "Failed to query database", http.StatusInternalServerError)
			return
		}

		// Store data in Redis cache
		err = rdb.Set(ctx, cacheKey, data, 10*time.Minute).Err()
		if err != nil {
			http.Error(w, "Failed to store data in Redis", http.StatusInternalServerError)
			return
		}

		// Return data as JSON response
		response := map[string]string{"data": data}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	} else if err != nil {
		http.Error(w, "Failed to check Redis cache", http.StatusInternalServerError)
		return
	}

	// Cache hit, return cached data as JSON response
	response := map[string]string{"data": cachedData}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
