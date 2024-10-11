package main

import (
    "log"
    "os"

    "github.com/joho/godotenv"
    "github.com/go-redis/redis/v8"
)

func main() {
    // Configure the logger to include timestamps and file line numbers
    log.SetFlags(log.LstdFlags | log.Lshortfile)
    
    // Load environment variables from .env file
    err := godotenv.Load()
    if err != nil {
        log.Println("No .env file found or error loading .env file, proceeding to use system environment variables")
    }
    
    server := NewServer(DISCONNECT_NONE)

    // Read Redis configuration from environment variables
    redisAddr := os.Getenv("REDIS_ADDRESS")
    redisPassword := os.Getenv("REDIS_PASSWORD")

    // Check if Redis address is set
    if redisAddr == "" {
        log.Fatal("REDIS_ADDRESS environment variable is not set")
    }

    // Initialize Redis client
    server.rdb = redis.NewClient(&redis.Options{
        Addr:     redisAddr,
        Password: redisPassword,
    })

    // Test Redis connection
    _, err = server.rdb.Ping(server.ctx).Result()
    if err != nil {
        log.Fatal("Error connecting to Redis:", err)
    }

    log.Println("Connected to Redis")

    // Start the server
    server.Start()
}