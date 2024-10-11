package main

import (
    "net/http"
    "sync"
)

var clientIDCounter uint64
var clientIDMutex = &sync.Mutex{}

func generateUniqueClientID() uint64 {
    clientIDMutex.Lock()
    defer clientIDMutex.Unlock()
    clientIDCounter++
    return clientIDCounter
}

// enableCors is a middleware that sets the CORS headers
func enableCors(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Adjust the allowed origin as needed. "*" allows all origins.
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
        if r.Method == "OPTIONS" {
            w.WriteHeader(http.StatusOK)
            return
        }
        next.ServeHTTP(w, r)
    })
}
