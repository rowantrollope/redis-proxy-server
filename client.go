package main

import (
    "log"
    "net"
    "sync"
    "time"
)

type Client struct {
    ID             uint64
    conn           net.Conn
    send           chan []byte
    lastActive     time.Time
    RedisServerID  string
    disconnectOnce sync.Once
    disconnectChan chan struct{}
}

// writePump sends messages from the agent to the client
func (client *Client) writePump() {
    for data := range client.send {
        _, err := client.conn.Write(data)
        if err != nil {
            // Handle error
            break
        }
    }
}

// monitorInactivity handles client disconnection after inactivity
func (client *Client) monitorInactivity(timeout time.Duration) {
    go func() {
        ticker := time.NewTicker(time.Minute)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                if time.Since(client.lastActive) > timeout {
                    log.Printf("Client %d inactive for %v, disconnecting", client.ID, timeout)
                    client.disconnect()
                    return
                }
            case <-client.disconnectChan:
                // Stop monitoring when client is disconnected
                return
            }
        }
    }()
}

// disconnect safely closes the client connection
func (client *Client) disconnect() {
    client.disconnectOnce.Do(func() {
        close(client.disconnectChan)
        client.conn.Close()
        close(client.send)
    })
}

// Other client methods (if any) go here...