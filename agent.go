package main

import (
	"log"
	"sync"
	"time"
    "encoding/json"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gorilla/websocket"
)

type Agent struct {
	conn       *websocket.Conn
	clients    map[uint64]*Client // Map ClientID to Client
	clientsMux sync.RWMutex       // Mutex for clients map
	register   chan *Client
	unregister chan *Client
	messages   chan Message // Channel for incoming messages with ClientID
	done       chan struct{}
	pingPeriod time.Duration
	pongWait   time.Duration
	send       chan Message
	agentID    string  // Agent ID
	server     *Server // Reference to Server

	connected bool
	mu        sync.Mutex
}

// run handles registration and broadcasting to clients
func (agent *Agent) run() {
	for {
		select {
		case message := <-agent.messages:
			// Route the message to the correct client
			agent.clientsMux.RLock()
			client, ok := agent.clients[message.ClientID]
			agent.clientsMux.RUnlock()
			if ok {
				client.send <- message.Data
			} else {
				log.Printf("Client %d not found for agent %s", message.ClientID, agent.agentID)
			}
		}
	}
}

// writePump sends messages from the agent to the client
func (agent *Agent) writePump() {
	for message := range agent.send {
		writer, err := agent.conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			// Handle error
			return
		}
		// Write ClientID (8 bytes)
		err = binary.Write(writer, binary.BigEndian, message.ClientID)
		if err != nil {
			writer.Close()
			return
		}
		// Write MessageLength (4 bytes)
		length := uint32(len(message.Data))
		err = binary.Write(writer, binary.BigEndian, length)
		if err != nil {
			writer.Close()
			return
		}
		// Write MessageData
		_, err = writer.Write(message.Data)
		if err != nil {
			writer.Close()
			return
		}
		writer.Close()
	}
}

// agent.readPump reads from wsConn and broadcasts to clients
func (agent *Agent) readPump() {
	defer func() {
		agent.conn.Close()
		agent.done <- struct{}{}
	}()
	for {
		_, reader, err := agent.conn.NextReader()
		if err != nil {
			log.Printf("Error reading from WebSocket: %v", err)
			break
		}

		for {
			message, err := readMessage(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading message: %v", err)
				break
			}
			if message.ClientID == 0 {
				// Control message
				err := agent.handleControlMessage(message.Data)
				if err != nil {
					log.Printf("Error handling control message: %v", err)
				}
			} else {
				// Data message
				// Send the message to the appropriate client
				agent.messages <- message
			}
		}
	}
}

func (agent *Agent) unregisterClientByID(clientID uint64) {
	agent.unregister <- &Client{ID: clientID}
}

func (agent *Agent) handleControlMessage(data []byte) error {
	// Parse the JSON data
	var controlMsg map[string]interface{}
	err := json.Unmarshal(data, &controlMsg)
	if err != nil {
		return fmt.Errorf("failed to parse control message JSON: %v", err)
	}

	// Extract the message type
	msgType, ok := controlMsg["message_type"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'message_type' field")
	}

	switch msgType {
	case "__activation_request__":
		// Handle activation
		return agent.server.handleActivationRequest(agent.agentID, controlMsg)
	case "__agent_managed_redis_server__":
		return agent.server.handleAgentManagedRedisServer(controlMsg, agent.agentID)
	case "__redis_server_heartbeat__":
		return agent.server.handleRedisServerHeartbeat(controlMsg, agent.agentID)
	default:
		return fmt.Errorf("unknown control message type: %s", msgType)
	}
}

func (agent *Agent) setupPingPong() {
	agent.conn.SetReadDeadline(time.Now().Add(agent.pongWait))
	agent.conn.SetPongHandler(func(string) error {
		agent.conn.SetReadDeadline(time.Now().Add(agent.pongWait))
		return nil
	})
}

func (agent *Agent) startPing() {
	ticker := time.NewTicker(agent.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-agent.done:
			return
		case <-ticker.C:
			if err := agent.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				// Handle disconnection
				log.Printf("Agent %s disconnecting due to ping timeout", agent.agentID)
				agent.disconnect()
				return
			}
		}
	}
}

func (agent *Agent) disconnect() {
	agent.mu.Lock()
	if !agent.connected {
		agent.mu.Unlock()
		return
	}
	agent.connected = false
	agent.mu.Unlock()

	close(agent.done)
	agent.conn.Close()

	// Close agent.send to signal writePump to exit
	close(agent.send)

	// Disconnect all clients
	agent.clientsMux.Lock()
	for _, client := range agent.clients {
		client.conn.Close()
	}
	agent.clientsMux.Unlock()

	// Handle agent disconnect
	agent.server.handleDisconnectForAgent(agent.agentID)
}

// Add a client to the agent
func (agent *Agent) addClient(client *Client) {
	agent.clientsMux.Lock()
	defer agent.clientsMux.Unlock()
	agent.clients[client.ID] = client
}

// Remove a client from the agent
func (agent *Agent) removeClient(client *Client) {
	agent.clientsMux.Lock()
	defer agent.clientsMux.Unlock()
	delete(agent.clients, client.ID)
	client.disconnectOnce.Do(func() {
		close(client.send)
	})
}

// In Agent struct, add the disconnectClients method
func (agent *Agent) disconnectClients(redisServerID string) {
	// Collect client IDs to disconnect
	agent.clientsMux.Lock()
	clientsToDisconnect := make([]uint64, 0)
	for clientID, client := range agent.clients {
		if client.RedisServerID == redisServerID {
			clientsToDisconnect = append(clientsToDisconnect, clientID)
		}
	}
	agent.clientsMux.Unlock()

	// Now disconnect the clients
	for _, clientID := range clientsToDisconnect {
		agent.clientsMux.RLock()
		client, ok := agent.clients[clientID]
		agent.clientsMux.RUnlock()
		if ok {
			client.disconnect()
			agent.removeClient(client)
		}
	}
}

func (agent *Agent) SendMessage(message Message) error {
	agent.mu.Lock()
	connected := agent.connected
	agent.mu.Unlock()

	if !connected {
		return fmt.Errorf("agent is disconnected")
	}

	select {
	case agent.send <- message:
		return nil
	default:
		return fmt.Errorf("agent send channel is full or closed")
	}
}
