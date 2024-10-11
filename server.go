package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

type Server struct {
	agentUpgrader websocket.Upgrader
	agents        sync.Map // Concurrent safe map[string]*Agent
	basePort      int
	port          int

	rdb *redis.Client   // Redis client for database
	ctx context.Context // Context for Redis operations

	listenerPerRedisServerID      map[string]net.Listener
	listenerPerRedisServerIDMutex sync.RWMutex

	clientDisconnectMethod ClientDisconnectMethod

	activeClients      map[string]int
	activeClientsMutex sync.Mutex

	reconnectedAgents *sync.Map // Map to track reconnected agents
}

// NewServer initializes a new Server instance
func NewServer(disconnectMethod ClientDisconnectMethod) *Server {
	port := 8080 // Default port
	if envPort := os.Getenv("PORT"); envPort != "" {
		if parsedPort, err := strconv.Atoi(envPort); err == nil {
			port = parsedPort
			log.Printf("Using port %d from environment variable", port)
		} else {
			port = 8080
			log.Printf("Invalid PORT environment variable, using default port %d", port)
		}
	} else {
		log.Printf("No Environment Variable for port")
	}

	return &Server{
		agentUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		basePort:                 6400,
		port:                     port,
		ctx:                      context.Background(),
		listenerPerRedisServerID: make(map[string]net.Listener),
		clientDisconnectMethod:   disconnectMethod,
		activeClients:            make(map[string]int),
	}
}

// Start initializes the HTTP server and routes
func (s *Server) Start() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register your handlers
	mux.HandleFunc("/agent", s.handleAgentConnection)
	mux.HandleFunc("/servers", s.handleGetServers)
	mux.HandleFunc("/stats", s.handleGetStats)
	mux.HandleFunc("/connect", s.handleConnectToRedisServer)
	mux.HandleFunc("/disconnect", s.handleDisconnectFromRedisServer)
	mux.HandleFunc("/activation/claim", s.handleActivationClaim)
	mux.HandleFunc("/activation/deactivate", s.handleDeactivateRedisServer)

	// Wrap the mux with the CORS middleware
	handler := enableCors(mux)

	// After a timeout reconnection checks
	go s.checkAgentReconnections()

	// Start the HTTP server with the handler
	log.Printf("Server listening on :%d for agents and API requests", s.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", s.port), handler); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

}

// checkAgentReconnections checks for agent reconnections upon server startup
func (s *Server) checkAgentReconnections() {
	// Define the timeout duration for agents to reconnect
	const reconnectionTimeout = 30 * time.Second

	// Retrieve all known agent IDs
	agentIDs, err := s.getAllKnownAgentIDs()
	if err != nil {
		log.Printf("Error retrieving known agent IDs: %v", err)
		return
	}

	// Map to track whether agents have reconnected
	s.reconnectedAgents = &sync.Map{}

	// For each agent ID, start a timer to wait for reconnection
	for _, agentID := range agentIDs {
		go func(agentID string) {
			timer := time.NewTimer(reconnectionTimeout)
			select {
			case <-timer.C:
				// Timer expired, check if agent has reconnected
				if _, ok := s.LoadAgent(agentID); !ok {
					// Agent has not reconnected, mark associated Redis servers as "AGENT_UNREACHABLE"
					log.Printf("Agent %s did not reconnect within the timeout period", agentID)
					s.handleAgentDisconnect(agentID)
				} else {
					log.Printf("Agent %s reconnected", agentID)
				}
			}
		}(agentID)
	}
}

// StoreAgent stores the agent instance
func (s *Server) StoreAgent(agentID string, agent *Agent) {
	s.agents.Store(agentID, agent)
}

func (s *Server) RemoveAgent(agentID string) {
	s.agents.Delete(agentID)
}

// LoadAgent retrieves the agent instance
func (s *Server) LoadAgent(agentID string) (*Agent, bool) {
	value, ok := s.agents.Load(agentID)
	if !ok {
		return nil, false
	}
	return value.(*Agent), true
}

func (s *Server) incrementClientCounter(redisServerID string) {
	if s.clientDisconnectMethod == DISCONNECT_AUTO {
		s.activeClientsMutex.Lock()
		s.activeClients[redisServerID]++
		s.activeClientsMutex.Unlock()
	}
}

func (s *Server) decrementClientCounter(redisServerID string) {
	if s.clientDisconnectMethod == DISCONNECT_AUTO {
		s.activeClientsMutex.Lock()
		s.activeClients[redisServerID]--
		count := s.activeClients[redisServerID]
		s.activeClientsMutex.Unlock()

		// Check if count is zero and close the listener if needed
		if count == 0 {
			s.closeListenerIfNoClients(redisServerID)
		}
	}
}

// generateActivationCode generates a cryptographically secure activation code
func (server *Server) generateActivationCode() (string, error) {
	// I and O are removed to avoid confusion with 1 and 0
	const charset = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
	code := make([]byte, 8) // 8 characters (without the dash)

	for i := range code {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		code[i] = charset[num.Int64()]
	}

	// Insert the dash in the middle
	activationCode := fmt.Sprintf("%s-%s", string(code[:4]), string(code[4:]))
	return activationCode, nil
}

// closeListenerIfNoClients checks if there are no active clients for the given redisServerID
// and closes the listener if that's the case.
func (s *Server) closeListenerIfNoClients(redisServerID string) {
	s.activeClientsMutex.Lock()
	count := s.activeClients[redisServerID]
	s.activeClientsMutex.Unlock()

	if count > 0 {
		// There are still active clients; no action needed.
		return
	}

	// Close the listener for redisServerID
	s.listenerPerRedisServerIDMutex.Lock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	if exists {
		log.Printf("Closing listener for redisServerID %s as there are no active clients", redisServerID)
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener for redisServerID %s: %v", redisServerID, err)
		}
		delete(s.listenerPerRedisServerID, redisServerID)
	}
	s.listenerPerRedisServerIDMutex.Unlock()
}

func (s *Server) handleDisconnectForRedisServerID(redisServerID string) error {
	// Close the listener for redisServerID
	s.listenerPerRedisServerIDMutex.Lock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	if exists {
		log.Printf("Closing listener for redisServerID %s", redisServerID)
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener for redisServerID %s: %v", redisServerID, err)
			s.listenerPerRedisServerIDMutex.Unlock()
			return err
		}
		delete(s.listenerPerRedisServerID, redisServerID)
		log.Printf("Listener for redisServerID %s has been closed and removed from the map", redisServerID)
	} else {
		log.Printf("Listener for redisServerID %s not found", redisServerID)
	}
	s.listenerPerRedisServerIDMutex.Unlock()

	// Now disconnect clients associated with the redisServerID
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redisServerID %s", redisServerID)
		return nil // Or return an error if necessary
	}

	agent, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		return nil // Or return an error if necessary
	}

	// Instruct the agent to disconnect clients associated with redisServerID
	agent.disconnectClients(redisServerID)

	return nil
}

// handleDisconnectFromRedisServer handles the API request to disconnect from an agent
func (s *Server) handleDisconnectFromRedisServer(w http.ResponseWriter, r *http.Request) {
	redisServerID := r.URL.Query().Get("redis_server_id")
	log.Printf("Received /disconnect request for redis_server_id: %s", redisServerID)
	if redisServerID == "" {
		http.Error(w, "redis_server_id is required", http.StatusBadRequest)
		return
	}

	// Proceed to disconnect the agent
	err := s.handleDisconnectForRedisServerID(redisServerID)
	if err != nil {
		log.Printf("Error disconnecting redis_server_id %s: %v", redisServerID, err)
		http.Error(w, "Failed to disconnect", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully disconnected redis_server_id %s", redisServerID)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Disconnected successfully"))
}

func (s *Server) createListenerForRedisServerID(redisServerID string) net.Listener {
	// Find an available port
	var listener net.Listener
	var err error
	for port := s.basePort; port < 65535; port++ {
		address := "127.0.0.1:" + strconv.Itoa(port)
		listener, err = net.Listen("tcp", address)
		if err == nil {
			log.Printf("Allocated port %d for redisServerID %s", port, redisServerID)
			break
		}
	}
	if listener == nil {
		log.Printf("No available ports to allocate for redisServerID %s", redisServerID)
		return nil
	}

	log.Printf("Listener started on %s for redisServerID %s", listener.Addr().String(), redisServerID)

	// Start accepting client connections on the new port
	go s.acceptClients(listener, redisServerID)
	return listener
}

// handleDisconnectForAgent closes the listener for the given agentID
func (s *Server) handleDisconnectForAgent(agentID string) error {
	log.Printf("Disconnecting agent %s - TODO: nothing done", agentID)
	return nil
}

func (s *Server) handleRedisServerHeartbeat(controlMsg map[string]interface{}, agentID string) error {
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_server_id' field")
	}

	// only update and store heartbeat data if the server is claimed by an agent
	_, err := s.getAgentIDForRedisServerID(redisServerID)
	if err != nil {
		log.Printf("Server is not activated %s - discarding heartbeat", redisServerID)
		return err
	}

	status, ok := controlMsg["status"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'status' field")
	}

	statsJSON, ok := controlMsg["redis_stats"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_stats' field")

	}

	// Unmarshal the JSON string into a map
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(statsJSON), &stats); err != nil {
		log.Printf("Failed to unmarshal redis_stats JSON: %v", err)
		return err
	}

	// Flatten the stats map to string fields
	flatStats := make(map[string]interface{})
	for k, v := range stats {
		flatStats[k] = fmt.Sprint(v)
	}

	if status == "RUNNING" {
		// Add status and timestamp to the stats
		flatStats["status"] = status
		flatStats["timestamp"] = time.Now().Unix()

		// Store in Redis Stream
		streamKey := fmt.Sprintf("redis_server_id:%s:heartbeat_stream", redisServerID)
		_, err = s.rdb.XAdd(s.ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: 1000,
			Approx: true,
			Values: flatStats,
		}).Result()
		if err != nil {
			log.Printf("Error adding heartbeat to stream: %v", err)
			return err
		}
	}

	// Store the status and timestamp to the serverInfo
	serverInfoKey := fmt.Sprintf("redis_server_id:%s", redisServerID)

	if s.rdb.Exists(s.ctx, serverInfoKey).Val() == 1 {
		_, err = s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.timestamp", time.Now().Unix()).Result()
		if err != nil {
			log.Printf("Error storing timestamp to serverInfo JSON: %v", err)
			return err
		}
		statusString := fmt.Sprintf("\"%s\"", status)
		_, err = s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.status", statusString).Result()
		if err != nil {
			log.Printf("Error storing status to serverInfo JSON: %v", err)
			return err
		}
	}

	return nil
}

func (s *Server) handleAgentManagedRedisServer(controlMsg map[string]interface{}, agentID string) error {
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_server_id' field")
	}

	// Associate the agentID with the redisServerID in Redis
	return s.associateAgentWithRedisServerID(agentID, redisServerID)
}

func (s *Server) handleActivationRequest(agentID string, controlMsg map[string]interface{}) error {
	// Extract the UUID of the registering server
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("activation request message missing 'redis_server_uuid' field")
	}

	// Check if the redisServerID is already associated with another agent
	existingAgentID, err := s.getAgentIDForRedisServerID(redisServerID)
	if err == nil && existingAgentID != agentID {
		// Remove the redisServerID from the old agent
		log.Printf("Removing redisServerID %s from old agent %s", redisServerID, existingAgentID)
		err = s.removeRedisServerIDFromAgent(existingAgentID, redisServerID)
		if err != nil {
			log.Printf("Error removing redisServerID %s from old agent %s: %v", redisServerID, existingAgentID, err)
			return err
		}
	}

	// Associate the agentID with the redisServerID in Redis
	err = s.associateAgentWithRedisServerID(agentID, redisServerID)
	if err != nil {
		log.Printf("Error associating agent with redisServerID %s: %v", redisServerID, err)
		return err
	}

	// Extract `request_id` from the control message
	requestID, ok := controlMsg["request_id"].(string)
	if !ok {
		return fmt.Errorf("activation request message missing 'request_id' field")
	}

	// Attempt to retrieve existing RedisServerInfo
	serverInfo, err := s.readServerInfo(redisServerID)

	if err != nil {
		// No existing server info, create new with ActivationStatePending
		activationCode, err := s.getActivationCode(redisServerID)

		if err != nil {
			return fmt.Errorf("error generating activation code: %v", err)
		}

		serverInfo = RedisServerInfo{
			RedisServerID:   redisServerID,
			AgentID:         agentID,
			AccountID:       "",
			Status:          "RUNNING",
			Timestamp:       time.Now().Unix(),
			ActivationState: string(ActivationStatePending),
		}

		// Store the new RedisServerInfo
		err = s.writeServerInfo(redisServerID, serverInfo)
		if err != nil {
			return fmt.Errorf("error setting server info: %v", err)
		}

		// Send activation code to the agent
		// send status = "new_server" and activation_code
		responseData := map[string]string{"state": string(ActivationStatePending), "activation_code": activationCode}

		return s.sendActivationResponse(agentID, requestID, responseData)

	} else {

		// Handle based on the current activation state
		switch serverInfo.ActivationState {
		case string(ActivationStatePending):

			// Send existing activation code to the agent
			activationCode, err := s.getActivationCode(redisServerID)

			if err == nil {
				responseData := map[string]string{"activation_code": activationCode}
				return s.sendActivationResponse(agentID, requestID, responseData)
			} else {
				return fmt.Errorf("error retrieving activation code: %v", err)
			}

		case string(ActivationStateActivated):
			responseData := map[string]bool{"activated": true}
			return s.sendActivationResponse(agentID, requestID, responseData)

		case string(ActivationStateDeactivated):
			responseData := map[string]bool{"activated": false}
			return s.sendActivationResponse(agentID, requestID, responseData)

		default:
			return fmt.Errorf("unknown activation state: %s", serverInfo.ActivationState)
		}
	}

}

func (s *Server) claimActivationCode(activationCode string) (string, error) {

	// Check if the activation code is pending
	redisServerID, err := s.rdb.Get(s.ctx, "activation_code:"+activationCode).Result()
	if err == redis.Nil {
		// Activation code not found or already claimed
		return "", fmt.Errorf("invalid or already claimed activation code")
		//http.Error(w, "Invalid or already claimed activation code", http.StatusNotFound)

	} else if err != nil {
		log.Println("Error checking pending activation:", err)
		return "", fmt.Errorf("internal server error", err)
		//http.Error(w, "Internal server error", http.StatusInternalServerError)

	}

	// Remove the pending activation
	err = s.rdb.Del(s.ctx, "activation_code:"+activationCode).Err()
	if err != nil {
		log.Println("Error deleting pending activation:", err)
		return "", fmt.Errorf("internal server error", err)
		//http.Error(w, "Internal server error", http.StatusInternalServerError)

	}

	// Remove the server_id mapping
	err = s.rdb.Del(s.ctx, "redis_server_id:"+redisServerID+":activation_code").Err()
	if err != nil {
		log.Println("Error deleting activation code for redis_server_id:", err)
		return "", fmt.Errorf("internal server error", err)
		//http.Error(w, "Internal server error", http.StatusInternalServerError)
	}

	return redisServerID, nil
}

// getActivationCode retrieves the activation code for a given redisServerID or generates a new one if not found
func (s *Server) getActivationCode(redisServerID string) (string, error) {
	activationCodeKey := "activation_code:"

	// Check if the activation code is already stored/pending and return that
	activationCode, err := s.rdb.Get(s.ctx, "redis_server_id:"+redisServerID+":activation_code").Result()
	if err == nil {
		return activationCode, nil
	}

	// Generate a new activation code if not found
	activationCode, err = s.generateActivationCode()

	if err != nil {
		return "", fmt.Errorf("error generating activation code: %v", err)
	}

	// Store the new activation code
	err = s.rdb.Set(s.ctx, "redis_server_id:"+redisServerID+":activation_code", activationCode, 24*time.Hour).Err()
	if err != nil {
		return "", fmt.Errorf("error storing activation code: %v", err)
	}

	// Store activation code with expiration
	err = s.rdb.Set(s.ctx, activationCodeKey+activationCode, redisServerID, 24*time.Hour).Err()
	if err != nil {
		return "", fmt.Errorf("error storing activation code: %v", err)
	}

	return activationCode, nil
}

func (s *Server) sendActivationResponse(agentID, requestID string, responseData interface{}) error {
	agent, ok := s.LoadAgent(agentID)
	if !ok {
		return fmt.Errorf("agent %s not found", agentID)
	}

	controlResp := map[string]interface{}{
		"message_type": "__activation_response__",
		"request_id":   requestID,
		"status":       "success",
		"data":         responseData,
	}

	respData, err := json.Marshal(controlResp)
	if err != nil {
		return fmt.Errorf("failed to marshal activation response: %v", err)
	}

	message := Message{
		ClientID: 0,
		Data:     respData,
	}

	agent.send <- message
	return nil
}

func (s *Server) sendControlMessage(agentID string, controlData map[string]interface{}) error {
	agent, ok := s.LoadAgent(agentID)
	if !ok {
		return fmt.Errorf("agent %s not found", agentID)
	}

	// Serialize the control data to JSON
	jsonData, err := json.Marshal(controlData)
	if err != nil {
		return fmt.Errorf("failed to marshal control data: %v", err)
	}

	// Create a Message with ClientID == 0
	message := Message{
		ClientID: 0,
		Data:     jsonData,
	}

	// Send the control message to the agent
	agent.send <- message

	return nil
}

// handleConnectToRedisServer handles the API request to connect to an agent /Connect
func (s *Server) handleConnectToRedisServer(w http.ResponseWriter, r *http.Request) {
	redisServerID := r.URL.Query().Get("redis_server_id")
	log.Printf("Received /connect request for redis_server_id: %s", redisServerID)
	if redisServerID == "" {
		http.Error(w, "redis_server_id is required", http.StatusBadRequest)
		return
	}

	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redis_server_id %s", redisServerID)
		http.Error(w, "Agent not found for the provided redis_server_id", http.StatusNotFound)
		return
	}

	// Check if the agent is connected
	_, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		http.Error(w, "Agent not found", http.StatusNotFound)
		return
	}

	// Retrieve the existing listener for this redisServerID
	s.listenerPerRedisServerIDMutex.RLock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	s.listenerPerRedisServerIDMutex.RUnlock()

	if !exists {
		// Create a new listener for the redisServerID
		listener = s.createListenerForRedisServerID(redisServerID)
		if listener == nil {
			log.Printf("Failed to create listener for redis_server_id %s", redisServerID)
			http.Error(w, "Failed to create listener", http.StatusInternalServerError)
			return
		}

		// Store the listener
		s.listenerPerRedisServerIDMutex.Lock()
		s.listenerPerRedisServerID[redisServerID] = listener
		s.listenerPerRedisServerIDMutex.Unlock()
	}

	portNum := listener.Addr().(*net.TCPAddr).Port
	response := map[string]int{"port": portNum}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)

	if err != nil {
		log.Printf("Error sending response to client: %v", err)
	}

	log.Printf("Responded to /connect request with port %d for redis_server_id %s", portNum, redisServerID)

}

// acceptClients accepts client connections on the given listener for a specific agent
func (s *Server) acceptClients(listener net.Listener, redisServerID string) {
	log.Printf("Starting to accept clients for redisServerID %s on %s", redisServerID, listener.Addr().String())
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Listener for redisServerID %s closed or error occurred: %v", redisServerID, err)
			return
		}
		log.Printf("Accepted new client connection for redisServerID %s from %s", redisServerID, clientConn.RemoteAddr().String())
		go s.handleClient(clientConn, redisServerID)
	}
}

// handleClient handles TCP connections from clients to the specified agent
func (s *Server) handleClient(clientConn net.Conn, redisServerID string) {

	// Retrieve the agentID associated with the redisServerID
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redisServerID %s", redisServerID)
		clientConn.Close()
		return
	}

	agent, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		clientConn.Close()
		return
	}

	clientID := generateUniqueClientID()

	client := &Client{
		ID:             clientID,
		conn:           clientConn,
		send:           make(chan []byte, 256),
		lastActive:     time.Now(),
		RedisServerID:  redisServerID,
		disconnectChan: make(chan struct{}),
	}

	// Add client to agent's client map
	agent.addClient(client)

	// Send __client_connect__ control message to the agent
	controlData := map[string]interface{}{
		"message_type":    "__client_connected__",
		"client_id":       client.ID,
		"redis_server_id": redisServerID,
	}
	// Serialize the control data to JSON
	jsonData, err := json.Marshal(controlData)
	if err != nil {
		log.Printf("Error marshaling connect control message: %v", err)
		clientConn.Close()
		return
	}
	// Create a control message with ClientID == 0
	message := Message{
		ClientID: 0,
		Data:     jsonData,
	}
	agent.send <- message

	// Start the client's write pump
	go client.writePump()

	switch s.clientDisconnectMethod {
	case DISCONNECT_TIMEOUT:
		// Set timeout duration as needed, e.g., 5 minutes
		client.monitorInactivity(5 * time.Minute)
	case DISCONNECT_AUTO:
		s.incrementClientCounter(redisServerID)
	}

	// Read from client and send to agent
	buf := make([]byte, 4096)
	for {
		n, err := client.conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client: %v", err)
			}
			break // Exit the loop when the client disconnects or an error occurs
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		// Refresh last activity time
		client.lastActive = time.Now()

		// Create a Message with ClientID and Data
		message := Message{
			ClientID: client.ID,
			Data:     data,
		}

		// Send the message to the agent using SendMessage()
		if err := agent.SendMessage(message); err != nil {
			log.Printf("Error sending message to agent %s: %v", agent.agentID, err)
			break // Exit the loop if agent is disconnected or send fails
		}
	}

	// Deferred function will handle client disconnection
	defer func() {
		// Remove client from agent's client map
		agent.removeClient(client)
		client.disconnect()

		log.Printf("Disconnecting client: %s", client.conn.RemoteAddr().String())

		// Decrement client counter if AUTO method is selected
		if s.clientDisconnectMethod == DISCONNECT_AUTO {
			s.decrementClientCounter(redisServerID)
		}

		// Send a __client_disconnect__ control message to the agent
		controlData := map[string]interface{}{
			"message_type": "__client_disconnect__",
			"client_id":    client.ID,
		}
		jsonData, err := json.Marshal(controlData)
		if err != nil {
			log.Printf("Error marshaling disconnect control message: %v", err)
			return
		}
		message := Message{
			ClientID: 0,
			Data:     jsonData,
		}
		agent.send <- message
	}()
}

func (s *Server) handleDeactivateRedisServer(w http.ResponseWriter, r *http.Request) {
	redisServerID := r.URL.Query().Get("redisServerId")
	if redisServerID == "" {
		http.Error(w, "Missing redisServerID parameter", http.StatusBadRequest)
		return
	}

	// Read the server info
	serverInfo, err := s.readServerInfo(redisServerID)
	if err != nil {
		http.Error(w, "Redis server not found", http.StatusNotFound)
		return
	}

	// Set the activation state to DEACTIVATED
	serverInfo.ActivationState = string(ActivationStateDeactivated)

	// Update the server info in Redis
	err = s.writeServerInfo(redisServerID, serverInfo)
	if err != nil {
		http.Error(w, "Failed to update server info", http.StatusInternalServerError)
		return
	}

	// Remove the association between the agentID and the redisServerID
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)
	if err != nil {
		// No agent found, proceed
	} else {
		// Send control message to the agent to stop managing this redisServerID
		controlMsg := map[string]interface{}{
			"message_type":    "__deactivate_redis_server__",
			"redis_server_id": redisServerID,
		}
		err = s.sendControlMessage(agentID, controlMsg)
		if err != nil {
			log.Println("Error sending control message to agent:", err)
		}

		// Remove the mapping in Redis
		err = s.rdb.SRem(s.ctx, "agentIDToRedisServerIDs:"+agentID, redisServerID).Err()
		if err != nil {
			log.Println("Error removing redisServerID from agentIDToRedisServerIDs:", err)
		}
		err = s.rdb.Del(s.ctx, "redisServerIDToAgentID:"+redisServerID).Err()
		if err != nil {
			log.Println("Error deleting redisServerIDToAgentID mapping:", err)
		}
	}

	// Remove redisServerID from accountServers
	accountID := serverInfo.AccountID
	if accountID != "" {
		err = s.rdb.SRem(s.ctx, "accountServers:"+accountID, redisServerID).Err()
		if err != nil {
			log.Println("Error removing redisServerID from accountServers:", err)
		}
	}

	// Delete the server info from Redis
	err = s.rdb.Del(s.ctx, "redis_server_id:"+redisServerID).Err()
	if err != nil {
		log.Println("Error deleting server info from Redis:", err)
	}

	// Delete the heartbeat stream
	streamKey := fmt.Sprintf("redis_server_id:%s:heartbeat_stream", redisServerID)
	err = s.rdb.Del(s.ctx, streamKey).Err()
	if err != nil {
		log.Println("Error deleting heartbeat stream from Redis:", err)
	}

	// Respond success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Redis server deactivated and removed successfully"))
}

// Handle activation claim requests from clients (management console)
func (s *Server) handleActivationClaim(w http.ResponseWriter, r *http.Request) {
	activationCode := r.URL.Query().Get("activationCode")
	accountID := r.URL.Query().Get("accountID")

	log.Println("handleActivationClaim activation code: " + activationCode + " accountID: " + accountID)

	if activationCode == "" || accountID == "" {
		http.Error(w, "Missing activationCode or accountID parameter", http.StatusBadRequest)
		return
	}

	redisServerID, err := s.claimActivationCode(activationCode)
	if err != nil {
		log.Println("Error claiming activation code:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create the ServerInfo object
	redisServerInfo := RedisServerInfo{
		AccountID:       accountID,
		Status:          "RUNNING",
		Timestamp:       time.Now().Unix(),
		ActivationState: string(ActivationStateActivated),
		RedisServerID:   redisServerID,
	}

	err = s.writeServerInfo(redisServerID, redisServerInfo)
	if err != nil {
		log.Println("Error writing server info:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Add redisServerID to the set of servers for the accountID
	err = s.rdb.SAdd(s.ctx, "accountServers:"+accountID, redisServerID).Err()
	if err != nil {
		log.Println("Error adding UUID to accountServers:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Inform the agent that the server is activated - send a control message "__activation_complete__"
	controlMsg := map[string]interface{}{
		"message_type":    "__activation_claimed__",
		"redis_server_id": redisServerID,
	}
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)
	if err != nil {
		log.Printf("Error retrieving agentID for redisServerID %s: %v", redisServerID, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	s.sendControlMessage(agentID, controlMsg)

	// Respond with success
	log.Println("handleActivationClaim success")
	response := map[string]string{
		"status":          "activation code claimed successfully",
		"redis_server_id": redisServerID,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// getAllKnownAgentIDs retrieves all known agent IDs from Redis
func (s *Server) getAllKnownAgentIDs() ([]string, error) {
	return s.rdb.SMembers(s.ctx, "knownAgentIDs").Result()
}

// Store mapping from agentID to redisServerID
func (s *Server) associateAgentWithRedisServerID(agentID, redisServerID string) error {
	// Add agentID to the set of known agents
	err := s.rdb.SAdd(s.ctx, "knownAgentIDs", agentID).Err()
	if err != nil {
		return err
	}

	key := "agentIDToRedisServerIDs:" + agentID
	err = s.rdb.SAdd(s.ctx, key, redisServerID).Err()
	if err != nil {
		return err
	}
	// Also store reverse mapping
	return s.rdb.Set(s.ctx, "redisServerIDToAgentID:"+redisServerID, agentID, 0).Err()
}

// Get all redisServerIDs associated with an agentID
func (s *Server) getRedisServerIDsForAgent(agentID string) ([]string, error) {
	key := "agentIDToRedisServerIDs:" + agentID
	return s.rdb.SMembers(s.ctx, key).Result()
}

// Get serverID for  redisServerIDs associated with an agentID
func (s *Server) getAgentIDForRedisServerID(redisServerID string) (string, error) {
	key := "redisServerIDToAgentID:" + redisServerID
	return s.rdb.Get(s.ctx, key).Result()
}

// Handle retrieval of servers associated with an accountID
func (s *Server) handleGetServers(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("accountID")
	if accountID == "" {
		http.Error(w, "Missing accountID parameter", http.StatusBadRequest)
		return
	}

	// Get the set of UUIDs associated with the accountID
	uuids, err := s.rdb.SMembers(s.ctx, "accountServers:"+accountID).Result()
	if err != nil {
		log.Println("Error getting servers for accountID:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Prepare a slice to hold server info
	servers := []map[string]string{}

	// For each UUID, retrieve the serverInfo JSON object
	for _, uuid := range uuids {
		// Get the JSON data from Redis
		serverInfoData, err := s.readServerInfo(uuid)

		if err != nil {
			log.Println("Error getting server info for UUID:", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Append server info to the list
		servers = append(servers, map[string]string{
			"uuid": serverInfoData.RedisServerID,
		})
	}

	response := map[string][]map[string]string{
		"servers": servers,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Handle retrieval of servers associated with a databaseID
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	redisServerId := r.URL.Query().Get("id")
	if redisServerId == "" {
		http.Error(w, "Missing redisServerId parameter", http.StatusBadRequest)
		return
	}

	// return the server info for the redisServerId
	serverInfo, err := s.readServerInfo(redisServerId)
	if err != nil {
		log.Printf("Error retrieving server info for redisServerId %s: %v", redisServerId, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Construct the stream key
	streamKey := fmt.Sprintf("redis_server_id:%s:heartbeat_stream", redisServerId)

	// Read the latest entry from the stream using XRevRangeN
	streamEntries, err := s.rdb.XRevRangeN(s.ctx, streamKey, "+", "-", 1).Result()
	if err != nil {
		log.Printf("Error reading from stream for redisServerId %s: %v", redisServerId, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Check if there are any entries
	if len(streamEntries) == 0 {
		http.Error(w, "No entries in the stream", http.StatusNotFound)
		return
	}

	// Get the latest entry
	entry := streamEntries[0]

	// Prepare the combined response
	response := map[string]interface{}{
		"stats":      entry.Values,
		"serverInfo": serverInfo,
	}

	// Set the content type and write the JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}

}

// handleAgentConnection handles WebSocket connections from agents
func (s *Server) handleAgentConnection(w http.ResponseWriter, r *http.Request) {
	log.Println("Received a new agent connection request")
	// Upgrade HTTP to WebSocket
	wsConn, err := s.agentUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		http.Error(w, fmt.Sprintf("Failed to upgrade to WebSocket: %v", err), http.StatusBadRequest)
		return
	}

	// Read the agent ID sent by the agent
	log.Println("Waiting to receive agent ID")
	_, agentIDBytes, err := wsConn.ReadMessage()
	if err != nil {
		log.Printf("Failed to read agent ID: %v", err)
		http.Error(w, fmt.Sprintf("Failed to read agent ID: %v", err), http.StatusBadRequest)
		wsConn.Close()
		return
	}
	agentID := string(agentIDBytes)
	log.Printf("Agent connected: %s", agentID)

	agent := &Agent{
		conn:       wsConn,
		clients:    make(map[uint64]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		messages:   make(chan Message),
		done:       make(chan struct{}),
		pingPeriod: 54 * time.Second,
		pongWait:   60 * time.Second,
		send:       make(chan Message, 100), // Make agent.send a buffered channel
		agentID:    agentID,
		server:     s,
		connected:  true, // Set connected to true
	}

	// Store the agent
	s.StoreAgent(agentID, agent)

	// Retrieve redisServerIDs associated with this agentID from Redis
	redisServerIDs, err := s.getRedisServerIDsForAgent(agentID)
	if err != nil {
		log.Printf("Error retrieving redisServerIDs for agentID %s: %v", agentID, err)
	}

	log.Printf("redisServerIDs for agentID %s: %v", agentID, redisServerIDs)

	// Start agent's run loop
	go agent.run()

	// Start read and write pumps
	go agent.readPump()
	go agent.writePump()

	// Set up ping/pong handlers to detect disconnections
	agent.setupPingPong()

	// Optionally, send periodic pings
	go agent.startPing()

	go func() {
		<-agent.done
		// Handle agent disconnection
		s.handleAgentDisconnect(agentID)
	}()
}

// handleAgentDisconnect updates the status of all associated Redis servers to "unreachable" when an agent disconnects.
func (s *Server) handleAgentDisconnect(agentID string) {
	log.Printf("Agent %s disconnected", agentID)

	// Remove the agent from the server's agents map
	s.RemoveAgent(agentID)

	// Get the list of redis_server_ids associated with the agent
	redisServerIDs, err := s.getRedisServerIDsForAgent(agentID)
	if err != nil {
		log.Printf("Error getting redis_server_ids for agent %s: %v", agentID, err)
		return
	}

	for _, redisServerID := range redisServerIDs {
		// Update the status of the redis_server to "unreachable" in Redis
		serverInfoKey := fmt.Sprintf("redis_server_id:%s", redisServerID)
		statusString := fmt.Sprintf("\"%s\"", "AGENT UNREACHABLE")
		_, err := s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.status", statusString).Result()
		if err != nil {
			log.Printf("Error setting status to 'unreachable' for redis_server_id %s: %v", redisServerID, err)
			continue
		}

		// Update the timestamp
		_, err = s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.timestamp", time.Now().Unix()).Result()
		if err != nil {
			log.Printf("Error setting timestamp for redis_server_id %s: %v", redisServerID, err)
			continue
		}
		log.Printf("Set status of redis_server_id %s to 'unreachable'", redisServerID)
	}
}

func (s *Server) removeRedisServerIDFromAgent(agentID, redisServerID string) error {
	// Remove the redisServerID from the agent's set
	err := s.rdb.SRem(s.ctx, "agentIDToRedisServerIDs:"+agentID, redisServerID).Err()
	if err != nil {
		return fmt.Errorf("error removing redisServerID from agentIDToRedisServerIDs: %v", err)
	}

	// Remove the reverse mapping
	err = s.rdb.Del(s.ctx, "redisServerIDToAgentID:"+redisServerID).Err()
	if err != nil {
		return fmt.Errorf("error deleting redisServerIDToAgentID mapping: %v", err)
	}

	return nil
}
