package main

import (
    "encoding/json"
    "fmt"
    "log"
)

type RedisServerInfo struct {
    RedisServerID   string `json:"redis_server_id"`
    AccountID       string `json:"account_id"`
    AgentID         string `json:"agent_id"`
    Status          string `json:"status"`
    Timestamp       int64  `json:"timestamp"`
    ActivationState string `json:"activation_state"`
}

// readServerInfo reads the server info from Redis for a given redisServerID
func (s *Server) readServerInfo(redisServerID string) (RedisServerInfo, error) {
    serverInfoData, err := s.rdb.Do(s.ctx, "JSON.GET", "redis_server_id:"+redisServerID).Result()
    if err != nil {
        return RedisServerInfo{}, fmt.Errorf("error getting server info for redisServerID: %s, error: %v", redisServerID, err)
    }

    // Convert the result to a string or byte slice
    var serverInfo RedisServerInfo
    switch data := serverInfoData.(type) {
    case string:
        err = json.Unmarshal([]byte(data), &serverInfo)
    case []byte:
        err = json.Unmarshal(data, &serverInfo)
    default:
        return RedisServerInfo{}, fmt.Errorf("unexpected data type for server info: %T", serverInfoData)
    }

    if err != nil {
        return RedisServerInfo{}, fmt.Errorf("error unmarshaling server info for redisServerID: %s, error: %v", redisServerID, err)
    }

    return serverInfo, nil
}

// writeServerInfo writes the server info to Redis for a given redisServerID
func (s *Server) writeServerInfo(redisServerID string, serverInfo RedisServerInfo) error {
    // Store the redisServerInfo as a JSON object in Redis
    redisServerInfoJSON, err := json.Marshal(serverInfo)
    if err != nil {
        log.Println("Error marshaling server info:", err)
        return err
    }

    // Use JSON.SET command to store the JSON object
    _, err = s.rdb.Do(s.ctx, "JSON.SET", "redis_server_id:"+redisServerID, ".", redisServerInfoJSON).Result()
    if err != nil {
        log.Println("Error setting server info in Redis:", err)
        return err
    }
    return nil
}

// Other functions related to Redis server info go here...