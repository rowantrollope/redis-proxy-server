package main

import (
    "encoding/binary"
    "io"
)

type Message struct {
    ClientID    uint64
    Data        []byte
    MessageType string // "control" or "data" (optional)
}

func readMessage(reader io.Reader) (Message, error) {
    var msg Message
    // Read ClientID (8 bytes)
    err := binary.Read(reader, binary.BigEndian, &msg.ClientID)
    if err != nil {
        return msg, err
    }
    // Read MessageLength (4 bytes)
    var length uint32
    err = binary.Read(reader, binary.BigEndian, &length)
    if err != nil {
        return msg, err
    }
    // Read MessageData
    msg.Data = make([]byte, length)
    _, err = io.ReadFull(reader, msg.Data)
    if err != nil {
        return msg, err
    }
    return msg, nil
}
