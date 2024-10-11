package main

// Define the possible client disconnect methods
type ClientDisconnectMethod string

const (
    DISCONNECT_AUTO    ClientDisconnectMethod = "AUTO"
    DISCONNECT_TIMEOUT ClientDisconnectMethod = "TIMEOUT"
    DISCONNECT_NONE    ClientDisconnectMethod = "NONE"
)

type ActivationState string

const (
    ActivationStatePending     ActivationState = "ACTIVATION_PENDING"
    ActivationStateActivated   ActivationState = "ACTIVATED"
    ActivationStateDeactivated ActivationState = "DEACTIVATED"
)