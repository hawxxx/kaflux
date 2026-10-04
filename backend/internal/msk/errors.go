package msk

// BlockedError means no Kafka submission was attempted, so the result is not ambiguous.
type BlockedError struct{ Reason string }

func (e *BlockedError) Error() string { return e.Reason }
