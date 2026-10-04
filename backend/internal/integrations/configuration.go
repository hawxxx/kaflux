package integrations

import (
	"encoding/json"
	"errors"
)

// PreserveRedacted keeps server-side secret values out of the browser while
// allowing edits to ordinary connector settings without overwriting credentials.
func PreserveRedacted(proposed, current []byte) ([]byte, error) {
	var next, old map[string]any
	if json.Unmarshal(proposed, &next) != nil || next == nil {
		return nil, errors.New("configuration must be a JSON object")
	}
	if json.Unmarshal(current, &old) != nil || old == nil {
		return nil, errors.New("current configuration is unavailable")
	}
	for key, value := range next {
		if value == "[REDACTED]" {
			original, exists := old[key]
			if !exists {
				return nil, errors.New("redacted placeholder has no existing value")
			}
			next[key] = original
		}
	}
	return json.Marshal(next)
}
