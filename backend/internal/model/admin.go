package model

import "time"

type TopicCreate struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Config            map[string]string `json:"config"`
}

// ConfigEntry is one topic configuration key as the broker describes it.
// Override is true when the value is set on the topic itself rather than
// inherited from broker or Kafka defaults.
type ConfigEntry struct {
	Name      string  `json:"name"`
	Value     *string `json:"value"`
	Source    string  `json:"source"`
	Override  bool    `json:"override"`
	Sensitive bool    `json:"sensitive"`
}

// ConfigValues returns the readable key/value pairs of entries.
func ConfigValues(entries []ConfigEntry) map[string]string {
	out := map[string]string{}
	for _, e := range entries {
		if !e.Sensitive && e.Value != nil {
			out[e.Name] = *e.Value
		}
	}
	return out
}

type GroupOffset struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committedOffset"`
	StartOffset     int64  `json:"startOffset"`
	EndOffset       int64  `json:"endOffset"`
	Lag             int64  `json:"lag"`
}
type GroupDetail struct {
	Group
	Offsets []GroupOffset `json:"offsets"`
}
type OffsetReset struct {
	Mode         string    `json:"mode"`
	Topics       []string  `json:"topics"`
	Timestamp    time.Time `json:"timestamp"`
	Offset       int64     `json:"offset"`
	Shift        int64     `json:"shift"`
	Preview      bool      `json:"preview"`
	Confirmation string    `json:"confirmation"`
	PreviewHash  string    `json:"previewHash"`
}
type OffsetChange struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Before    int64  `json:"before"`
	After     int64  `json:"after"`
}
type OffsetPreview struct {
	GroupID     string         `json:"groupId"`
	Changes     []OffsetChange `json:"changes"`
	PreviewHash string         `json:"previewHash"`
	Applied     bool           `json:"applied"`
}
