package config

import (
	"os"
	"path/filepath"
	"testing"
)

func loadYAML(t *testing.T, body string) (Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

const capacityHead = "runtime:\n  demo: true\n  storageBackend: memory\nclusters:\n  - id: brokers\n    seeds: [kafka:9092]\n    allowPlaintext: true\n"

func TestClusterCapacityIsLoaded(t *testing.T) {
	c, err := loadYAML(t, capacityHead+"    capacity:\n      label: r6i.4xlarge\n      partitionsPerBroker: {recommended: 4000, maximum: 6000}\n      diskBytesPerBroker: 2199023255552\n")
	if err != nil {
		t.Fatal(err)
	}
	got := c.Clusters[0].Capacity
	if got == nil || got.Label != "r6i.4xlarge" || got.PartitionsPerBroker.Recommended != 4000 || got.PartitionsPerBroker.Maximum != 6000 || got.DiskBytesPerBroker != 2199023255552 {
		t.Fatalf("capacity = %+v", got)
	}
}

func TestClusterWithoutCapacityIsStillValid(t *testing.T) {
	c, err := loadYAML(t, capacityHead)
	if err != nil || c.Clusters[0].Capacity != nil {
		t.Fatalf("err = %v capacity = %+v", err, c.Clusters[0].Capacity)
	}
}

func TestInvalidClusterCapacityIsRejectedAtStartup(t *testing.T) {
	for name, body := range map[string]string{
		"recommended over maximum": "    capacity:\n      partitionsPerBroker: {recommended: 7000, maximum: 6000}\n",
		"negative disk":            "    capacity:\n      diskBytesPerBroker: -1\n",
		"empty block":              "    capacity:\n      label: nothing to compare\n",
		"misspelled key":           "    capacity:\n      partitionsPerBrokers: {recommended: 10}\n",
	} {
		if _, err := loadYAML(t, capacityHead+body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
