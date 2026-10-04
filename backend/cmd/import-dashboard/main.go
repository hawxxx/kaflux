// import-dashboard normalizes a Grafana dashboard without retaining datasource identifiers.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hawxxx/kaflux/backend/internal/metrics"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: import-dashboard dashboard.json")
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open dashboard")
		os.Exit(1)
	}
	defer file.Close()
	definitions, err := metrics.ImportDashboard(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(definitions); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write definitions")
		os.Exit(1)
	}
}
