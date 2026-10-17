// Package buildinfo carries the version stamped into the binary at build time.
package buildinfo

import "strings"

// Version is set with -ldflags "-X .../buildinfo.Version=1.2.3". Unstamped builds report "dev".
var Version = "dev"

// Current returns the build version without a leading "v".
func Current() string {
	if v := strings.TrimPrefix(strings.TrimSpace(Version), "v"); v != "" {
		return v
	}
	return "dev"
}
