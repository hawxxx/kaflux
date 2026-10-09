package jobs

import (
	"context"
	"fmt"
	"time"
)

// humanBytes renders sizes the way the console shows them: 3.1 GiB, 512 MiB.
func humanBytes(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	v := float64(n)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f %s", v, units[i])
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

func humanRate(n int64) string { return humanBytes(n) + "/s" }

// humanDuration renders 5m12s, 1h12m or 40s.
func humanDuration(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}

// Event levels of the activity log.
const (
	levelInfo  = "info"
	levelWarn  = "warn"
	levelError = "error"
)

// log appends to the job's activity log. The log is best effort: a failed
// write never changes what the worker does to Kafka.
func (w *Worker) log(ctx context.Context, jobID, level, format string, args ...any) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = w.Store.AppendEvent(bounded, jobID, level, fmt.Sprintf(format, args...))
}
