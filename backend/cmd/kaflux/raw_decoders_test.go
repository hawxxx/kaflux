package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/config"
)

func TestLoadRawDecoders(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "r.proto"), []byte(`syntax = "proto2"; package sts; message Report { optional string node_id = 1; }`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := config.Decoder{ID: "stats", Directory: dir, File: "r.proto", Message: "sts.Report", Topics: []string{"*stats*"}}
	got, err := loadRawDecoders(context.Background(), []config.Decoder{ok})
	if err != nil || len(got) != 1 || got[0].Name != "sts.Report" {
		t.Fatalf("got %+v err %v", got, err)
	}
	for name, bad := range map[string][]config.Decoder{
		"no id":           {{Directory: dir, File: "r.proto", Message: "sts.Report"}},
		"duplicate id":    {ok, ok},
		"missing dir":     {{ID: "x", Directory: filepath.Join(dir, "none"), File: "r.proto", Message: "sts.Report"}},
		"unknown message": {{ID: "x", Directory: dir, File: "r.proto", Message: "sts.Nope"}},
	} {
		if _, err := loadRawDecoders(context.Background(), bad); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
