package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

func validConfig() Config {
	return Config{
		Host:     "localhost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Publication: publication.Config{
			Name:              "pub",
			CreateIfNotExists: true,
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationUpdate,
				publication.OperationDelete,
			},
			Tables: publication.Tables{{
				Name:            "events",
				Schema:          "public",
				ReplicaIdentity: publication.ReplicaIdentityDefault,
			}},
		},
		Slot: slot.Config{
			Name:                        "slot",
			CreateIfNotExists:           true,
			SlotActivityCheckerInterval: time.Second,
		},
	}
}

func TestSetDefaultOwnsRuntimeDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.Port = 0
	cfg.Metric.Port = 0
	cfg.Publication.Tables[0].Schema = ""
	cfg.Slot.ProtoVersion = 0
	cfg.Slot.SlotActivityCheckerInterval = 0

	cfg.SetDefault()

	if cfg.Port != 5432 || cfg.Metric.Port != 8080 {
		t.Fatalf("ports = %d/%d", cfg.Port, cfg.Metric.Port)
	}
	if cfg.Publication.Tables[0].Schema != "public" {
		t.Fatalf("table schema = %q", cfg.Publication.Tables[0].Schema)
	}
	if cfg.Slot.ProtoVersion != 1 || cfg.Slot.SlotActivityCheckerInterval != time.Second {
		t.Fatalf("slot defaults = proto %d interval %s", cfg.Slot.ProtoVersion, cfg.Slot.SlotActivityCheckerInterval)
	}
}

func TestSetDefaultOwnsPublicationInput(t *testing.T) {
	columns := []string{"id", "name"}
	tables := publication.Tables{{
		Name:            "events",
		Columns:         columns,
		ReplicaIdentity: publication.ReplicaIdentityDefault,
	}}
	operations := publication.Operations{publication.OperationInsert}
	cfg := validConfig()
	cfg.Publication.Tables = tables
	cfg.Publication.Operations = operations

	cfg.SetDefault()
	cfg.Publication.Tables[0].Columns[0] = "changed"
	cfg.Publication.Operations[0] = publication.OperationDelete

	if columns[0] != "id" || tables[0].Schema != "" || tables[0].ColumnsSpecified {
		t.Fatalf("SetDefault mutated caller-owned tables: %#v", tables)
	}
	if operations[0] != publication.OperationInsert {
		t.Fatalf("SetDefault aliased caller-owned operations: %v", operations)
	}
}

func TestSetDefaultPreservesPublicationColumnIntent(t *testing.T) {
	cfg := validConfig()
	cfg.Publication.Tables = publication.Tables{
		{
			Name:            "events",
			Columns:         []string{"id", "name"},
			ReplicaIdentity: publication.ReplicaIdentityDefault,
		},
		{
			Name:            "event_parts",
			Partitioned:     true,
			ReplicaIdentity: publication.ReplicaIdentityDefault,
		},
	}

	cfg.SetDefault()

	if !cfg.Publication.Tables[0].ColumnsSpecified {
		t.Fatal("configured column list was not marked explicit")
	}
	if cfg.Publication.Tables[1].ColumnsSpecified {
		t.Fatal("unrestricted table was marked explicit")
	}
	if !cfg.Publication.PublishViaPartitionRoot {
		t.Fatal("partition-root publication intent was not normalized")
	}
}

func TestValidateHeartbeatInPublication(t *testing.T) {
	cfg := validConfig()
	cfg.Heartbeat = HeartbeatConfig{
		Table:    publication.Table{Name: "heartbeat_events", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
		Interval: time.Second,
	}
	cfg.Publication.Tables = append(cfg.Publication.Tables, cfg.Heartbeat.Table)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	withoutUpdate := cfg
	withoutUpdate.Publication.Operations = publication.Operations{publication.OperationInsert}
	if err := withoutUpdate.Validate(); err == nil {
		t.Fatal("heartbeat publication without UPDATE was accepted")
	}

	actual := &publication.Config{Name: "pub", Tables: publication.Tables{{Name: "events", Schema: "public"}}}
	if err := cfg.ValidateHeartbeatInPublication(actual); err == nil {
		t.Fatal("missing heartbeat publication relation was accepted")
	}
	actual.Tables = append(actual.Tables, publication.Table{Name: "heartbeat_events", Schema: "public"})
	if err := cfg.ValidateHeartbeatInPublication(actual); err != nil {
		t.Fatal(err)
	}
}

func TestConfigReadersRejectRemovedConfiguration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"snapshot.json", `{"snapshot":{"enabled":true}}`, "snapshot"},
		{"snapshot.yaml", "snapshot:\n  enabled: true\n", "snapshot"},
		{"timescale.json", `{"extensionSupport":{"enableTimeScaleDB":true}}`, "extensionSupport"},
		{"timescale.yaml", "extensionSupport:\n  enableTimeScaleDB: true\n", "extensionSupport"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), test.name)
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			var err error
			if strings.HasSuffix(test.name, ".json") {
				_, err = ReadConfigJSON(path)
			} else {
				_, err = ReadConfigYAML(path)
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("reader error = %v, want removed field %q", err, test.want)
			}
		})
	}
}
