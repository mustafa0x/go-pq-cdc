# go-pq-cdc

`go-pq-cdc` is a small PostgreSQL logical-replication library for change data capture. It consumes `pgoutput`, validates every relation against an immutable capture plan, and delivers decoded changes to a caller-owned listener.

The top-level connector is **CDC-only**. It does not own initial-snapshot jobs, resnapshot requests, chunks, workers, or downstream projection state. Consumers that need bootstrap can use the lower-level `pq/capture` package to create a slot-exported snapshot, import it, scan the same compiled `CapturePlan`, and then start the logical stream.

PostgreSQL 16 or newer is required by this fork.

## Installation

```sh
go get github.com/Trendyol/go-pq-cdc
```

## Example

```go
package main

import (
    "context"
    "errors"
    "log/slog"

    cdc "github.com/Trendyol/go-pq-cdc"
    "github.com/Trendyol/go-pq-cdc/config"
    "github.com/Trendyol/go-pq-cdc/pq/message/format"
    "github.com/Trendyol/go-pq-cdc/pq/publication"
    "github.com/Trendyol/go-pq-cdc/pq/replication"
    "github.com/Trendyol/go-pq-cdc/pq/slot"
)

func main() {
    ctx := context.Background()
    connector, err := cdc.NewConnector(ctx, config.Config{
        Host:     "127.0.0.1",
        Port:     5432,
        Username: "cdc_user",
        Password: "cdc_pass",
        Database: "cdc_db",
        Publication: publication.Config{
            Name:              "cdc_publication",
            CreateIfNotExists: true,
            Operations: publication.Operations{
                publication.OperationInsert,
                publication.OperationUpdate,
                publication.OperationDelete,
                publication.OperationTruncate,
            },
            Tables: publication.Tables{{
                Schema:          "public",
                Name:            "users",
                ReplicaIdentity: publication.ReplicaIdentityDefault,
            }},
        },
        Slot: slot.Config{
            Name:              "cdc_slot",
            CreateIfNotExists: true,
            ProtoVersion:      1,
        },
    }, listen)
    if err != nil {
        panic(err)
    }

    if err := connector.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
        panic(err)
    }
}

func listen(ctx *replication.ListenerContext) {
    switch msg := ctx.Message.(type) {
    case *format.Insert:
        slog.Info("insert", "row", msg.Decoded)
    case *format.Update:
        slog.Info("update", "old", msg.OldDecoded, "new", msg.NewDecoded)
    case *format.Delete:
        slog.Info("delete", "row", msg.OldDecoded)
    case *format.Truncate:
        slog.Info("truncate", "relations", msg.RelationOIDs)
    }

    if err := ctx.Ack(); err != nil {
        slog.Error("ack", "error", err)
    }
}
```

A connector instance is one-shot. Create a new instance to restart it.

## Transaction-aware delivery

Set:

```go
Listener: config.ListenerConfig{EmitTransactionBoundaries: true}
```

The listener receives `Begin`, relation/DML messages, and `Commit` in source order. Only `Commit` acknowledgements advance the confirmed LSN. A consumer must commit its downstream transaction before calling `Ack()` on `Commit`.

For protocol version 2, in-progress transaction fragments are buffered by XID until PostgreSQL sends `StreamCommit`, then exposed through the same ordinary `Begin`/`Commit` boundary contract. `StreamAbort` discards them. See [Transaction-Aware Listener API](docs/TRANSACTION_AWARE_LISTENER_API.md).

## Low-level bootstrap

Bootstrap ownership belongs to the consumer. The low-level flow is:

```text
create logical slot with exported snapshot
→ import snapshot in REPEATABLE READ READ ONLY
→ compile CapturePlan inside the imported snapshot
→ scan the CapturePlan sequentially in bounded batches
→ commit downstream bootstrap state
→ start replication from the returned consistent point
```

The relevant API lives in `pq/capture`:

```text
capture.Compile
capture.CreateSlotSnapshot
capture.ImportSnapshot
capture.Compile
(*capture.Plan).Scan
capture.FinishSnapshot / capture.AbortSnapshot
capture.DropSlot
```

The same `CapturePlan` owns snapshot column order, PostgreSQL text normalization, `pgoutput` Relation validation, tuple decoding, and row-key metadata. The top-level connector intentionally does not expose a bootstrap mode or recovery state machine.

## Configuration

Configuration can be constructed in Go or read from strict JSON/YAML. Unknown fields—including the removed `snapshot` surface—are rejected.

| Field | Required | Default | Meaning |
|---|---:|---:|---|
| `host` | yes | — | PostgreSQL host |
| `port` | no | `5432` | PostgreSQL port |
| `username` | yes | — | PostgreSQL role |
| `password` | yes | — | PostgreSQL password |
| `database` | yes | — | Database name |
| `publication.name` | yes | — | Managed or adopted publication |
| `publication.createIfNotExists` | no | `false` | Create the publication when absent |
| `publication.operations` | yes | — | Exact `insert`, `update`, `delete`, `truncate` contract |
| `publication.tables` | yes | — | Exact static relation list, also required when adopting |
| `publication.tables[].columns` | no | all | Published columns |
| `publication.tables[].replicaIdentity` | yes | — | `DEFAULT`, `FULL`, `NOTHING`, or `USING INDEX` |
| `publication.tables[].replicaIdentityIndex` | with `USING INDEX` | — | Replica-identity index name |
| `publication.tables[].partitioned` | no | `false` | Publish partitioned changes through the root |
| `slot.name` | yes | — | Logical slot name |
| `slot.createIfNotExists` | no | `false` | Create the slot when absent |
| `slot.protoVersion` | no | `1` | `1` or `2` |
| `slot.messages` | no | `false` | Request logical messages |
| `slot.slotActivityCheckerInterval` | no | `1s` | Passive-owner polling interval |
| `listener.emitTransactionBoundaries` | no | `false` | Emit transaction boundaries |
| `heartbeat.table` | no | disabled | Optional pre-provisioned heartbeat relation |
| `heartbeat.interval` | with heartbeat | `100ms` | Heartbeat write interval |
| `metric.port` | no | `8080` | HTTP metric port |
| `debugMode` | no | `false` | Enable pprof endpoints |

The connector requires an explicit static relation and operation contract even when adopting an existing publication. It rejects dynamic `FOR ALL TABLES`, schema publications, row filters, generated columns, and partition-root `TRUNCATE` capture. Publication column lists and replica identity are validated exactly. Heartbeat requires `UPDATE` in the publication operations. Lower-level bootstrap consumers may impose additional source rules through their typed `capture.Spec`.

## Protocol versions

| `slot.protoVersion` | PostgreSQL | Behavior |
|---:|---:|---|
| `1` | 16+ | One transaction at a time; no streamed in-progress transactions |
| `2` | 16+ | Supports `STREAM START/STOP/COMMIT/ABORT` |

`slot.messages=true` requests PostgreSQL logical messages. Transactional messages follow their transaction's delivery boundary. In transaction-aware mode, nontransactional messages are checkpointed internally without passing an earlier unacknowledged transaction; row-oriented listeners receive them directly. See [Protocol Version Support](docs/PROTO_VERSION_SUPPORT.md).

## PostgreSQL role

A typical fixed-table deployment needs:

```sql
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'change_me';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

Additional privileges:

- `CREATE` on the database when `publication.createIfNotExists=true`.
- Ownership of an existing publication when it must be altered externally.
- `INSERT`/`UPDATE` on the pre-provisioned heartbeat relation.
- `pg_hba.conf` access for logical replication connections.

## Heartbeat

Heartbeat is optional. Its table must be created before connector startup and must belong to the selective publication so its writes reach the slot. Startup validates the relation without mutating it; the heartbeat loop creates the singleton row only after the stream is active. Heartbeat rows are filtered from listener delivery, but transaction boundaries around heartbeat transactions still obey the acknowledgement contract.

## TOAST and replica identity

For unchanged TOASTed columns, PostgreSQL may emit the unchanged marker instead of the value. The decoded update therefore omits that field unless the old tuple supplies it. Consumers that need complete rows must reconstruct them from their own durable prior state.

Replica identity determines which old-key values PostgreSQL emits for update/delete. The compiled capture plan validates relation metadata and rejects drift before delivering DML.

## Availability

Multiple connector instances may monitor one slot. Only the active PostgreSQL slot owner streams; passive instances poll slot activity and may take over after the slot becomes inactive. Consumers remain responsible for idempotent downstream projection because PostgreSQL acknowledgements are at least once across crashes.

## HTTP endpoints

| Endpoint | Description |
|---|---|
| `GET /status` | PostgreSQL connectivity status |
| `GET /metrics` | Prometheus metrics |
| `GET /debug/pprof/*` | pprof when `debugMode=true` |

## Metrics

The registry exposes operation counts, CDC/process latency, slot activity, current and confirmed LSNs, retained WAL size, and slot lag. Snapshot metrics were removed with the high-level snapshot product.

## Examples

- [Simple](example/simple)
- [File configuration](example/simple-file-config)
- [Heartbeat](example/simple-with-heartbeat)
- [Streaming transactions](example/streaming-transactions)
- [Column filtering](example/simple-column-filtering)
- [Partitioned tables](example/partitioned-table-mapping)
- [Replica identity using index](example/replica-identity-using-index)
- [Replica identity nothing](example/replica-identity-nothing)
- [PostgreSQL sink](example/postgresql)
