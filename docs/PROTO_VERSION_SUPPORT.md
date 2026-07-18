# Protocol Version Support and Streaming Transactions

## Overview

This document explains the changes introduced on `feature/protoversion1-support` compared with `main`.

Main improvements:

- Added configurable `slot.protoVersion` (`1` or `2`)
- Defaulted protocol version to `2` when not set
- Updated replication startup arguments according to protocol version
- Added full decoding for `STREAM START` and `STREAM ABORT` payloads
- Fixed streamed transaction buffering so rolled-back streamed data is never emitted
- Expanded integration tests to validate both protocol versions and streaming rollback scenarios

## What Changed vs `main`

### 1) Configurable protocol version

`slot.Config` now includes:

- `protoVersion: 1` for compatibility mode
- `protoVersion: 2` for streaming-aware mode (default)

Validation now rejects unsupported values.

### 2) Replication start behavior is version-aware

Replication startup now uses `slot.protoVersion`:

- Always sends `proto_version '<N>'`
- Sends `streaming 'true'` only when `protoVersion >= 2`
- Does not request logical `messages`, which are not part of the listener contract

Both protocol modes target the repository's PostgreSQL 16+ support floor.

### 3) Stream message decoding is complete

`STREAM START` and `STREAM ABORT` messages are now parsed into typed structures:

- `StreamStart { Xid, FirstSegment }`
- `StreamAbort { Xid, SubXid }`

This allows transaction-level stream control using real XIDs.

### 4) Streamed transaction handling is now commit-safe

For `protoVersion: 2`, streamed transactions can be split and interleaved by PostgreSQL.
The stream sink now buffers messages per transaction XID and applies this policy:

- On `STREAM START`: begin/continue buffering for XID
- On `STREAM STOP`: keep buffered data, emit nothing
- On `STREAM COMMIT`: emit buffered data for XID, set last event WAL to commit end LSN
- On `STREAM ABORT`: discard buffered data for XID

Result: rolled-back streamed transactions are never delivered to handlers.

## Choosing `protoVersion`

### Use `protoVersion: 1` when:

- You do not want streamed transaction protocol messages
- You do not require streamed in-progress transaction handling

### Use `protoVersion: 2` when:

- PostgreSQL version is 16+
- You want support for large/streamed in-progress transactions
- You want strict rollback safety for streamed transactions

## Configuration Examples

### YAML

```yaml
slot:
  name: cdc_slot
  createIfNotExists: true
  slotActivityCheckerInterval: 3000
  protoVersion: 2
```

### Go

```go
Slot: slot.Config{
    Name:                        "cdc_slot",
    CreateIfNotExists:           true,
    SlotActivityCheckerInterval: 3000,
    ProtoVersion:                2,
},
```

## Compatibility Matrix

| slot.protoVersion | Minimum PostgreSQL | Streaming protocol messages |
|-------------------|--------------------|-----------------------------|
| 1                 | 16                 | No                          |
| 2                 | 16                 | Yes                         |

## Test Coverage Added

The branch adds/updates tests to protect behavior:

- Unit tests for stream payload parsing:
  - `NewStreamStart`
  - `NewStreamAbort`
- Integration tests for streaming behavior:
  - Large streamed commit delivery
  - Interleaved streamed transactions
  - Streamed rollback emits no events
  - Rollback followed by commit emits only committed events
- Integration test helpers now run protocol-version subtests (`v1` and `v2`) where applicable

## Migration Notes

If you do not set `slot.protoVersion`, default is `2`.

Recommended migration path:

1. Upgrade PostgreSQL to version 16 or newer before using this fork.
2. Keep `protoVersion: 2` unless you intentionally disable streamed transaction handling.
3. Run CDC integration tests with the protocol version used in production.
