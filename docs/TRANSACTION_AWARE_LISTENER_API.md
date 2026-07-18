# Transaction-Aware Listener API

## Purpose

Transaction-aware mode lets a consumer project each committed PostgreSQL transaction atomically, persist its commit LSN, and acknowledge replication only after that projection is durable.

The mode is opt-in. The default listener remains row-oriented.

## Configuration

```go
type ListenerConfig struct {
    EmitTransactionBoundaries bool `json:"emitTransactionBoundaries" yaml:"emitTransactionBoundaries"`
}
```

## Listener metadata

```go
type ListenerContext struct {
    Message any

    // WALStart is the WAL start position of this decoded message.
    WALStart pq.LSN

    // AckLSN is the checkpoint associated with this message. In
    // transaction-aware mode, only Commit and StreamCommit acknowledgements
    // advance confirmed_flush_lsn.
    AckLSN pq.LSN

    Ack func() error
}
```

## Delivery contract

For a regular transaction the listener receives, in WAL order:

1. `*format.Begin`
2. decoded row and metadata messages emitted by PostgreSQL
3. `*format.Commit`

For a streamed transaction, messages are buffered by XID and are not delivered until PostgreSQL emits `StreamCommit`. The buffered messages are then delivered in order, followed by `*format.StreamCommit`. `StreamAbort` discards the buffered transaction.

Rolled-back transactions do not produce a commit boundary.

Malformed or unsupported replication messages terminate the stream. They are not skipped, because skipping a row and later acknowledging its commit would make a partial projection durable.

## Acknowledgement contract

When transaction-aware mode is disabled, `Ack()` keeps the existing row-oriented behavior.

When transaction-aware mode is enabled:

- `Ack()` on `Commit` and `StreamCommit` marks that transaction checkpoint durable.
- Commit acknowledgements are ordered. A later acknowledged commit cannot advance `confirmed_flush_lsn` past an earlier unacknowledged commit.
- `Ack()` on rows and metadata does not advance the confirmed LSN. Replication feedback remains coalesced by the stream loop.
- Feedback writes are owned by the stream sink. A socket-write failure terminates the stream and is returned from `Connector.Start`; it is not reported synchronously by `Ack()`.
- `AckLSN` on a commit boundary equals the PostgreSQL transaction-end LSN.

PostgreSQL replication acknowledgements are cumulative. A consumer must stop or cancel the connector after a projection or acknowledgement failure; it must not continue projecting later commits after an earlier commit failed.

Heartbeat rows are filtered before listener delivery. In transaction-aware mode, any visible transaction boundary around a heartbeat transaction must still be acknowledged.

## Projection pattern

```go
func listener(ctx *replication.ListenerContext) {
    switch msg := ctx.Message.(type) {
    case *format.Begin:
        txBuffer.Begin(msg.Xid)

    case *format.Insert, *format.Update, *format.Delete:
        txBuffer.Append(ctx.WALStart, msg)

    case *format.Commit:
        if err := projector.Apply(txBuffer.Rows(), ctx.AckLSN); err != nil {
            cancel(err)
            return
        }
        if err := ctx.Ack(); err != nil {
            cancel(err)
        }
    }
}
```

The projection transaction must commit before `Ack()` is called.

## Lifecycle

`Connector.Start` owns connector cleanup and returns startup and runtime errors. `WaitUntilReady` is a monotonic broadcast: once readiness is reached, all current and future waiters observe it, including after shutdown.

```go
startResult := make(chan error, 1)
go func() {
    startResult <- connector.Start(ctx)
}()

readyErr := connector.WaitUntilReady(ctx)
if readyErr == nil {
    onReady()
}
if startErr := <-startResult; startErr != nil {
    return startErr
}
return readyErr
```

A connector instance is one-shot. Create a new connector to restart it.

## Compatibility

Existing row-oriented consumers do not need to enable transaction boundaries. Transactional consumers receive an explicit transaction envelope and a checkpoint contract without depending on internal buffering details.
