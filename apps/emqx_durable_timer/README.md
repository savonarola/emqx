# EMQX Durable Timer

This application implements functionality similar to `timer:apply_after`, but timers survive restart of the node that started them.

An event that has been scheduled will be eventually executed, unless canceled.

# Features

- Timers have millisecond precision.

- Upgrade safety.
  Events are handled by the nodes that have the capacity to do so.

- "Dead Hand" activation: timers can be set up to fire after sudden stop of the node that created them.

# Limitation

- This application only guarantees that the events are executed _later_ than the specified timeout.
  There are no guarantees about the earliest time of execution.

- These timers aren't lightweight.
  Operations with the timers involve `emqx_durable_storage` transactions that cause disk I/O and network replication.

- Code may be executed on an arbitrary node.
  There are no guarantees that timer will fire on the same node that created it.
  However, canceling or overwriting the timer should be done on the node that create it.

- There might be duplication of events

## TODO

- Improve node down detection.

# Documentation links

# Usage

## Configuration

This application is configured via application environment variables.

Warning: heartbeat and health configuration must be consistent across the cluster.

- `heartbeat_interval`: Node heartbeat interval in milliseconds.
  Default: 5000 ms.
- `missed_heartbeats`: If remote node fails to update the heartbeat withing this period of time (in ms), it's considered down.
  Default: 30_000 ms.
- `replay_retry_interval`: Timers are executed by replaying a DS topic.
  When fetching batches from DS fails, last batch will be retried after this interval.

  Note: failures in the timers' handle timeout callback are NOT retried.


# HTTP APIs

None

# Other

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
