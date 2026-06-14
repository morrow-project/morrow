# Broker

A single-node, WAL-backed broker with a NATS-style text protocol. Durable
consumers are the core primitive: clients must declare a durable identity before
subscribing, delivered messages require explicit acks, and unacked messages are
redelivered after their ack timeout.

## Configuration

Runtime configuration is read from a JSON file. Start from the example:

```bash
cp broker.json.example broker.json
```

```json
{
  "listen": "127.0.0.1:4222",
  "wal_dir": "./broker-wal",
  "fsync_interval_ms": 5,
  "max_payload": 1048576,
  "verbose": false,
  "tls": null
}
```

Fields:

- `listen`: TCP socket address for client connections.
- `wal_dir`: directory for the broker WAL.
- `fsync_interval_ms`: maximum batching interval before fsync.
- `max_payload`: maximum accepted `PUB` payload size in bytes.
- `verbose`: enables `+OK` responses for connections unless overridden by
  `CONNECT`.
- `tls`: optional TLS-first listener config.

If a field is omitted, the value shown above is used.

TLS is disabled when `tls` is `null` or omitted. To enable TLS-first client
connections:

```json
{
  "listen": "127.0.0.1:4222",
  "wal_dir": "./broker-wal",
  "tls": {
    "cert_file": "./server-cert.pem",
    "key_file": "./server-key.pem",
    "handshake_timeout_ms": 2000
  }
}
```

When TLS is enabled, the TLS handshake happens before the broker sends `INFO`.
Plain `telnet` will not work against a TLS listener. For manual testing, use:

```bash
openssl s_client -connect 127.0.0.1:4222
```

## Run

Build and run the broker from the repository root:

```bash
cargo run --release -- broker.json
```

If no config path is provided, the broker reads `broker.json` from the current
directory:

```bash
cargo run --release
```

The WAL directory is created automatically. Stop the broker with `Ctrl-C`; it
flushes the WAL before exiting.

## Minimal Session

Open a TCP connection to the broker. The server immediately sends an `INFO`
line. Then connect with a durable identity:

```text
CONNECT {"durable_id":"client1","verbose":true,"ack_timeout_ms":30000,"max_in_flight":1024}
SUB orders.* sid1
```

Publish from any connected client:

```text
PUB orders.created 5
hello
```

The subscriber receives a message with an ack reply subject:

```text
MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1 5
hello
```

Ack by publishing to that reply subject. The ack payload may be empty:

```text
PUB _BROKER.ACK.durable-client1-sid1.1.1 0

```

If the ack is not received before `ack_timeout_ms`, the broker redelivers the
message to an active member of the same durable consumer.

## Durable Consumers

- `CONNECT` must include `durable_id` before `SUB`.
- Non-queue subscriptions create consumers keyed by durable id and sid.
- Queue subscriptions create a shared durable queue consumer keyed by queue
  group and subject.
- Messages are retained only when they match at least one existing durable
  consumer.
- Messages published before a matching durable consumer exists are not replayed
  to that later consumer.
- Ack subjects are reserved under `_BROKER.ACK.*`; publishing to other
  `_BROKER.*` subjects is rejected.

## Supported Protocol Commands

The protocol uses CRLF-delimited NATS-style frames.

### Server Frames

```text
INFO {...}
MSG <subject> <sid> <size>
MSG <subject> <sid> <reply-to> <size>
PONG
+OK
-ERR '<message>'
```

`+OK` is sent only when verbose mode is enabled for the connection.

### Client Commands

#### CONNECT

```text
CONNECT <json>
```

Supported JSON fields:

```json
{
  "verbose": true,
  "durable_id": "client1",
  "ack_timeout_ms": 30000,
  "max_in_flight": 1024
}
```

`durable_id`, subscription sids, and queue names must be non-empty and must not
contain `.`, whitespace, or start with `_`.

#### PING

```text
PING
```

The broker replies with `PONG`.

#### PONG

```text
PONG
```

Accepted and ignored.

#### PUB

```text
PUB <subject> <size>
<payload>
```

```text
PUB <subject> <reply-to> <size>
<payload>
```

Publish subjects must be concrete subjects, not wildcard subscriptions. Payload
size must be less than or equal to `max_payload` from the config file.

Publishing to `_BROKER.ACK.<consumer-id>.<seq>.<delivery-id>` records an ack for
that durable delivery.

#### SUB

```text
SUB <subject> <sid>
```

```text
SUB <subject> <queue> <sid>
```

Subjects support NATS-style subscription wildcards:

- `*` matches one token.
- `>` matches the remaining tail.

`SUB` requires a prior `CONNECT` with `durable_id`.

#### UNSUB

```text
UNSUB <sid>
```

```text
UNSUB <sid> <max-messages>
```

The broker detaches the current connection from the durable subscription member
identified by `sid`. Durable consumer state remains in the WAL.

## Development Checks

```bash
cargo fmt --check
cargo test
cargo build --release
```
