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
  "tls": null,
  "auth": {
    "enabled": false,
    "clients": []
  }
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
- `auth`: optional Ed25519 challenge-response authentication config.

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

Authentication is disabled when `auth.enabled` is `false`. To enable
challenge-response authentication, configure a list of allowed client IDs and
Ed25519 public keys:

```json
{
  "auth": {
    "enabled": true,
    "clients": [
      {
        "client_id": "client1",
        "public_key": "64-hex-character-ed25519-public-key"
      }
    ]
  }
}
```

When authentication is enabled, every new incoming connection receives a freshly
generated server nonce in the `INFO` frame. The client signs that nonce and sends
the configured client ID plus hex-encoded signature in `CONNECT`. After
successful verification, the authenticated client ID becomes the durable
identity for that connection.

## Run

Build and run the broker from the repository root:

```bash
cargo run --release -p server -- broker.json
```

If no config path is provided, the broker reads `broker.json` from the current
directory:

```bash
cargo run --release -p server
```

The WAL directory is created automatically. Stop the broker with `Ctrl-C`; it
flushes the WAL before exiting.

## Workspace Layout

- `crates/server`: the broker runtime, WAL, TLS, and JSON configuration.
- `crates/protocol`: shared wire protocol parsing/encoding, subject matching,
  and challenge-response helpers.
- `crates/client`: client crate scaffold for broker client tooling.

## Minimal Session

Open a TCP connection to the broker. The server immediately sends an `INFO`
line. With authentication disabled, connect with a durable identity:

```text
CONNECT {"durable_id":"client1","verbose":true,"ack_timeout_ms":30000,"max_in_flight":1024}
SUB orders.* sid1
```

With authentication enabled, sign the `INFO` nonce with the configured private
key and connect with the client ID plus signature:

```text
CONNECT {"client_id":"client1","signature":"128-hex-character-ed25519-signature","verbose":true}
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

- `CONNECT` must establish a durable identity before `SUB`.
- With authentication disabled, provide `durable_id`.
- With authentication enabled, provide `client_id` and `signature`. If
  `durable_id` is also provided, it must match the authenticated `client_id`.
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
  "client_id": "client1",
  "signature": "128-hex-character-ed25519-signature",
  "ack_timeout_ms": 30000,
  "max_in_flight": 1024
}
```

`durable_id`, `client_id`, subscription sids, and queue names must be non-empty
and must not contain `.`, whitespace, or start with `_`.

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
cargo test --workspace
cargo build --release --workspace
```
