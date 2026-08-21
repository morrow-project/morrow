# Broker Protocol

This document describes the public TCP protocol implemented by this broker.
It is intended to be sufficient to implement a client without using the Rust
client crate.

The protocol is NATS-style and line oriented. Commands and control frames are
UTF-8 text lines terminated by `\r\n`. For compatibility, received command
lines ending in `\n` are also accepted. Frames that carry a payload include a
decimal byte length in the protocol line, followed by exactly that many payload
bytes and a trailing `\r\n`.
The server enforces a configurable maximum control-line length before reading
payload bodies.

## Connection Lifecycle

1. The client opens a TCP connection to the broker.
2. If the listener is TLS-enabled, the TLS handshake happens first.
3. The server immediately sends one `INFO` frame.
4. The client sends `CONNECT <json>`.
5. The client may then send `PING`, `PONG`, `SUB`, `UNSUB`, and `PUB`.

Command names are case-insensitive on input. Subjects, sids, queue names, JSON
field names, and header names are case-sensitive unless noted otherwise.

The server may send `-ERR '<message>'` for malformed frames or rejected
operations. After protocol read errors, the server sends `-ERR` when possible
and closes the connection.

## Identifiers and Subjects

### Identifiers

The following values are identifiers:

- `durable_id`
- authenticated `client_id`
- subscription `sid`
- queue group name

Identifiers must be non-empty, must not contain `.`, must not contain
whitespace, and must not start with `_`.

### Publish Subjects

Publish subjects are concrete dot-separated tokens:

```text
orders.created
service.echo
_INBOX.client1.1
```

A publish subject is invalid if it is empty, starts or ends with `.`, contains
an empty token, contains whitespace, or contains `*` or `>`.

Subjects under `_BROKER.` are reserved. Publishing to `_BROKER.ACK...` is the
ACK mechanism. Publishing to any other `_BROKER.*` subject is rejected.
When authentication is enabled, an ACK publish is accepted only from an active
member of the durable consumer named in that ACK subject.

### Subscription Subjects

Subscription subjects use the same dot-token form and additionally support:

- `*` to match exactly one token.
- `>` to match the remaining tail and only when it is the final token.

Examples:

```text
orders.*
orders.>
>
_INBOX.client1.>
```

When authentication is enabled, `_INBOX.*` publish and subscribe subjects are
scoped to the authenticated client prefix: `_INBOX.<client_id>...`.

Invalid examples:

```text
orders..created
orders.foo*
orders.>.created
```

## Server Frames

### INFO

The server sends `INFO` immediately after connection setup.

Format:

```text
INFO <json>\r\n
```

Example without auth:

```text
INFO {"server_id":"broker","server_name":"broker","version":"0.1.0","proto":1,"max_payload":1048576,"auth_required":false,"tls_required":false}
```

Example with auth:

```text
INFO {"server_id":"broker","server_name":"broker","version":"0.1.0","proto":1,"max_payload":1048576,"nonce":"64-hex-character-nonce","auth_required":true,"tls_required":false}
```

Fields:

- `server_id`: stable string identifying this implementation, currently
  `"broker"`.
- `server_name`: server display name, currently `"broker"`.
- `version`: broker crate version.
- `proto`: protocol version, currently `1`.
- `max_payload`: maximum accepted `PUB` payload bytes for this connection.
- `auth_required`: boolean.
- `nonce`: present when `auth_required` is true.
- `tls_required`: boolean in the INFO payload. TLS, when configured, has
  already happened before this frame.

Clients should preserve unknown INFO fields for diagnostics but do not need to
interpret them.

### PONG

Response to `PING`.

```text
PONG\r\n
```

### +OK

Verbose success response.

```text
+OK\r\n
```

The server sends `+OK` only when verbose mode is enabled for the connection.
Verbose mode is enabled when either the server config has `verbose: true` or the
client sends `CONNECT {"verbose":true}`.

### P-ACK

Producer acknowledgement for a publish that requested per-message QoS.

```text
P-ACK <msg-id> <level> OK <retained> <seq> [<stream> <partition> <offset> <partitioning-epoch> <leader-epoch>]\r\n
```

`level` is the requested QoS value. `retained` is `true` when the subject is
bound to a configured stream and the publication is owned by that stream.
`seq` is the durable sequence number after append, or `-` for an accepted-only
acknowledgement that does not wait for append. A committed stream publication
also includes its stream-owned position and the partitioning and leader epochs
under which it was appended. `seq` remains a transitional consumer-ACK identity;
stream offsets are authoritative within each partition.

### -ERR

Error response.

```text
-ERR '<message>'\r\n
```

The message is human-readable. Single quotes are removed from emitted error
messages.

### MSG

Message delivery without protocol headers.

Formats:

```text
MSG <subject> <sid> <size>\r\n
<payload>\r\n
```

```text
MSG <subject> <sid> <reply-to> <size>\r\n
<payload>\r\n
```

Fields:

- `subject`: the publish subject.
- `sid`: the subscriber sid that matched the message.
- `reply-to`: optional reply subject. For durable messages without an original
  reply subject, the broker uses this slot for the ACK subject.
- `size`: decimal payload byte length.
- `payload`: exactly `size` bytes, followed by `\r\n`.

Example durable delivery:

```text
MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1 5\r\n
hello\r\n
```

Example transient delivery:

```text
MSG orders.created sid1 5\r\n
hello\r\n
```

Client behavior:

- If the optional `reply-to` starts with a valid broker ACK subject, treat it as
  `ack_subject`, not as an application reply subject.
- Otherwise, expose it as the application reply subject.

### HMSG

Message delivery with NATS-style headers.

Formats:

```text
HMSG <subject> <sid> <headers-len> <total-len>\r\n
<headers><payload>\r\n
```

```text
HMSG <subject> <sid> <reply-to> <headers-len> <total-len>\r\n
<headers><payload>\r\n
```

Fields:

- `subject`: the publish subject.
- `sid`: the subscriber sid that matched the message.
- `reply-to`: optional application reply subject.
- `headers-len`: decimal byte length of the header block.
- `total-len`: decimal byte length of header block plus payload.
- `headers`: a UTF-8 header block that starts with `NATS/1.0\r\n`, has
  zero or more `Name: Value\r\n` lines, and ends with a blank `\r\n`.
- `payload`: `total-len - headers-len` bytes.

The complete `<headers><payload>` section is followed by `\r\n`.

Durable deliveries with an application reply subject use `HMSG` so the reply
subject can remain available while the durable ACK subject is carried in the
`Broker-Ack` header.

Example:

```text
HMSG service.echo sid1 _INBOX.client1.1 65 70\r\n
NATS/1.0\r\n
Broker-Ack: _BROKER.ACK.durable-responder1-sid1.1.1\r\n
\r\n
hello\r\n
```

Client behavior:

- Parse header names case-insensitively.
- If a `Broker-Ack` header is present, expose it as the durable ACK subject.
- Preserve other headers for application use.

## Client Commands

### CONNECT

Identifies and configures the connection.

Format:

```text
CONNECT <json>\r\n
```

`CONNECT` with no payload is accepted and treated as `CONNECT {}`:

```text
CONNECT\r\n
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

Field types are strict:

- `verbose`: boolean, default `false`.
- `durable_id`: string identifier.
- `client_id`: string identifier.
- `signature`: string.
- `ack_timeout_ms`: unsigned integer.
- `max_in_flight`: unsigned integer that fits in the server platform `usize`.

Unknown fields are ignored for forward compatibility.

Authentication:

- When server auth is disabled, `durable_id` is optional. If omitted, `SUB`
  creates live transient subscriptions.
- When server auth is enabled, the client must send both `client_id` and
  `signature`.
- `signature` is an Ed25519 signature over the `nonce` string from `INFO`,
  encoded as lowercase or uppercase hex.
- If `durable_id` is also sent with auth enabled, it must match the
  authenticated `client_id`.
- Authenticated connections use the authenticated client ID as the durable
  identity.
- Servers may configure per-client publish and subscribe allowlists. Allowlist
  entries are subject patterns using the same `*` and `>` wildcard rules as
  subscriptions. Broker ack subjects and `_INBOX.*` request/reply subjects are
  allowed for their protocol roles.

Durable settings:

- `ack_timeout_ms` controls when unacked durable deliveries are eligible for
  redelivery. If omitted, the server default is used.
- `max_in_flight` limits simultaneous unacked durable deliveries for the
  durable consumer. If omitted, the server default is used.
- Both values must be greater than zero when applied by the server.

Examples:

Transient client:

```text
CONNECT {}\r\n
```

Durable client without auth:

```text
CONNECT {"durable_id":"client1","verbose":true,"ack_timeout_ms":30000,"max_in_flight":1024}\r\n
```

Authenticated durable client:

```text
CONNECT {"client_id":"client1","signature":"128-hex-character-ed25519-signature","verbose":true}\r\n
```

### PING

Health check.

```text
PING\r\n
```

The server replies:

```text
PONG\r\n
```

### PONG

Accepted and ignored.

```text
PONG\r\n
```

### SUB

Creates or attaches a subscription.

Formats:

```text
SUB <subject> <sid>\r\n
```

```text
SUB <subject> <queue> <sid>\r\n
```

Durable subscriptions may append an explicit start position to either form:

```text
SUB <subject> <sid> <start>\r\n
SUB <subject> <queue> <sid> <start>\r\n
```

Fields:

- `subject`: subscription subject, including optional `*` or `>`.
- `queue`: optional queue group identifier.
- `sid`: subscription identifier scoped to this connection.
- `start`: optional durable starting position: `@latest` (the default),
  `@earliest`, `@committed`, `@offset:<offset>`, or
  `@time:<unix-timestamp-ms>`.

Behavior:

- On a non-durable connection, `SUB` creates a live transient subscription.
- On a durable connection, non-`_INBOX.*` `SUB` creates or attaches a durable
  consumer.
- `_INBOX.*` subscriptions are always transient, even on durable connections.
- Queue groups are supported for durable non-inbox subscriptions.
- Transient subscriptions, including `_INBOX.*`, do not support queue groups.
- A start position initializes a new durable consumer. Attaching to an existing
  durable consumer resumes its persisted committed cursor instead.

Durable consumer identity:

- Non-queue durable subscriptions are keyed by durable identity and `sid`.
- Queue durable subscriptions are keyed by queue group and subscription subject.
- Reusing the same durable consumer across reconnects allows retained messages
  to be delivered again until ACKed.

Examples:

```text
SUB orders.* sid1\r\n
```

```text
SUB orders.* workers worker1\r\n
```

```text
SUB orders.* sid1 @earliest\r\n
```

```text
SUB _INBOX.client1.1 inbox1\r\n
```

### UNSUB

Detaches a subscription by sid.

Formats:

```text
UNSUB <sid>\r\n
```

```text
UNSUB <sid> <max-messages>\r\n
```

Fields:

- `sid`: existing subscription sid on the current connection.
- `max-messages`: optional positive integer.

Behavior:

- Without `max-messages`, the subscription is detached immediately.
- With `max-messages`, the subscription remains active for at most that many
  additional matching deliveries, then detaches.
- Unknown `sid` is an error.
- `max-messages` must be greater than zero.
- Detaching a durable member does not delete the durable consumer state.

Examples:

```text
UNSUB sid1\r\n
```

```text
UNSUB sid1 1\r\n
```

### PUB

Publishes a payload.

Formats:

```text
PUB <subject> <size>\r\n
<payload>\r\n
```

```text
PUB <subject> <reply-to> <size>\r\n
<payload>\r\n
```

Fields:

- `subject`: concrete publish subject.
- `reply-to`: optional concrete reply subject.
- `size`: decimal payload byte length.
- `payload`: exactly `size` bytes, followed by `\r\n`.

Payload bytes are opaque. They may contain arbitrary bytes, including newlines.
The `size` field is the only delimiter for the payload body. The body must be
followed by `\r\n`.

The `size` must be less than or equal to `max_payload` from `INFO`.

Examples:

```text
PUB orders.created 5\r\n
hello\r\n
```

```text
PUB service.echo _INBOX.client1.1 5\r\n
hello\r\n
```

Verbose response:

- If verbose mode is enabled, successful non-ACK publishes receive `+OK`.
- ACK publishes also receive `+OK` when verbose mode is enabled.

### HPUB

Header publish uses NATS-style headers and may request a per-message producer
acknowledgement:

```text
HPUB <subject> <headers-len> <total-len>\r\n
NATS/1.0\r\n
Broker-QoS: 1\r\n
Broker-Msg-Id: msg-123\r\n
\r\n
hello\r\n
```

With reply subject:

```text
HPUB <subject> <reply-to> <headers-len> <total-len>\r\n
...
```

QoS headers are producer metadata and are not forwarded to subscribers.

- `Broker-QoS`: optional numeric value.
  - `0`: accepted after validation, authorization, transient delivery
    preparation, and route forwarding.
  - `1`: durable after local durable append or successful cluster write.
  - `2`: high durability after local WAL flush, or successful cluster write in
    clustered mode.
  - `3`: cluster durable after successful cluster write; rejected when
    clustering is disabled.
- `Broker-Msg-Id`: required when `Broker-QoS` is present. It must be non-empty,
  at most 128 bytes, and contain no whitespace.
- `Broker-Key`: optional opaque UTF-8 partition key. An explicit key takes
  precedence over the stream's configured subject or fallback strategy.

Other HPUB headers are application headers. The broker retains them in the
immutable stream envelope and returns them on durable and live `HMSG`
deliveries. `Broker-QoS`, `Broker-Msg-Id`, and `Broker-Key` are broker metadata
and are not forwarded to subscribers.

Successful QoS publishes receive `P-ACK` and do not also receive verbose `+OK`.
QoS levels 1 through 3 require a configured stream binding for the publish
subject; an unbound subject receives `-ERR 'NO_DURABLE_BINDING ...'`. Level 0
may succeed without a stream binding and reports `retained=false`. Other
failures receive `-ERR`.

## Durable ACKs

Durable deliveries must be acknowledged by publishing an empty or non-empty
payload to the ACK subject supplied by the broker.

ACK subject format:

```text
_BROKER.ACK.<consumer-id>.<seq>.<delivery-id>
```

Fields:

- `consumer-id`: broker durable consumer identifier.
- `seq`: durable message sequence number.
- `delivery-id`: delivery attempt identifier.

ACK example:

```text
PUB _BROKER.ACK.durable-client1-sid1.1.1 0\r\n
\r\n
```

Only the current valid delivery attempt is ACKed. Late, duplicate, malformed,
or unknown ACK subjects are ignored.

If a durable delivery is not ACKed before `ack_timeout_ms`, it becomes eligible
for redelivery to an active member of the same durable consumer.

## Request/Reply

Request/reply is built from `SUB` and `PUB`:

1. The requester subscribes to a unique `_INBOX.*` subject.
2. The requester publishes to the service subject with the inbox as `reply-to`.
3. A responder receives the request.
4. The responder publishes the response to the inbox.

Example requester:

```text
CONNECT {}\r\n
SUB _INBOX.client1.1 inbox1\r\n
PING\r\n
PUB service.echo _INBOX.client1.1 5\r\n
hello\r\n
```

Example durable responder delivery:

```text
HMSG service.echo sid1 _INBOX.client1.1 65 70\r\n
NATS/1.0\r\n
Broker-Ack: _BROKER.ACK.durable-responder1-sid1.1.1\r\n
\r\n
hello\r\n
```

Example response and ACK:

```text
PUB _INBOX.client1.1 5\r\n
world\r\n
PUB _BROKER.ACK.durable-responder1-sid1.1.1 0\r\n
\r\n
```

Inbox behavior:

- `_INBOX.*` subscriptions are transient and live-only.
- Inbox messages are not retained when there is no matching live transient
  subscription.
- In clustered route-mesh mode, transient inbox interest is propagated so
  request/reply can cross nodes.

## Retention and Delivery Semantics

Transient subscriptions:

- Live-only.
- Removed on disconnect or `UNSUB`.
- Do not create WAL state.
- Do not receive ACK subjects.

Durable subscriptions:

- Persist consumer state in the WAL or Raft state, depending on server mode.
- Read retained messages owned by configured streams that match their subject.
- Track independent delivered and committed offsets for each matching stream
  partition. Queue-group members attach to the same durable consumer and share
  those cursors and delivery leases.
- Keep out-of-order acknowledgements in a bounded window. Acknowledging a later
  offset does not advance the committed cursor across an unacknowledged matching
  offset; closing the gap advances it through the acknowledged run.
- Preserve delivery leases and attempt numbers across restart and replicated
  failover. Redelivery updates lease state without modifying the stored record.
- Advance deterministically to the earliest retained offset when retention has
  removed unread history, and expose that event as a retention gap in the admin
  subscription response.
- Do not control whether new publications are retained; adding or removing a
  consumer does not change a stream's append behavior.
- Deliveries include an ACK subject either as the `MSG` reply slot or as the
  `Broker-Ack` header in `HMSG`.
- Unacked messages are redelivered after `ack_timeout_ms`.

Stream retention:

- A non-inbox publication whose subject matches a configured stream is appended
  once to its primary stream, whether or not a durable consumer exists.
- A publication without a stream binding remains live-only unless durable QoS
  was requested, in which case it is rejected as described above.
- `_INBOX.*` publications always remain transient.
- Live transient delivery is attempted before the durable append or cluster
  commit. A later durability failure can therefore follow a live delivery.

Partition storage:

- Each stream partition has an independent monotonically increasing offset and
  segmented append log under the configured WAL directory's `streams` tree.
- The immutable envelope retains namespace, stream, partition, offset, subject,
  key, application headers, timestamp, reply subject, payload, partitioning
  epoch, and leader epoch.
- Explicit keys use a stable hash during a partitioning epoch. Subject-token
  partitioning uses the configured token; subject-hash and sticky selection are
  the documented fallbacks when the preferred value is absent.
- The control WAL stores partition append references and consumer/control state,
  not new stream payloads. On first startup after upgrading, transitional
  stream-owned publish records from the previous WAL format are copied into
  partition history and replaced by references at checkpoint. Missing stream
  configuration or a dangling reference fails startup with a compatibility
  error.

Queue durable subscriptions:

- Share one durable consumer across members with the same queue group and
  subject.
- Each message is delivered to one active member at a time.

## Parser Limits and Error Cases

Clients should expect `-ERR` for:

- Unsupported commands.
- Empty protocol lines.
- Non-UTF-8 command lines.
- Malformed `CONNECT` JSON.
- Wrong JSON types for supported `CONNECT` fields.
- Missing or extra command arguments.
- Non-integer payload lengths or `UNSUB` counts.
- Payload lengths greater than `max_payload`.
- Payloads not followed by `\r\n`.
- Invalid publish subjects, subscription subjects, identifiers, or queue usage.
- Reserved non-ACK `_BROKER.*` publishes.

The server accepts command lines terminated by `\n` or `\r\n`, but emitted
server frames always use `\r\n`.

## Minimal Durable Client Flow

```text
S: INFO {"server_id":"broker","server_name":"broker","version":"0.1.0","proto":1,"max_payload":1048576,"auth_required":false,"tls_required":false}\r\n
C: CONNECT {"durable_id":"client1","ack_timeout_ms":30000,"max_in_flight":1024}\r\n
C: SUB orders.* sid1\r\n
C: PING\r\n
S: PONG\r\n

C: PUB orders.created 5\r\n
C: hello\r\n

S: MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1 5\r\n
S: hello\r\n

C: PUB _BROKER.ACK.durable-client1-sid1.1.1 0\r\n
C: \r\n
```

## Minimal Transient Client Flow

```text
S: INFO {"server_id":"broker","server_name":"broker","version":"0.1.0","proto":1,"max_payload":1048576,"auth_required":false,"tls_required":false}\r\n
C: CONNECT {}\r\n
C: SUB orders.* sid1\r\n
C: PING\r\n
S: PONG\r\n

S: MSG orders.created sid1 5\r\n
S: hello\r\n
```
