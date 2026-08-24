# Morrow protocol v1 CBOR framing

The CBOR transport uses a fixed big-endian envelope followed by CBOR metadata
and an opaque payload:

```text
offset  size  field
0       4     magic: MOR1
4       2     protocol version
6       1     frame kind
7       1     flags
8       8     request ID, or zero when not applicable
16      4     metadata length
20      4     payload length
24      4     CRC32 checksum of metadata followed by payload
28      ...   CBOR metadata, then payload bytes
```

All integer fields are unsigned big-endian values. Protocol v1 currently
requires flags to be zero. Frame kinds are:

| Value | Kind |
| ---: | --- |
| 1 | request |
| 2 | response |
| 3 | delivery |
| 4 | window update |
| 5 | error |

Publish requests and deliveries carry their message payload outside the CBOR
metadata. The corresponding semantic model still exposes that payload as a
single byte vector after decoding.

Decoders must validate the magic, version, flags, all lengths, configured frame
limits, exact frame length, and checksum before accepting metadata or payload.
Unknown CBOR fields are preserved or ignored according to the semantic model's
forward-compatibility rules.
