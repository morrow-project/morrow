# Protocol v1 model

Protocol v1 has one semantic model and multiple wire codecs. The broker
boundary should exchange the values in `src/model.rs`; a codec is responsible
only for turning bytes into those values and values back into bytes.

The initial production codec will be CBOR. The text codec remains a supported
debug and prototyping frontend. Both codecs must represent the same request,
response, delivery, error, and flow-control semantics.

## Model invariants

- Requests carry a client-selected `request_id`.
- Responses repeat the request ID and asynchronous deliveries do not use the
  response channel.
- Messages have one representation for publishing, delivery, fetching, and
  replay.
- Application headers are distinct from broker metadata and may contain
  repeated binary values.
- Durable acknowledgement identity is an opaque delivery token.
- Unknown encoded fields may be ignored unless the operation explicitly marks
  them as required.
- Payload bytes are never interpreted by the protocol model.

The current text parser and encoder are retained while the CBOR transport is
introduced. New protocol features should be added to this model first and then
implemented by each codec.
