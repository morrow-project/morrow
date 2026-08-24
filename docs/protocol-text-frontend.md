# Protocol v1 text frontend

Protocol v1 retains the existing command-oriented text grammar for quick
experiments. It also provides a lossless model-backed debug frame:

```text
FRAME {"Request":{"request_id":9,"body":{"Publish":{...}}}}
```

The JSON value is the same semantic model encoded inside a CBOR frame. It is
line-oriented, accepts CRLF or LF on input, and is subject to an explicit line
limit. This representation is intended for `nc`-style debugging and
prototyping; CBOR remains the production encoding for compactness and binary
payload efficiency.
