# Protocol v1 error codes

Clients must branch on the stable error `code`, not on the human-readable
`message`. An error may include a request ID, retry-after duration, and detail.

| Code | Retryable by default | Meaning |
| --- | :---: | --- |
| `INVALID_REQUEST` | no | Request arguments are invalid. |
| `INVALID_FRAME` | no | Wire framing or metadata is malformed. |
| `UNSUPPORTED_VERSION` | no | The requested protocol version is unavailable. |
| `UNSUPPORTED_ENCODING` | no | The requested wire encoding is unavailable. |
| `AUTHENTICATION_REQUIRED` | no | The connection must authenticate first. |
| `AUTHENTICATION_FAILED` | no | Authentication proof was rejected. |
| `PERMISSION_DENIED` | no | The identity lacks permission. |
| `NOT_FOUND` | no | The requested resource does not exist. |
| `CONFLICT` | no | The request conflicts with current state. |
| `RESOURCE_EXHAUSTED` | no | A configured resource limit was exceeded. |
| `TIMEOUT` | yes | The operation did not complete before its deadline. |
| `STORAGE_UNAVAILABLE` | yes | Required durable storage is unavailable. |
| `OVERLOADED` | yes | The server cannot accept more work now. |
| `INTERNAL` | no | An unexpected server error occurred. |
