# PEM utility crate

The `broker-pem` crate provides strict certificate and private-key loading for
Morrow's TLS configuration. It validates supported PEM sections, rejects
ambiguous key material, and is shared by the server and client crates.
