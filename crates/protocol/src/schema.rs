//! Machine-readable protocol schema exposed to SDK and conformance tooling.

pub const PROTOCOL_V1_CDDL: &str = include_str!("../schema/protocol-v1.cddl");

#[cfg(test)]
mod tests {
    use super::PROTOCOL_V1_CDDL;

    #[test]
    fn schema_contains_the_core_protocol_values() {
        for rule in [
            "request =",
            "response =",
            "message =",
            "delivery =",
            "error =",
        ] {
            assert!(PROTOCOL_V1_CDDL.contains(rule), "missing CDDL rule {rule}");
        }
    }
}
