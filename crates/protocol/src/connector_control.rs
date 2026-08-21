pub const CONTROL_PLANE_VERSION: u32 = 1;
pub const CONFIG_SUBJECT: &str = "$BROKER.CONNECT.config";
pub const STATUS_SUBJECT: &str = "$BROKER.CONNECT.status";
pub const OFFSET_SUBJECT: &str = "$BROKER.CONNECT.offset";
pub const SCHEMA_SUBJECT: &str = "$BROKER.CONNECT.schema";

pub const CONTROL_SUBJECTS: [(&str, &str); 4] = [
    ("broker-connect-config", CONFIG_SUBJECT),
    ("broker-connect-status", STATUS_SUBJECT),
    ("broker-connect-offset", OFFSET_SUBJECT),
    ("broker-connect-schema", SCHEMA_SUBJECT),
];
