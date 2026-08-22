pub const CONTROL_PLANE_VERSION: u32 = 1;
pub const CONFIG_SUBJECT: &str = "morrow/connect/config";
pub const STATUS_SUBJECT: &str = "morrow/connect/status";
pub const OFFSET_SUBJECT: &str = "morrow/connect/offset";
pub const SCHEMA_SUBJECT: &str = "morrow/connect/schema";

pub const CONTROL_SUBJECTS: [(&str, &str); 4] = [
    ("morrow-connect-config", CONFIG_SUBJECT),
    ("morrow-connect-status", STATUS_SUBJECT),
    ("morrow-connect-offset", OFFSET_SUBJECT),
    ("morrow-connect-schema", SCHEMA_SUBJECT),
];
