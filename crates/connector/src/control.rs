use client::{Client, ProducerAck, protocol::AckLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRecordKind {
    Config,
    Status,
    Offset,
    Schema,
}

impl ControlRecordKind {
    pub fn subject(self) -> &'static str {
        match self {
            Self::Config => client::protocol::connector_control::CONFIG_SUBJECT,
            Self::Status => client::protocol::connector_control::STATUS_SUBJECT,
            Self::Offset => client::protocol::connector_control::OFFSET_SUBJECT,
            Self::Schema => client::protocol::connector_control::SCHEMA_SUBJECT,
        }
    }
}

pub async fn store_control_record(
    client: &mut Client,
    kind: ControlRecordKind,
    key: &str,
    payload: &[u8],
    message_id: &str,
) -> Result<ProducerAck, String> {
    let ack = client
        .publish_with_qos_and_key(
            kind.subject(),
            None,
            payload,
            AckLevel::HighDurability,
            message_id,
            Some(key),
        )
        .await
        .map_err(display)?;
    if !ack.retained {
        return Err(format!(
            "connector control subject {} has no durable stream binding",
            kind.subject()
        ));
    }
    Ok(ack)
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
