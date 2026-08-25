use super::*;

pub(super) fn get_views(value: &serde_json::Value) -> Result<HashMap<String, ViewConfig>> {
    let Some(views) = value.get("views") else {
        return Ok(HashMap::new());
    };
    let serde_json::Value::Object(views) = views else {
        return Err(BrokerError::msg("config field views must be an object"));
    };
    let mut result = HashMap::new();
    for (name, value) in views {
        crate::tenancy::TenantId::new(name.clone())
            .or_else(|_| Err(BrokerError::msg("config field views.name is invalid")))?;
        let serde_json::Value::Object(fields) = value else {
            return Err(BrokerError::msg(format!(
                "config field views.{name} must be an object"
            )));
        };
        for field in fields.keys() {
            crate::broker_ensure!(
                matches!(
                    field.as_str(),
                    "tenant"
                        | "source_stream"
                        | "source_subject"
                        | "key_header"
                        | "max_entries"
                        | "max_value_bytes"
                        | "watch_capacity"
                ),
                "unknown field views.{name}.{field}"
            );
        }
        let tenant = get_string(value, "tenant")?
            .unwrap_or("default")
            .to_string();
        crate::tenancy::TenantId::new(tenant.clone())?;
        let source_stream = get_string(value, "source_stream")?
            .ok_or_else(|| {
                BrokerError::msg(format!(
                    "config field views.{name}.source_stream is required"
                ))
            })?
            .to_string();
        crate::stream::StreamId::new(source_stream.clone())?;
        let source_subject = get_string(value, "source_subject")?.map(str::to_string);
        if let Some(subject) = &source_subject {
            crate::broker_ensure!(
                protocol::subject::validate_subscription(subject),
                "config field views.{name}.source_subject is invalid"
            );
        }
        let key_header = get_string(value, "key_header")?.map(str::to_string);
        let max_entries = get_bounded_usize(value, "max_entries", 100_000)?;
        let max_value_bytes = get_bounded_usize(value, "max_value_bytes", 1_048_576)?;
        let watch_capacity = get_bounded_usize(value, "watch_capacity", 10_000)?;
        crate::broker_ensure!(
            max_entries > 0 && max_value_bytes > 0 && watch_capacity > 0,
            "view limits must be greater than zero"
        );
        result.insert(
            name.clone(),
            ViewConfig {
                tenant,
                source_stream,
                source_subject,
                key_header,
                max_entries,
                max_value_bytes,
                watch_capacity,
            },
        );
    }
    Ok(result)
}
