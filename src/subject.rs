pub fn validate_subject(subject: &str) -> bool {
    if subject.is_empty() || subject.starts_with('.') || subject.ends_with('.') {
        return false;
    }

    subject.split('.').all(|token| {
        !token.is_empty()
            && token != "*"
            && token != ">"
            && !token.contains('*')
            && !token.contains('>')
            && !token.chars().any(char::is_whitespace)
    })
}

pub fn validate_subscription(pattern: &str) -> bool {
    if pattern.is_empty() || pattern.starts_with('.') || pattern.ends_with('.') {
        return false;
    }

    let mut saw_tail = false;
    for (idx, token) in pattern.split('.').enumerate() {
        if token.is_empty() || saw_tail || token.chars().any(char::is_whitespace) {
            return false;
        }
        if token == ">" {
            saw_tail = true;
            if idx == 0 && pattern != ">" {
                return false;
            }
            continue;
        }
        if token != "*" && (token.contains('*') || token.contains('>')) {
            return false;
        }
    }
    true
}

pub fn matches(pattern: &str, subject: &str) -> bool {
    let mut pattern_tokens = pattern.split('.');
    let mut subject_tokens = subject.split('.');

    loop {
        match (pattern_tokens.next(), subject_tokens.next()) {
            (Some(">"), _) => return true,
            (Some("*"), Some(_)) => {}
            (Some(pattern), Some(subject)) if pattern == subject => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_publish_subjects() {
        assert!(validate_subject("orders.created"));
        assert!(!validate_subject("orders.*"));
        assert!(!validate_subject(".orders"));
        assert!(!validate_subject("orders."));
        assert!(!validate_subject("orders..created"));
    }

    #[test]
    fn validates_subscription_patterns() {
        assert!(validate_subscription("orders.*"));
        assert!(validate_subscription("orders.>"));
        assert!(validate_subscription(">"));
        assert!(!validate_subscription("orders.>.created"));
        assert!(!validate_subscription("orders.foo*"));
    }

    #[test]
    fn matches_nats_wildcards() {
        assert!(matches("orders.*", "orders.created"));
        assert!(!matches("orders.*", "orders.us.created"));
        assert!(matches("orders.>", "orders.us.created"));
        assert!(matches(">", "anything"));
        assert!(!matches("orders.created", "orders.deleted"));
    }
}
