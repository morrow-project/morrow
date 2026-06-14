use std::{error::Error, fmt};

#[derive(Debug)]
pub struct BrokerError {
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

pub type Result<T> = std::result::Result<T, BrokerError>;

impl BrokerError {
    pub fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for BrokerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<std::io::Error> for BrokerError {
    fn from(source: std::io::Error) -> Self {
        Self::with_source(source.to_string(), source)
    }
}

impl From<std::string::FromUtf8Error> for BrokerError {
    fn from(source: std::string::FromUtf8Error) -> Self {
        Self::with_source("invalid UTF-8 string", source)
    }
}

impl From<std::num::TryFromIntError> for BrokerError {
    fn from(source: std::num::TryFromIntError) -> Self {
        Self::with_source("integer conversion failed", source)
    }
}

impl From<protocol::ProtocolError> for BrokerError {
    fn from(source: protocol::ProtocolError) -> Self {
        Self::with_source(source.to_string(), source)
    }
}

impl From<protocol::auth::AuthError> for BrokerError {
    fn from(source: protocol::auth::AuthError) -> Self {
        Self::with_source(source.to_string(), source)
    }
}

pub trait ResultExt<T> {
    fn context(self, message: impl Into<String>) -> Result<T>;
    fn with_context<F>(self, message: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Error + Send + Sync + 'static,
{
    fn context(self, message: impl Into<String>) -> Result<T> {
        self.map_err(|source| BrokerError::with_source(message, source))
    }

    fn with_context<F>(self, message: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|source| BrokerError::with_source(message(), source))
    }
}

#[macro_export]
macro_rules! broker_ensure {
    ($condition:expr, $($arg:tt)+) => {
        if !$condition {
            return Err($crate::error::BrokerError::msg(format!($($arg)+)));
        }
    };
}

#[macro_export]
macro_rules! broker_bail {
    ($($arg:tt)+) => {
        return Err($crate::error::BrokerError::msg(format!($($arg)+)))
    };
}
