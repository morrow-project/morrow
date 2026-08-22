use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Byte(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(&'static str);

impl Byte {
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl FromStr for Byte {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        let split = input
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(input.len());
        let (number, unit) = input.split_at(split);
        let value = number
            .parse::<f64>()
            .map_err(|_| ParseError("invalid byte count"))?;
        if !value.is_finite() || value.is_sign_negative() {
            return Err(ParseError("invalid byte count"));
        }
        let multiplier = match unit.trim() {
            "" | "B" => 1_f64,
            "KB" => 1_000_f64,
            "MB" => 1_000_000_f64,
            "GB" => 1_000_000_000_f64,
            "TB" => 1_000_000_000_000_f64,
            "KiB" => 1_024_f64,
            "MiB" => 1_048_576_f64,
            "GiB" => 1_073_741_824_f64,
            "TiB" => 1_099_511_627_776_f64,
            _ => return Err(ParseError("unknown byte unit")),
        };
        let bytes = value * multiplier;
        if bytes > u64::MAX as f64 {
            return Err(ParseError("byte count is too large"));
        }
        Ok(Self(bytes as u64))
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
