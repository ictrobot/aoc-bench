// Protocol parser for META and SAMPLE lines

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolLine {
    Meta(MetaLine),
    Sample(SampleLine),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetaLine {
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SampleLine {
    pub iters: u64,
    pub total_ns: u64,
    pub checksum: Option<String>,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    InvalidFormat(String),
    InvalidNumber(String),
    MissingField(String),
    ChecksumMismatch {
        expected: String,
        actual: Option<String>,
    },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidFormat(msg) => write!(f, "Invalid format: {msg}"),
            ParseError::InvalidNumber(msg) => write!(f, "Invalid number: {msg}"),
            ParseError::MissingField(msg) => write!(f, "Missing field: {msg}"),
            ParseError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "Checksum mismatch: expected '{expected}', got {actual:?}"
                )
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a line from the benchmark protocol
pub fn parse_line(line: &str) -> Result<ProtocolLine, ParseError> {
    let line = line.trim();

    if line.starts_with("META ") {
        parse_meta_line(line)
    } else if line.starts_with("SAMPLE ") {
        parse_sample_line(line)
    } else {
        Err(ParseError::InvalidFormat(format!(
            "Line must start with 'META ' or 'SAMPLE ', got: {line}"
        )))
    }
}

/// Parse a META line: META key=value,key2=value2,...
fn parse_meta_line(line: &str) -> Result<ProtocolLine, ParseError> {
    let content = &line[5..]; // Skip "META "
    let fields = parse_comma_separated_kv(content)?;

    Ok(ProtocolLine::Meta(MetaLine { fields }))
}

/// Parse a SAMPLE line: SAMPLE <iters> <`total_ns`> [key=value,key2=value2,...]
fn parse_sample_line(line: &str) -> Result<ProtocolLine, ParseError> {
    let content = &line[7..]; // Skip "SAMPLE "
    let mut parts = content.split_whitespace();

    // Parse iters (required)
    let iters_str = parts
        .next()
        .ok_or_else(|| ParseError::MissingField("SAMPLE line requires iters".to_string()))?;
    let iters = iters_str.parse::<u64>().map_err(|e| {
        ParseError::InvalidNumber(format!("Failed to parse iters '{iters_str}': {e}"))
    })?;

    // Ensure iters is non-zero
    if iters == 0 {
        return Err(ParseError::InvalidNumber(
            "iters must be non-zero".to_string(),
        ));
    }

    // Parse total_ns (required)
    let total_ns_str = parts
        .next()
        .ok_or_else(|| ParseError::MissingField("SAMPLE line requires total_ns".to_string()))?;
    let total_ns = total_ns_str.parse::<u64>().map_err(|e| {
        ParseError::InvalidNumber(format!("Failed to parse total_ns '{total_ns_str}': {e}"))
    })?;

    // Parse optional comma-separated key=value pairs
    // Since KV pairs must be URL encoded (no spaces allowed), there should be at most one more part
    let fields = if let Some(kv_part) = parts.next() {
        // Check for extra whitespace-separated parts (not allowed)
        if parts.next().is_some() {
            return Err(ParseError::InvalidFormat(
                "SAMPLE line has unexpected whitespace in key=value pairs (must be URL encoded)"
                    .to_string(),
            ));
        }
        parse_comma_separated_kv(kv_part)?
    } else {
        HashMap::new()
    };

    let checksum = fields.get("checksum").cloned();

    Ok(ProtocolLine::Sample(SampleLine {
        iters,
        total_ns,
        checksum,
        fields,
    }))
}

/// Parse comma-separated key=value pairs (URL encoded, no spaces allowed)
fn parse_comma_separated_kv(content: &str) -> Result<HashMap<String, String>, ParseError> {
    let mut fields = HashMap::new();

    // Empty content is OK (no fields)
    if content.is_empty() {
        return Ok(fields);
    }

    // Strict parsing: no trailing/leading commas, no empty parts
    for part in content.split(',') {
        if part.is_empty() {
            return Err(ParseError::InvalidFormat(
                "Empty key=value pair (trailing/leading/double comma not allowed)".to_string(),
            ));
        }

        // Check for spaces (not allowed - must be URL encoded)
        if part.contains(' ') {
            return Err(ParseError::InvalidFormat(format!(
                "Spaces not allowed in key=value pairs (must be URL encoded as %20), got: {part}"
            )));
        }

        if let Some((key, value)) = part.split_once('=') {
            if key.is_empty() || value.is_empty() {
                return Err(ParseError::InvalidFormat(format!(
                    "Empty key or value not allowed, got: {part}"
                )));
            }

            // URL decode the key and value
            let key_decoded = url_decode(key)?;
            let value_decoded = url_decode(value)?;
            fields.insert(key_decoded, value_decoded);
        } else {
            return Err(ParseError::InvalidFormat(format!(
                "Expected key=value format, got: {part}"
            )));
        }
    }

    Ok(fields)
}

/// URL decode (percent decode) a string
/// Handles %HH encoding
fn url_decode(s: &str) -> Result<String, ParseError> {
    // Fast path: if no % in string, return as-is
    if !s.contains('%') {
        return Ok(s.to_string());
    }

    // Pre-allocate with exact capacity to avoid reallocations
    let mut result = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' {
            // Need at least 2 more bytes for %HH
            if i + 2 >= bytes.len() {
                return Err(ParseError::InvalidFormat(
                    "Incomplete percent encoding".to_string(),
                ));
            }

            // Parse hex digits directly from bytes
            let hex1 = bytes[i + 1];
            let hex2 = bytes[i + 2];

            let digit1 = hex_digit_to_value(hex1)?;
            let digit2 = hex_digit_to_value(hex2)?;

            let byte = (digit1 << 4) | digit2;
            result.push(byte as char);
            i += 3;
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    Ok(result)
}

/// Convert a hex digit byte to its numeric value
#[inline]
fn hex_digit_to_value(b: u8) -> Result<u8, ParseError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => Err(ParseError::InvalidFormat(format!(
            "Invalid hex digit in percent encoding: {}",
            b as char
        ))),
    }
}

/// Validate that a sample's checksum matches the expected checksum
/// Returns Ok if validation passes, Err if mismatch
pub fn validate_checksum(sample: &SampleLine, expected: &str) -> Result<(), ParseError> {
    match &sample.checksum {
        Some(actual) if actual == expected => Ok(()),
        actual => Err(ParseError::ChecksumMismatch {
            expected: expected.to_string(),
            actual: actual.clone(),
        }),
    }
}

/// Validate META line version field if present
/// Returns Ok if no version field, or if version=1
/// Returns Err if version field is present but not "1"
pub fn validate_meta_version(meta: &MetaLine) -> Result<(), ParseError> {
    if let Some(version) = meta.fields.get("version")
        && version != "1"
    {
        return Err(ParseError::InvalidFormat(format!(
            "Unsupported protocol version: {version}, expected version=1"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meta_line() {
        let line = "META version=1";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                assert_eq!(meta.fields.get("version"), Some(&"1".to_string()));
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_parse_meta_line_multiple_fields() {
        let line = "META version=1,foo=bar,baz=qux";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                assert_eq!(meta.fields.get("version"), Some(&"1".to_string()));
                assert_eq!(meta.fields.get("foo"), Some(&"bar".to_string()));
                assert_eq!(meta.fields.get("baz"), Some(&"qux".to_string()));
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_parse_sample_line_basic() {
        let line = "SAMPLE 10000 30920000000";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.iters, 10_000);
                assert_eq!(sample.total_ns, 30_920_000_000);
                assert_eq!(sample.checksum, None);
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_sample_line_with_checksum() {
        let line = "SAMPLE 10000 30920000000 checksum=8f024a8e";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.iters, 10_000);
                assert_eq!(sample.total_ns, 30_920_000_000);
                assert_eq!(sample.checksum, Some("8f024a8e".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_sample_line_with_extra_fields() {
        let line = "SAMPLE 10000 30920000000 checksum=8f024a8e,foo=bar";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.iters, 10_000);
                assert_eq!(sample.total_ns, 30_920_000_000);
                assert_eq!(sample.checksum, Some("8f024a8e".to_string()));
                assert_eq!(sample.fields.get("foo"), Some(&"bar".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_invalid_line() {
        let line = "INVALID 123";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_missing_fields() {
        let line = "SAMPLE 10000";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_invalid_number() {
        let line = "SAMPLE abc 30920000000";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_meta_invalid_format() {
        let line = "META invalid";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_checksum_match() {
        let line = "SAMPLE 10000 30920000000 checksum=8f024a8e";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert!(validate_checksum(&sample, "8f024a8e").is_ok());
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_validate_checksum_mismatch() {
        let line = "SAMPLE 10000 30920000000 checksum=8f024a8e";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                let result = validate_checksum(&sample, "different");
                assert!(result.is_err());
                assert!(matches!(
                    result.unwrap_err(),
                    ParseError::ChecksumMismatch { .. }
                ));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_validate_checksum_missing() {
        let line = "SAMPLE 10000 30920000000";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.total_ns, 30_920_000_000);
                let result = validate_checksum(&sample, "8f024a8e");
                assert!(result.is_err());
                assert!(matches!(
                    result.unwrap_err(),
                    ParseError::ChecksumMismatch { .. }
                ));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_empty_line() {
        let line = "";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_only() {
        let line = "   ";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_meta_without_trim() {
        // When there's content after META, it works
        let line = "META version=1  "; // Has trailing spaces
        let result = parse_line(line);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_meta_only() {
        let line = "META";
        // Should fail - no space after META
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_meta_with_space_only() {
        let line = "META ";
        // After trim() this becomes "META" which should fail
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_zero_iters() {
        let line = "SAMPLE 0 0";
        let result = parse_line(line);
        assert!(result.is_err());
        assert!(matches!(result, Err(ParseError::InvalidNumber(_))));
    }

    #[test]
    fn test_parse_sample_large_numbers() {
        let line = "SAMPLE 18446744073709551615 18446744073709551615";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.iters, u64::MAX);
                assert_eq!(sample.total_ns, u64::MAX);
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_sample_overflow() {
        // Number larger than u64::MAX
        let line = "SAMPLE 99999999999999999999999999 100";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_negative_number() {
        let line = "SAMPLE -100 50000";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_float_number() {
        let line = "SAMPLE 100.5 50000";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_meta_version_valid() {
        let line = "META version=1";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                assert!(validate_meta_version(&meta).is_ok());
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_validate_meta_version_missing() {
        let line = "META foo=bar";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                // No version field is OK
                assert!(validate_meta_version(&meta).is_ok());
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_validate_meta_version_invalid() {
        let line = "META version=2";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                let result = validate_meta_version(&meta);
                assert!(result.is_err());
                assert!(matches!(result.unwrap_err(), ParseError::InvalidFormat(_)));
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_validate_meta_version_multiple_fields() {
        let line = "META version=1,foo=bar";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Meta(meta) => {
                assert!(validate_meta_version(&meta).is_ok());
            }
            _ => panic!("Expected Meta line"),
        }
    }

    #[test]
    fn test_parse_meta_trailing_comma_fails() {
        // Trailing commas are not allowed (strict parsing)
        let line = "META version=1,";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_with_spaces_fails() {
        // Spaces are not allowed - must be URL encoded
        let line = "SAMPLE 100 5000 checksum=abc , foo=bar";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_with_url_encoded_space() {
        // Spaces must be URL encoded as %20
        let line = "SAMPLE 100 5000 message=hello%20world";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(
                    sample.fields.get("message"),
                    Some(&"hello world".to_string())
                );
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_parse_meta_double_comma_fails() {
        // Double comma is not allowed (strict parsing)
        let line = "META version=1,,foo=bar";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sample_value_with_special_chars() {
        // Values can contain special characters (but not commas or equals)
        let line = "SAMPLE 100 5000 checksum=abc-123_def";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.checksum, Some("abc-123_def".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_url_decode_special_chars() {
        // Test URL encoding of special characters
        let line = "SAMPLE 100 5000 msg=%3Ctest%3E%26%22";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.fields.get("msg"), Some(&"<test>&\"".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_url_decode_hex_digits() {
        // Test hex encoding
        let line = "SAMPLE 100 5000 msg=%41%42%43";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.fields.get("msg"), Some(&"ABC".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_url_decode_comma_and_equals() {
        // Comma and equals must be encoded
        let line = "SAMPLE 100 5000 msg=a%3Db%2Cc";
        let result = parse_line(line).unwrap();

        match result {
            ProtocolLine::Sample(sample) => {
                assert_eq!(sample.fields.get("msg"), Some(&"a=b,c".to_string()));
            }
            _ => panic!("Expected Sample line"),
        }
    }

    #[test]
    fn test_url_decode_incomplete_fails() {
        // Incomplete percent encoding should fail
        let line = "SAMPLE 100 5000 msg=test%2";
        let result = parse_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_url_decode_invalid_hex_fails() {
        // Invalid hex digits should fail
        let line = "SAMPLE 100 5000 msg=test%ZZ";
        let result = parse_line(line);
        assert!(result.is_err());
    }
}
