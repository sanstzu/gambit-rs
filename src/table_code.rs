use url::Url;

use crate::{Error, Result};

/// Normalizes a table slug, `/play/<slug>` path, or first-party Gambit play URL.
pub fn normalize_table_code(value: &str) -> Result<String> {
    let mut candidate = value.trim().to_owned();
    if candidate.is_empty() {
        return Err(Error::Configuration(
            "table code cannot be empty".to_owned(),
        ));
    }

    if candidate.contains("://") {
        let parsed = Url::parse(&candidate)
            .map_err(|_| Error::Configuration("table URL is invalid".to_owned()))?;
        if parsed.scheme() != "https"
            || !matches!(parsed.host_str(), Some("gambit.com" | "www.gambit.com"))
        {
            return Err(Error::Configuration(
                "table URL must use HTTPS on gambit.com".to_owned(),
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::Configuration(
                "table URL cannot contain a query or fragment".to_owned(),
            ));
        }
        let segments: Vec<_> = parsed
            .path_segments()
            .ok_or_else(|| Error::Configuration("table URL path is invalid".to_owned()))?
            .filter(|segment| !segment.is_empty())
            .collect();
        if segments.len() != 2 || segments[0] != "play" {
            return Err(Error::Configuration(
                "table URL must have the form /play/<table-code>".to_owned(),
            ));
        }
        candidate = percent_decode(segments[1])?;
    } else {
        if let Some(stripped) = candidate.strip_prefix("/play/") {
            candidate = stripped.to_owned();
        }
        candidate.truncate(candidate.find(['?', '#']).unwrap_or(candidate.len()));
    }

    if !valid_slug(&candidate) {
        return Err(Error::Configuration(
            "table code contains unsupported characters".to_owned(),
        ));
    }
    Ok(candidate)
}

fn valid_slug(slug: &str) -> bool {
    let bytes = slug.as_bytes();
    (1..=128).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn percent_decode(segment: &str) -> Result<String> {
    let mut output = Vec::with_capacity(segment.len());
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(Error::Configuration(
                    "table URL contains invalid percent encoding".to_owned(),
                ));
            }
            let high = hex(bytes[index + 1])?;
            let low = hex(bytes[index + 2])?;
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output)
        .map_err(|_| Error::Configuration("table code is not valid UTF-8".to_owned()))
}

fn hex(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::Configuration(
            "table URL contains invalid percent encoding".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_supported_table_code_forms() {
        assert_eq!(
            normalize_table_code("quick-nuts-raise").unwrap(),
            "quick-nuts-raise"
        );
        assert_eq!(
            normalize_table_code("/play/quick-nuts-raise").unwrap(),
            "quick-nuts-raise"
        );
        assert_eq!(
            normalize_table_code("https://www.gambit.com/play/quick-nuts-raise").unwrap(),
            "quick-nuts-raise"
        );
    }

    #[test]
    fn rejects_untrusted_or_malformed_values() {
        assert!(normalize_table_code("https://example.com/play/table").is_err());
        assert!(normalize_table_code("https://www.gambit.com/profile/table").is_err());
        assert!(normalize_table_code("-invalid-").is_err());
    }
}
