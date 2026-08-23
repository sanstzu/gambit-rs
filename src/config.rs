use std::{env, fmt, fs, path::Path, time::Duration};

use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::{Error, Result};

/// Credentials used by Gambit's login endpoint.
#[derive(Clone)]
pub struct Credentials {
    email: SecretString,
    password: SecretString,
}

impl Credentials {
    #[must_use]
    pub fn new(email: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            email: SecretString::new(email.into()),
            password: SecretString::new(password.into()),
        }
    }

    pub(crate) fn email(&self) -> &str {
        self.email.expose_secret()
    }

    pub(crate) fn password(&self) -> &str {
        self.password.expose_secret()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Credentials { email: [REDACTED], password: [REDACTED] }")
    }
}

/// Network and timeout settings for a [`crate::GambitClient`].
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub http_origin: Url,
    pub websocket_url: Url,
    pub origin_header: String,
    pub request_timeout: Duration,
    pub websocket_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub timezone_offset: Option<i32>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            http_origin: Url::parse("https://www.gambit.com").expect("constant URL is valid"),
            websocket_url: Url::parse("wss://www.gambit.com/ws").expect("constant URL is valid"),
            origin_header: "https://www.gambit.com".to_owned(),
            request_timeout: Duration::from_secs(15),
            websocket_timeout: Duration::from_secs(15),
            heartbeat_interval: Duration::from_secs(10),
            timezone_offset: None,
        }
    }
}

impl ClientConfig {
    /// Rejects settings that cannot form a safe client connection.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.http_origin.scheme(), "http" | "https") {
            return Err(Error::Configuration(
                "HTTP origin must use http or https".to_owned(),
            ));
        }
        if !matches!(self.websocket_url.scheme(), "ws" | "wss") {
            return Err(Error::Configuration(
                "WebSocket URL must use ws or wss".to_owned(),
            ));
        }
        if self.http_origin.cannot_be_a_base() || self.http_origin.host_str().is_none() {
            return Err(Error::Configuration("HTTP origin is invalid".to_owned()));
        }
        if self.websocket_url.host_str().is_none() {
            return Err(Error::Configuration("WebSocket URL is invalid".to_owned()));
        }
        if self.origin_header.parse::<http::HeaderValue>().is_err() {
            return Err(Error::Configuration("Origin header is invalid".to_owned()));
        }
        if self.request_timeout.is_zero()
            || self.websocket_timeout.is_zero()
            || self.heartbeat_interval.is_zero()
        {
            return Err(Error::Configuration(
                "timeouts and heartbeat interval must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Loads credentials from process variables, filling missing values from a dotenv file.
///
/// Process values take precedence. Parsing is deliberately limited to `KEY=VALUE` lines and does
/// not modify the process environment.
pub fn load_credentials(dotenv_path: impl AsRef<Path>) -> Result<Credentials> {
    load_credentials_from(
        env::var("GAMBIT_USERNAME").ok(),
        env::var("GAMBIT_PASSWORD").ok(),
        dotenv_path.as_ref(),
    )
}

fn load_credentials_from(
    environment_email: Option<String>,
    environment_password: Option<String>,
    dotenv_path: &Path,
) -> Result<Credentials> {
    let dotenv = read_dotenv(dotenv_path)?;
    let email = nonempty(environment_email)
        .or_else(|| dotenv_value(&dotenv, "GAMBIT_USERNAME"))
        .ok_or_else(|| Error::Configuration("missing GAMBIT_USERNAME".to_owned()))?;
    let password = nonempty(environment_password)
        .or_else(|| dotenv_value(&dotenv, "GAMBIT_PASSWORD"))
        .ok_or_else(|| Error::Configuration("missing GAMBIT_PASSWORD".to_owned()))?;
    Ok(Credentials::new(email, password))
}

fn read_dotenv(path: &Path) -> Result<String> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(Error::Configuration(format!(
            "could not read credential file: {}",
            error.kind()
        ))),
    }
}

fn dotenv_value(contents: &str, name: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        if key.trim() != name {
            return None;
        }
        let value = value.trim();
        let unquoted = if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            &value[1..value.len() - 1]
        } else {
            value
        };
        nonempty(Some(unquoted.to_owned()))
    })
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_values_take_precedence() {
        let path = std::env::temp_dir().join(format!("gambit-config-{}.env", uuid::Uuid::new_v4()));
        fs::write(
            &path,
            "GAMBIT_USERNAME=dotenv@example.com\nGAMBIT_PASSWORD=dotenv-secret\n",
        )
        .unwrap();

        let credentials =
            load_credentials_from(Some("environment@example.com".to_owned()), None, &path).unwrap();
        fs::remove_file(path).unwrap();

        assert_eq!(credentials.email(), "environment@example.com");
        assert_eq!(credentials.password(), "dotenv-secret");
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credentials = Credentials::new("person@example.com", "secret-password");
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("person@example.com"));
        assert!(!rendered.contains("secret-password"));
    }
}
