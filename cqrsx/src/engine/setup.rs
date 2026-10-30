//! What a module gets when the engine is built: its variables of the environment and the settings of the engine.
use crate::{
    engine::settings::{Settings, duration_text, variable},
    locking::{get_mut, lock},
};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt::Display,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;

/// Where the variables of the environment come from: a lookup by the variable's full name.
pub(crate) type Lookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// What a module root gets: its variables under the application's prefix ([`env`](Self::env)) and the engine's
/// settings ([`engine`](Self::engine)). No shared resources: a module makes what it needs and provides it.
pub struct Setup {
    prefix: String,
    lookup: Lookup,
    settings: Settings,
    /// What the modules read and described, by key: `Cqrs::describe` lists it next to the engine's settings.
    described: Mutex<BTreeMap<String, String>>,
}

/// A variable of a module that cannot be read, or a resource the module could not make.
#[derive(Debug, Error)]
pub enum SetupError {
    #[error("{variable}: {message}")]
    Invalid { variable: String, message: String },
    #[error("{0}")]
    Failed(Box<dyn Error + Send + Sync>),
}

impl SetupError {
    /// A resource the module could not make, e.g. a client of its own.
    pub fn failed(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Failed(Box::new(error))
    }
}

impl Setup {
    /// The setup of a module over `lookup` under `prefix`, in an engine with `settings`: what the engine's builder
    /// gives every module, and a test of a module root on its own.
    pub fn new(
        prefix: impl Into<String>,
        lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
        settings: Settings,
    ) -> Self {
        Self::over(prefix.into(), Arc::new(lookup), settings)
    }

    pub(crate) fn over(prefix: String, lookup: Lookup, settings: Settings) -> Self {
        Self {
            prefix,
            lookup,
            settings,
            described: Mutex::default(),
        }
    }

    /// The variable `name` under the prefix (`ENCORE_TICKET_LIMIT` for `TICKET_LIMIT`), trimmed and parsed; `default`
    /// when it is not set or blank. A value that does not parse names the variable. The value the module gets is
    /// described under the variable's name (`Cqrs::describe`).
    pub fn env<T: FromStr + Display>(&self, name: &str, default: T) -> Result<T, SetupError>
    where
        T::Err: Display,
    {
        let value = self.read(name, default)?;
        self.describe(self.variable(name), value.to_string());
        Ok(value)
    }

    /// The variable `name` as milliseconds; `default_ms` when it is not set. Described as a duration (`1 s`, `80 ms`).
    pub fn millis(&self, name: &str, default_ms: u64) -> Result<Duration, SetupError> {
        let value = Duration::from_millis(self.read(name, default_ms)?);
        self.describe(self.variable(name), duration_text(value));
        Ok(value)
    }

    /// The name of the variable `name` under the prefix.
    pub fn variable(&self, name: &str) -> String {
        variable(&self.prefix, name)
    }

    /// A value of the variable `name` the module refuses, e.g. `must be positive`.
    pub fn invalid(&self, name: &str, message: impl Into<String>) -> SetupError {
        SetupError::Invalid {
            variable: self.variable(name),
            message: message.into(),
        }
    }

    /// A setting of the module with no variable of its own (derived from others) in the engine's description, under a
    /// dotted name in lower case (`mail.request_timeout`); a key described twice keeps the last value.
    pub fn describe(&self, key: impl Into<String>, value: impl Into<String>) {
        let mut described = lock(&self.described, "setup descriptions");
        described.insert(key.into(), value.into());
    }

    /// What the modules read and described, sorted by key.
    pub(crate) fn described(mut self) -> Vec<(String, String)> {
        std::mem::take(get_mut(&mut self.described, "setup descriptions"))
            .into_iter()
            .collect()
    }

    fn read<T: FromStr>(&self, name: &str, default: T) -> Result<T, SetupError>
    where
        T::Err: Display,
    {
        let variable = self.variable(name);
        match (self.lookup)(&variable).map(|value| value.trim().to_owned()) {
            Some(value) if !value.is_empty() => value.parse().map_err(|error: T::Err| SetupError::Invalid {
                message: format!("cannot parse {value:?}: {error}"),
                variable,
            }),
            _ => Ok(default),
        }
    }

    /// The settings of the engine the module is built into.
    pub fn engine(&self) -> &Settings {
        &self.settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::settings::DEFAULT_SNAPSHOT_EVERY;
    use std::collections::HashMap;

    fn setup(prefix: &str, variables: &[(&str, &str)]) -> Setup {
        let variables: HashMap<String, String> = variables
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        Setup::new(prefix, move |name| variables.get(name).cloned(), Settings::default())
    }

    #[test]
    fn a_variable_is_read_under_the_prefix_and_a_blank_one_is_unset() {
        let encore = setup(
            "ENCORE",
            &[("ENCORE_TICKET_LIMIT", " 7 "), ("ENCORE_BLANK", "  "), ("TICKET_LIMIT", "9")],
        );
        assert_eq!(encore.env("TICKET_LIMIT", 4).unwrap(), 7);
        assert_eq!(encore.env("BLANK", 4).unwrap(), 4);
        assert_eq!(encore.env("ABSENT", String::from("http://mail")).unwrap(), "http://mail");
        let bare = setup("", &[("TICKET_LIMIT", "9"), ("RETRY_MS", "250")]);
        assert_eq!(bare.env("TICKET_LIMIT", 4).unwrap(), 9, "no prefix: the name as it is");
        assert_eq!(bare.millis("RETRY_MS", 1000).unwrap(), Duration::from_millis(250));
        assert_eq!(bare.millis("ABSENT_MS", 1000).unwrap(), Duration::from_secs(1));
        assert_eq!(bare.engine().snapshot_every(), DEFAULT_SNAPSHOT_EVERY);
    }

    #[test]
    fn an_unreadable_value_names_its_variable() {
        let encore = setup("ENCORE", &[("ENCORE_TICKET_LIMIT", "four")]);
        let error = r#"ENCORE_TICKET_LIMIT: cannot parse "four": invalid digit found in string"#;
        assert_eq!(encore.env::<usize>("TICKET_LIMIT", 4).unwrap_err().to_string(), error);
        assert_eq!(encore.millis("TICKET_LIMIT", 1).unwrap_err().to_string(), error);
        let failed = SetupError::failed(std::io::Error::other("no client"));
        assert_eq!(failed.to_string(), "no client");
    }

    #[test]
    fn a_module_records_what_it_reads_and_names_its_refusals() {
        let encore = setup(
            "ENCORE",
            &[
                ("ENCORE_TICKET_LIMIT", "7"),
                ("ENCORE_RETRY_MS", "1500"),
                ("ENCORE_URL", "http://mail/"),
            ],
        );
        assert_eq!(encore.env("TICKET_LIMIT", 4).unwrap(), 7);
        assert_eq!(encore.millis("RETRY_MS", 1000).unwrap(), Duration::from_millis(1500));
        assert_eq!(encore.millis("PACE_MS", 1000).unwrap(), Duration::from_secs(1));
        assert_eq!(encore.env("URL", String::new()).unwrap(), "http://mail/");
        encore.describe("mail.request_timeout", "4 s");
        encore.describe("mail.request_timeout", "3 s");
        assert_eq!(encore.variable("TICKET_LIMIT"), "ENCORE_TICKET_LIMIT");
        let refused = encore.invalid("TICKET_LIMIT", "must be positive");
        assert_eq!(refused.to_string(), "ENCORE_TICKET_LIMIT: must be positive");
        assert!(matches!(refused, SetupError::Invalid { ref variable, .. } if variable == "ENCORE_TICKET_LIMIT"));
        assert_eq!(
            encore.described(),
            [
                ("ENCORE_PACE_MS", "1 s"),
                ("ENCORE_RETRY_MS", "1500 ms"),
                ("ENCORE_TICKET_LIMIT", "7"),
                ("ENCORE_URL", "http://mail/"),
                ("mail.request_timeout", "3 s"),
            ]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
        );
    }
}
