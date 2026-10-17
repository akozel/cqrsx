//! DEMO ONLY: the lifecycle of the demo data. The back-end is a plain production-style service and
//! knows nothing about "default concerts" or "resetting everything"; both live here.
//!
//! * `seed` announces the default concerts through the back-end's **public** API — ordinary
//!   `AnnounceConcert` commands, exactly what the announce dialog of the UI sends. A repeated
//!   announce is rejected by rule R2 (`already_announced`) and that is the idempotency.
//! * `reset` empties the ledger, the snapshots and the outbox (role `encore_demo_admin`, which may
//!   do nothing but TRUNCATE those three tables), drops the read models and the mail inbox, then
//!   seeds again. The back-end keeps running meanwhile: a task a worker holds at that moment ends
//!   with a harmless "row not found" in its log.
use mongodb::Client;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DefaultConcert {
    pub concert_id: &'static str,
    pub artist: &'static str,
    pub city: &'static str,
    pub date: &'static str,
}

/// `concert_id` = `<artist-slug>-<yyyy-mm-dd>`, the same rule as the announce dialog of the UI.
pub const DEFAULT_CONCERTS: [DefaultConcert; 3] = [
    DefaultConcert {
        concert_id: "radiohead-2026-11-21",
        artist: "Radiohead",
        city: "Minsk",
        date: "2026-11-21",
    },
    DefaultConcert {
        concert_id: "rammstein-2026-12-05",
        artist: "Rammstein",
        city: "Tokyo",
        date: "2026-12-05",
    },
    DefaultConcert {
        concert_id: "muse-2027-01-16",
        artist: "Muse",
        city: "Tbilisi",
        date: "2027-01-16",
    },
];

pub const TRUNCATE_SQL: &str = "TRUNCATE TABLE events, snapshots, transactional_outbox RESTART IDENTITY";
/// `(logical database, collection)` dropped by a reset: the read models and the mail inbox.
pub const DROPPED: [(&str, &str); 3] = [
    ("encore_read", "concert_halls"),
    ("encore_read", "fan_wallets"),
    ("mail_service", "inbox"),
];

#[derive(Debug, Error)]
pub enum DemoError {
    #[error("postgres: {0}")]
    Postgres(#[from] sqlx::Error),
    #[error("mongodb: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("back-end: {0}")]
    Backend(#[from] reqwest::Error),
    #[error("back-end answered {status} to AnnounceConcert {concert_id}: {body}")]
    Rejected {
        concert_id: &'static str,
        status: u16,
        body: String,
    },
}

/// Outcome of one announce: accepted now, or already there (rule R2).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Announced {
    pub concert_id: &'static str,
    pub outcome: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ResetReport {
    pub truncated: Vec<&'static str>,
    pub dropped: Vec<String>,
    pub announced: Vec<Announced>,
}

#[derive(Clone)]
pub struct DemoAdmin {
    admin: PgPool,
    mongo: Client,
    read_db: String,
    mail_db: String,
    backend_url: String,
    http: reqwest::Client,
}

impl DemoAdmin {
    pub fn new(admin: PgConnectOptions, mongo: Client, read_db: String, mail_db: String, backend_url: String) -> Self {
        Self {
            admin: sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(3))
                .connect_lazy_with(admin),
            mongo,
            read_db,
            mail_db,
            backend_url: backend_url.trim_end_matches('/').to_owned(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    fn database_name(&self, logical: &str) -> String {
        match logical {
            "encore_read" => self.read_db.clone(),
            "mail_service" => self.mail_db.clone(),
            other => other.to_owned(),
        }
    }

    /// The command the UI's announce dialog would send for this concert.
    pub fn announce_command(concert: &DefaultConcert) -> Value {
        json!({ "type": "AnnounceConcert", "artist": concert.artist, "city": concert.city, "date": concert.date })
    }

    async fn announce(&self, concert: &DefaultConcert) -> Result<Announced, DemoError> {
        let url = format!("{}/api/concerts/{}/commands", self.backend_url, concert.concert_id);
        let response = self.http.post(url).json(&Self::announce_command(concert)).send().await?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        match (status.as_u16(), body.get("outcome").and_then(Value::as_str)) {
            (200, Some("accepted")) => Ok(Announced {
                concert_id: concert.concert_id,
                outcome: "announced",
            }),
            (422, _) if body.pointer("/error/code").and_then(Value::as_str) == Some("already_announced") => Ok(Announced {
                concert_id: concert.concert_id,
                outcome: "already_announced",
            }),
            _ => Err(DemoError::Rejected {
                concert_id: concert.concert_id,
                status: status.as_u16(),
                body: body.to_string(),
            }),
        }
    }

    /// Announces every default concert; stops at the first error.
    pub async fn seed(&self) -> Result<Vec<Announced>, DemoError> {
        let mut out = Vec::with_capacity(DEFAULT_CONCERTS.len());
        for concert in &DEFAULT_CONCERTS {
            out.push(self.announce(concert).await?);
        }
        Ok(out)
    }

    /// Seeds at start-up: the back-end may still be starting, so retry for a while.
    pub async fn seed_with_retries(&self, attempts: u32, every: Duration) {
        for attempt in 1..=attempts {
            match self.seed().await {
                Ok(announced) => {
                    for a in &announced {
                        tracing::info!("seed: {} {}", a.concert_id, a.outcome);
                    }
                    return;
                }
                Err(error) if attempt < attempts => {
                    tracing::debug!("seed attempt {attempt}/{attempts} failed: {error}");
                    tokio::time::sleep(every).await;
                }
                Err(error) => tracing::warn!("seed gave up after {attempts} attempts: {error}"),
            }
        }
    }

    /// Empties everything and announces the default concerts again.
    pub async fn reset(&self) -> Result<ResetReport, DemoError> {
        sqlx::query(TRUNCATE_SQL).execute(&self.admin).await?;
        let mut dropped = Vec::with_capacity(DROPPED.len());
        for (database, collection) in DROPPED {
            self.mongo
                .database(&self.database_name(database))
                .collection::<mongodb::bson::Document>(collection)
                .drop()
                .await?;
            dropped.push(format!("{database}.{collection}"));
        }
        let announced = self.seed().await?;
        Ok(ResetReport {
            truncated: vec!["events", "snapshots", "transactional_outbox"],
            dropped,
            announced,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_concert_ids_follow_the_artist_date_rule() {
        for c in &DEFAULT_CONCERTS {
            let slug = c.artist.to_lowercase().replace(|ch: char| !ch.is_ascii_alphanumeric(), "-");
            assert_eq!(c.concert_id, format!("{slug}-{}", c.date));
            assert!(
                c.concert_id
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
            );
        }
        let ids: std::collections::BTreeSet<_> = DEFAULT_CONCERTS.iter().map(|c| c.concert_id).collect();
        assert_eq!(ids.len(), DEFAULT_CONCERTS.len());
    }

    #[test]
    fn the_announce_command_is_what_the_ui_sends() {
        let cmd = DemoAdmin::announce_command(&DEFAULT_CONCERTS[0]);
        assert_eq!(cmd["type"], "AnnounceConcert");
        assert_eq!(cmd["artist"], "Radiohead");
        assert_eq!(cmd["city"], "Minsk");
        assert_eq!(cmd["date"], "2026-11-21");
    }

    #[test]
    fn a_reset_touches_exactly_the_demo_tables_and_collections() {
        assert!(TRUNCATE_SQL.starts_with("TRUNCATE TABLE events, snapshots, transactional_outbox"));
        assert_eq!(DROPPED, crate::mongo::WHITELIST);
    }
}
