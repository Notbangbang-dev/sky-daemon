//! Provisions per-user MariaDB/MySQL databases on the node's local database
//! server. The panel generates and stores the database name, user, and
//! password; this module just runs the DDL over an admin connection and reports
//! back the public endpoint clients should connect to. It is only constructed
//! when the operator has set `SKY_DB_ADMIN_*` (see `config::DatabaseConfig`).

use anyhow::{bail, Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};

use crate::config::DatabaseConfig;

pub struct DbAdmin {
    pool: Pool,
    public_host: String,
    public_port: u16,
}

impl DbAdmin {
    pub fn new(cfg: &DatabaseConfig) -> Self {
        let opts = OptsBuilder::default()
            .ip_or_hostname(cfg.admin_host.clone())
            .tcp_port(cfg.admin_port)
            .user(Some(cfg.admin_user.clone()))
            .pass(Some(cfg.admin_password.clone()));
        Self {
            pool: Pool::new(opts),
            public_host: cfg.public_host.clone(),
            public_port: cfg.public_port,
        }
    }

    pub fn public_host(&self) -> &str {
        &self.public_host
    }

    pub fn public_port(&self) -> u16 {
        self.public_port
    }

    /// Creates the database, a user scoped to it, and grants. Idempotent: uses
    /// IF NOT EXISTS so a retried command doesn't error.
    pub async fn create_database(&self, name: &str, user: &str, password: &str) -> Result<()> {
        validate_ident(name, 64).context("invalid database name")?;
        validate_ident(user, 32).context("invalid database user")?;
        let pass = escape_sql_string(password);

        let mut conn = self.pool.get_conn().await.context("connect to MariaDB")?;
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{name}`"))
            .await
            .context("create database")?;

        // Create the user + grant; if either fails, compensate by dropping the
        // database (and user) we just created so a partial failure doesn't
        // strand an empty, untracked database on the node. DDL is
        // auto-committing in MariaDB, so an explicit rollback is the only option.
        let rest = async {
            conn.query_drop(format!(
                "CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{pass}'"
            ))
            .await
            .context("create user")?;
            conn.query_drop(format!(
                "GRANT ALL PRIVILEGES ON `{name}`.* TO '{user}'@'%'"
            ))
            .await
            .context("grant privileges")?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = rest {
            let _ = conn
                .query_drop(format!("DROP DATABASE IF EXISTS `{name}`"))
                .await;
            let _ = conn
                .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
                .await;
            return Err(e);
        }

        conn.query_drop("FLUSH PRIVILEGES").await.ok();
        Ok(())
    }

    /// Drops the database and its user. Idempotent: uses IF EXISTS.
    pub async fn delete_database(&self, name: &str, user: &str) -> Result<()> {
        validate_ident(name, 64).context("invalid database name")?;
        validate_ident(user, 32).context("invalid database user")?;

        let mut conn = self.pool.get_conn().await.context("connect to MariaDB")?;
        conn.query_drop(format!("DROP DATABASE IF EXISTS `{name}`"))
            .await
            .context("drop database")?;
        conn.query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
            .await
            .ok();
        conn.query_drop("FLUSH PRIVILEGES").await.ok();
        Ok(())
    }
}

/// Defence-in-depth: the panel already generates these from a safe charset, but
/// the daemon re-checks before interpolating into DDL so a malformed value can
/// never break out of the identifier.
fn validate_ident(s: &str, max: usize) -> Result<()> {
    if s.is_empty() || s.len() > max {
        bail!("identifier length must be 1..={max}");
    }
    if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("identifier may only contain [A-Za-z0-9_]");
    }
    Ok(())
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_ident_accepts_safe_names() {
        assert!(validate_ident("s_ab12_mydb", 64).is_ok());
        assert!(validate_ident("u_deadbeef", 32).is_ok());
    }

    #[test]
    fn validate_ident_rejects_injection() {
        assert!(validate_ident("db`; DROP DATABASE x;--", 64).is_err());
        assert!(validate_ident("bad name", 64).is_err());
        assert!(validate_ident("", 64).is_err());
        assert!(validate_ident(&"x".repeat(65), 64).is_err());
    }

    #[test]
    fn escape_sql_string_neutralizes_quotes() {
        assert_eq!(escape_sql_string("a'b"), "a\\'b");
        assert_eq!(escape_sql_string("a\\b"), "a\\\\b");
    }
}
