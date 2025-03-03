use sqlx::{PgPool, Row}; 
use omnipaxos_kv::common::kv::KVCommand;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn handle_command(&self, command: KVCommand) -> Result<Option<Option<String>>, sqlx::Error> {
        match command {
            KVCommand::Put(key, value) => {
                let query = "INSERT INTO kv_store (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2";
                sqlx::query(query)
                    .bind(&key)
                    .bind(&value)
                    .execute(&self.pool)
                    .await?;
                Ok(None)
            }
            KVCommand::Delete(key) => {
                let query = "DELETE FROM kv_store WHERE key = $1";
                sqlx::query(query)
                    .bind(&key)
                    .execute(&self.pool)
                    .await?;
                Ok(None)
            }
            KVCommand::Get(key) => {
                let query = "SELECT value FROM kv_store WHERE key = $1";
                let row = sqlx::query(query)
                    .bind(&key)
                    .fetch_optional(&self.pool)
                    .await?;
                Ok(Some(row.map(|r| r.get::<String, _>("value"))))
            }
            KVCommand::SQLQuery(query) => {
                let result = sqlx::query(&query)
                    .fetch_all(&self.pool)
                    .await?;
                let result_str = result
                    .get(0)
                    .and_then(|row| row.try_get::<String, _>(0).ok());
                Ok(Some(result_str))
            }
        }
    }
}