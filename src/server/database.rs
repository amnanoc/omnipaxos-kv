use omnipaxos_kv::common::kv::KVCommand;
use sqlx::{Pool, Postgres, Row};
use sqlx::postgres::PgPoolOptions;

pub struct Database {
    pool: Pool<Postgres>,
}

impl Database {
    /// Initializes the database and connects to PostgreSQL
    pub async fn new() -> Self {
        let database_url = "postgres://user:password@postgres/mydatabase";
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .expect("Failed to connect to PostgreSQL");

        println!("✅ Successfully connected to PostgreSQL!");

        // Ensure table exists
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );"
        )
        .execute(&pool)
        .await
        .expect("Failed to create table");

        Self { pool }
    }

    /// Handles a command (Put, Get, Delete) and interacts with PostgreSQL
    pub async fn handle_command(&self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                let result = sqlx::query("INSERT INTO kv_store (key, value) VALUES ($1, $2) 
                                          ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;")
                    .bind(&key)
                    .bind(&value)
                    .execute(&self.pool)
                    .await
                    .expect("Failed to insert into PostgreSQL");
                None
            }
            KVCommand::Delete(key) => {
                sqlx::query("DELETE FROM kv_store WHERE key = $1")
                    .bind(key)
                    .execute(&self.pool)
                    .await
                    .expect("Failed to delete from PostgreSQL");
                None
            }
            KVCommand::Get{ key, consistency } => {
                //  Leader Reads: Always read from the leader node, get the most recent data
                //  Local Reads: Read from any available node, potentially returning stale data
                //  Linearizable Reads: Ensure reads reflect the latest committed state across all nodes
                let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await
                    .expect("Failed to fetch from PostgreSQL");
                
                Some(row.map(|r| r.get::<String, _>(0)))
            }
        }
    }
}
