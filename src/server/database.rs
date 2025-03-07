use omnipaxos_kv::common::kv::{KVCommand, ConsistencyLevel};
use sqlx::{Pool, Postgres, Row};
use sqlx::postgres::PgPoolOptions;

pub struct Database {
    pool: Pool<Postgres>,
    is_leader: bool,
}

impl Database {
    /// Initializes the database and connects to PostgreSQL
    pub async fn new(is_leader: bool) -> Self {
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

        Self {
            pool,
            is_leader,
        }
    }

    pub fn set_leader_status(&mut self, is_leader: bool) { // Update leader over time
        self.is_leader = is_leader;
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
            KVCommand::Get { key, consistency } => {
                match consistency {
                    ConsistencyLevel::Leader => { // Most up-to-date log, strong consistency
                        if self.is_leader {
                            let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
                            .bind(key)
                            .fetch_optional(&self.pool)
                            .await
                            .expect("Failed to fetch from PostgreSQL");

                            Some(row.map(|r| r.get::<String, _>(0)))
                        } else {
                            println!("Forwarding to leader..."); // Forwarding code in update_database_and_respond server.rs
                            None // Indicating forward
                        }
                    }                    
                    ConsistencyLevel::Local => { // Read from any node, no guaranteeing the latest committed data
                        let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
                            .bind(key)
                            .fetch_optional(&self.pool)
                            .await
                            .expect("Failed to fetch from PostgreSQL");
                        Some(row.map(|r| r.get::<String, _>(0)))
                    }
                    ConsistencyLevel::Linearizable => { // See the most recent committed write
                        // Ensure this node has applied all committed log entries
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
    }
}