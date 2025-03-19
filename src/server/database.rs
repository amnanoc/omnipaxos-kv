use omnipaxos_kv::common::kv::{KVCommand, ConsistencyLevel};
use sqlx::{Pool, Postgres, Row};
use sqlx::postgres::PgPoolOptions;
use omnipaxos::util::NodeId;
use std::collections::HashMap;

pub struct Database {
    pool: Pool<Postgres>, // Local database connection
    is_leader: bool,     // Flag to check if the current node is the leader
    peers: Vec<NodeId>,  // List of peer node IDs
    peer_database_urls: HashMap<NodeId, String>, // Map of peer node IDs to their database URLs
}

impl Database {
    /// Initializes the database and connects to PostgreSQL
    pub async fn new(is_leader: bool, peers: Vec<NodeId>) -> Self {
        let postgres_host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "postgres".to_string());
        let postgres_db = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "mydatabase".to_string());

        let database_url = format!(
            "postgres://user:password@{}/{}",
            postgres_host, postgres_db
        );

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
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

        // Hardcoded peer database URLs
        let peer_database_urls = peers
            .iter()
            .map(|&peer| {
                let db_name = match peer {
                    1 => "mydatabase_s1",
                    2 => "mydatabase_s2",
                    3 => "mydatabase_s3",
                    _ => panic!("Unknown peer ID"),
                };
                let url = format!("postgres://user:password@postgres-{}/{}", db_name, db_name);
                (peer, url)
            })
            .collect();

        Self {
            pool,
            is_leader,
            peers,
            peer_database_urls,
        }
    }

    pub fn set_leader_status(&mut self, is_leader: bool) {
        self.is_leader = is_leader;
    }

    /// Perform a quorum-based read
    async fn quorum_read(&self, key: String) -> Option<String> {
        let mut responses = HashMap::new();
        let quorum_size = (self.peers.len() / 2) + 1; // Simple majority quorum

        // Query each peer for the value
        for peer in &self.peers {
            if let Some(value) = self.read_from_peer(peer, &key).await {
                *responses.entry(value).or_insert(0) += 1;
            }
        }

        // Find the value with a quorum
        for (value, count) in responses {
            if count >= quorum_size {
                return Some(value);
            }
        }

        None // No quorum reached
    }

    /// Read a value from a specific peer's database
    async fn read_from_peer(&self, peer: &NodeId, key: &str) -> Option<String> {
        let database_url = self.peer_database_urls.get(peer).expect("Invalid peer ID");

        let pool = PgPoolOptions::new()
            .max_connections(1) 
            .connect(database_url)
            .await
            .expect("Failed to connect to peer PostgreSQL");

        let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
            .bind(key)
            .fetch_optional(&pool)
            .await
            .expect("Failed to fetch from peer PostgreSQL");

        row.map(|r| r.get::<String, _>(0))
    }

    /// Handles a command (Put, Get, Delete) and interacts with PostgreSQL
    pub async fn handle_command(&self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                let _result = sqlx::query("INSERT INTO kv_store (key, value) VALUES ($1, $2) 
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
                    ConsistencyLevel::Leader => {
                        if self.is_leader {
                            let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
                                .bind(key)
                                .fetch_optional(&self.pool)
                                .await
                                .expect("Failed to fetch from PostgreSQL");
                            Some(row.map(|r| r.get::<String, _>(0)))
                        } else {
                            println!("Forwarding to leader...");
                            None // Indicating forward
                        }
                    }
                    ConsistencyLevel::Local => {
                        let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
                            .bind(key)
                            .fetch_optional(&self.pool)
                            .await
                            .expect("Failed to fetch from PostgreSQL");
                        Some(row.map(|r| r.get::<String, _>(0)))
                    }
                    ConsistencyLevel::Linearizable => {
                        let value = self.quorum_read(key).await;
                        Some(value)
                    }
                }
            }
        }
    }
}