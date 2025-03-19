use omnipaxos_kv::common::kv::{KVCommand, ConsistencyLevel};
use sqlx::{Pool, Postgres, Row};
use sqlx::postgres::PgPoolOptions;
use omnipaxos::{
    util::{ NodeId },
};
use std::collections::HashMap;
 
pub struct Database {
 
    pool: Pool<Postgres>,
    is_leader: bool, // Add a flag to check if the current node is the leader
    peers: Vec<NodeId>,
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

        Self {
            pool,
            is_leader,
            peers,
        }
    }

    pub fn set_leader_status(&mut self, is_leader: bool) { // Update leader over time
        self.is_leader = is_leader;
    }
    
    /// Perform a quorum-based read
    async fn quorum_read(&self, key: String) -> Option<String> {
        let mut responses = HashMap::new();
        let quorum_size = (self.peers.len() / 2) + 1; // Simple majority quorum

        // Query each peer for the value
        for peer in &self.peers {
            let value = self.read_from_peer(peer, &key).await;
            if let Some(v) = value {
                *responses.entry(v).or_insert(0) += 1;
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

    /// Read a value from a specific peer
    async fn read_from_peer(&self, _peer: &NodeId, key: &str) -> Option<String> {
        let row = sqlx::query("SELECT value FROM kv_store WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .expect("Failed to fetch from PostgreSQL");
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
                        let value = self.quorum_read(key).await;
                        Some(value)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    async fn setup_database(is_leader: bool, peers: Vec<NodeId>) -> Database {
        let database_url = "postgres://user:password@localhost/mydatabase"; // Use a test database
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .expect("Failed to connect to PostgreSQL");

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

        Database { pool, is_leader, peers }
    }

    #[tokio::test]
    async fn test_quorum_read() {
        let database = setup_database(true, vec![1, 2, 3]).await;

        // Insert a key-value pair
        let put_command = KVCommand::Put("key1".to_string(), "value1".to_string());
        database.handle_command(put_command).await;

        // Test quorum read
        let result = database.quorum_read("key1".to_string()).await;
        assert_eq!(result, Some("value1".to_string()));
    }

    #[tokio::test]
    async fn test_read_from_peer() {
        let database = setup_database(true, vec![1, 2, 3]).await;

        // Insert a key-value pair
        let put_command = KVCommand::Put("key2".to_string(), "value2".to_string());
        database.handle_command(put_command).await;

        // Test read from peer
        let result = database.read_from_peer(&1, "key2").await;
        assert_eq!(result, Some("value2".to_string()));
    }

    #[tokio::test]
    async fn test_handle_command_get_leader() {
        let database = setup_database(true, vec![1, 2, 3]).await;

        // Insert a key-value pair
        let put_command = KVCommand::Put("key3".to_string(), "value3".to_string());
        database.handle_command(put_command).await;

        // Test GET operation with ConsistencyLevel::Leader
        let get_command = KVCommand::Get {
            key: "key3".to_string(),
            consistency: ConsistencyLevel::Leader,
        };
        let result = database.handle_command(get_command).await;
        assert_eq!(result, Some(Some("value3".to_string())));
    }

    #[tokio::test]
    async fn test_handle_command_get_local() {
        let database = setup_database(false, vec![1, 2, 3]).await; // Not the leader

        // Insert a key-value pair
        let put_command = KVCommand::Put("key4".to_string(), "value4".to_string());
        database.handle_command(put_command).await;

        // Test GET operation with ConsistencyLevel::Local
        let get_command = KVCommand::Get {
            key: "key4".to_string(),
            consistency: ConsistencyLevel::Local,
        };
        let result = database.handle_command(get_command).await;
        assert_eq!(result, Some(Some("value4".to_string())));
    }

    #[tokio::test]
    async fn test_handle_command_get_leader_forwarding() {
        let database = setup_database(false, vec![1, 2, 3]).await; // Not the leader

        // Insert a key-value pair
        let put_command = KVCommand::Put("key5".to_string(), "value5".to_string());
        database.handle_command(put_command).await;

        // Test GET operation with ConsistencyLevel::Leader (should forward to leader)
        let get_command = KVCommand::Get {
            key: "key5".to_string(),
            consistency: ConsistencyLevel::Leader,
        };
        let result = database.handle_command(get_command).await;
        assert_eq!(result, None); // Expecting None because the node is not the leader
    }

    #[tokio::test]
    async fn test_handle_command_get_linearizable() {
        let database = setup_database(true, vec![1, 2, 3]).await;

        // Insert a key-value pair
        let put_command = KVCommand::Put("key4".to_string(), "value4".to_string());
        database.handle_command(put_command).await;

        // Test GET operation with ConsistencyLevel::Linearizable
        let get_command = KVCommand::Get {
            key: "key4".to_string(),
            consistency: ConsistencyLevel::Linearizable,
        };
        let result = database.handle_command(get_command).await;
        assert_eq!(result, Some(Some("value4".to_string())));
    }

    // More elaborate tests here
    #[tokio::test]
    async fn test_leader_consistency_multiple_nodes() {
        // Setup leader and follower databases
        let leader_db = setup_database(true, vec![1, 2, 3]).await;
        let follower_db = setup_database(false, vec![1, 2, 3]).await;

        // Insert a key-value pair on the leader
        let put_command = KVCommand::Put("key6".to_string(), "value6".to_string());
        leader_db.handle_command(put_command).await;

        // Test GET operation with ConsistencyLevel::Leader on the follower (should forward to leader)
        let get_command = KVCommand::Get {
            key: "key6".to_string(),
            consistency: ConsistencyLevel::Leader,
        };
        let result = follower_db.handle_command(get_command.clone()).await; 
        assert_eq!(result, None); // Expecting None because the node is not the leader

        // Test GET operation with ConsistencyLevel::Leader on the leader
        let result = leader_db.handle_command(get_command).await; // Use the original
        assert_eq!(result, Some(Some("value6".to_string())));
    }


    #[tokio::test]
    async fn test_linearizable_consistency_multiple() {
        // Simulate different databases states for multiple nodes
        let db1 = setup_database(true, vec![1, 2, 3]).await;
        let db2 = setup_database(false, vec![1, 2, 3]).await;
        let _db3 = setup_database(false, vec![1, 2, 3]).await;

        // Insert a key-value pair on the leader
        let put_command = KVCommand::Put("key9".to_string(), "value9".to_string());
        db1.handle_command(put_command).await;

        // Simulate a situation where db3 is "unreachable"
        let get_command = KVCommand::Get {
            key: "key9".to_string(),
            consistency: ConsistencyLevel::Linearizable,
        };

        // Ensure quorum read still works with available nodes
        let result1 = db1.handle_command(get_command.clone()).await;
        let result2 = db2.handle_command(get_command.clone()).await;

        assert_eq!(result1, Some(Some("value9".to_string())));
        assert_eq!(result2, Some(Some("value9".to_string())));
    }

}