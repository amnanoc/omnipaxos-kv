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
            let (host, db_name) = match peer {
                1 => ("postgres-s1", "mydatabase_s1"),
                2 => ("postgres-s2", "mydatabase_s2"),
                3 => ("postgres-s3", "mydatabase_s3"),
                _ => panic!("Unknown peer ID"),
            };
            let url = format!("postgres://user:password@{}/{}", host, db_name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    use std::env;
    use std::sync::Once;

    // Initialize test environment once
    static INIT: Once = Once::new();
    
    fn initialize() {
        INIT.call_once(|| {
            // Set environment variables for testing
            env::set_var("POSTGRES_HOST", "localhost");
            env::set_var("POSTGRES_DB", "test_db");
            
            // You could also set up a test logger here
            env_logger::init();
        });
    }

    // Mock version of Database for testing
    struct MockDatabase {
        is_leader: bool,
        storage: HashMap<String, String>,
        peers: Vec<NodeId>,
        peer_storage: HashMap<NodeId, HashMap<String, String>>,
    }

    impl MockDatabase {
        fn new(is_leader: bool, peers: Vec<NodeId>) -> Self {
            let mut peer_storage = HashMap::new();
            for &peer in &peers {
                peer_storage.insert(peer, HashMap::new());
            }
            
            Self {
                is_leader,
                storage: HashMap::new(),
                peers,
                peer_storage,
            }
        }

        fn set_leader_status(&mut self, is_leader: bool) {
            self.is_leader = is_leader;
        }

        async fn handle_command(&mut self, command: KVCommand) -> Option<Option<String>> {
            match command {
                KVCommand::Put(key, value) => {
                    self.storage.insert(key, value);
                    None
                },
                KVCommand::Delete(key) => {
                    self.storage.remove(&key);
                    None
                },
                KVCommand::Get { key, consistency } => {
                    match consistency {
                        ConsistencyLevel::Leader => {
                            if self.is_leader {
                                Some(self.storage.get(&key).cloned())
                            } else {
                                None // Indicating forward to leader
                            }
                        },
                        ConsistencyLevel::Local => {
                            Some(self.storage.get(&key).cloned())
                        },
                        ConsistencyLevel::Linearizable => {
                            // Simulate quorum read
                            let mut values = HashMap::new();
                            
                            // Count this node's value
                            if let Some(value) = self.storage.get(&key) {
                                *values.entry(value.clone()).or_insert(0) += 1;
                            }
                            
                            // Count peer values
                            for (&peer_id, peer_data) in &self.peer_storage {
                                if let Some(value) = peer_data.get(&key) {
                                    *values.entry(value.clone()).or_insert(0) += 1;
                                }
                            }
                            
                            // Find value with quorum
                            let quorum_size = (self.peers.len() / 2) + 1;
                            for (value, count) in values {
                                if count >= quorum_size {
                                    return Some(Some(value));
                                }
                            }
                            
                            Some(None) // No quorum
                        }
                    }
                }
            }
        }

        // Helper method to set peer data for testing
        fn set_peer_data(&mut self, peer_id: NodeId, key: &str, value: &str) {
            if let Some(peer_data) = self.peer_storage.get_mut(&peer_id) {
                peer_data.insert(key.to_string(), value.to_string());
            }
        }
    }

    #[tokio::test]
    async fn test_local_consistency() {
        initialize();
        
        // Create mock databases
        let peers = vec![1, 2, 3];
        let mut db1 = MockDatabase::new(true, peers.clone());  // Node 1 is leader
        let mut db2 = MockDatabase::new(false, peers.clone()); // Node 2 is follower
        let mut db3 = MockDatabase::new(false, peers.clone()); // Node 3 is follower
        
        // Insert data into db1 (leader)
        let put_cmd = KVCommand::Put("test_local_key".to_string(), "leader_value".to_string());
        db1.handle_command(put_cmd).await;
        
        // Insert different data into db2 (follower)
        let put_cmd = KVCommand::Put("test_local_key".to_string(), "follower_value".to_string());
        db2.handle_command(put_cmd).await;
        
        // Test local consistency on each node
        let get_cmd = KVCommand::Get {
            key: "test_local_key".to_string(),
            consistency: ConsistencyLevel::Local,
        };
        
        // Each node should return its own local value
        let result1 = db1.handle_command(get_cmd.clone()).await;
        let result2 = db2.handle_command(get_cmd.clone()).await;
        let result3 = db3.handle_command(get_cmd.clone()).await;
        
        assert_eq!(result1, Some(Some("leader_value".to_string())));
        assert_eq!(result2, Some(Some("follower_value".to_string())));
        assert_eq!(result3, Some(None)); // db3 doesn't have the key
    }

    #[tokio::test]
    async fn test_leader_consistency() {
        initialize();
        
        // Create mock databases
        let peers = vec![1, 2, 3];
        let mut db1 = MockDatabase::new(true, peers.clone());  // Node 1 is leader
        let mut db2 = MockDatabase::new(false, peers.clone()); // Node 2 is follower
        
        // Insert data into db1 (leader)
        let put_cmd = KVCommand::Put("test_leader_key".to_string(), "leader_value".to_string());
        db1.handle_command(put_cmd).await;
        
        // Insert different data into db2 (follower)
        let put_cmd = KVCommand::Put("test_leader_key".to_string(), "follower_value".to_string());
        db2.handle_command(put_cmd).await;
        
        // Test leader consistency
        let get_cmd = KVCommand::Get {
            key: "test_leader_key".to_string(),
            consistency: ConsistencyLevel::Leader,
        };
        
        // Leader should return its value
        let result1 = db1.handle_command(get_cmd.clone()).await;
        // Follower should return None (indicating forward to leader)
        let result2 = db2.handle_command(get_cmd.clone()).await;
        
        assert_eq!(result1, Some(Some("leader_value".to_string())));
        assert_eq!(result2, None); // Indicates forwarding to leader
    }

    #[tokio::test]
    async fn test_linearizable_consistency() {
        initialize();
        
        // Create mock databases
        let peers = vec![1, 2, 3];
        let mut db1 = MockDatabase::new(true, peers.clone());
        
        // Set up data for quorum test
        // db1 and db2 have "quorum_value", db3 has "minority_value"
        db1.storage.insert("test_quorum_key".to_string(), "quorum_value".to_string());
        db1.set_peer_data(2, "test_quorum_key", "quorum_value");
        db1.set_peer_data(3, "test_quorum_key", "minority_value");
        
        // Test linearizable consistency (quorum read)
        let get_cmd = KVCommand::Get {
            key: "test_quorum_key".to_string(),
            consistency: ConsistencyLevel::Linearizable,
        };
        
        // Should return the value that has quorum
        let result = db1.handle_command(get_cmd.clone()).await;
        assert_eq!(result, Some(Some("quorum_value".to_string())));
        
        // Now change db1's value and test again
        db1.storage.insert("test_quorum_key".to_string(), "new_value".to_string());
        
        // No quorum for any value now (1 for "new_value", 1 for "quorum_value", 1 for "minority_value")
        let result = db1.handle_command(get_cmd.clone()).await;
        assert_eq!(result, Some(None)); // No quorum reached
        
        // Create quorum for "new_value"
        db1.set_peer_data(3, "test_quorum_key", "new_value");
        
        // Now there should be quorum for "new_value"
        let result = db1.handle_command(get_cmd.clone()).await;
        assert_eq!(result, Some(Some("new_value".to_string())));
    }

    #[tokio::test]
    async fn test_leader_change() {
        initialize();
        
        // Create mock databases
        let peers = vec![1, 2, 3];
        let mut db1 = MockDatabase::new(true, peers.clone());  // Node 1 starts as leader
        let mut db2 = MockDatabase::new(false, peers.clone()); // Node 2 starts as follower
        
        // Insert data as leader
        let put_cmd = KVCommand::Put("leader_change_key".to_string(), "original_value".to_string());
        db1.handle_command(put_cmd).await;
        
        // Test leader consistency
        let get_cmd = KVCommand::Get {
            key: "leader_change_key".to_string(),
            consistency: ConsistencyLevel::Leader,
        };
        
        let result1 = db1.handle_command(get_cmd.clone()).await;
        let result2 = db2.handle_command(get_cmd.clone()).await;
        
        assert_eq!(result1, Some(Some("original_value".to_string())));
        assert_eq!(result2, None); // Indicates forwarding to leader
        
        // Change leadership
        db1.set_leader_status(false);
        db2.set_leader_status(true);
        
        // Copy data to db2 to simulate replication
        db2.storage.insert("leader_change_key".to_string(), "original_value".to_string());
        
        // Test again after leadership change
        let result1 = db1.handle_command(get_cmd.clone()).await;
        let result2 = db2.handle_command(get_cmd.clone()).await;
        
        assert_eq!(result1, None); // Now db1 forwards to leader
        assert_eq!(result2, Some(Some("original_value".to_string()))); // db2 is now leader
    }

    #[tokio::test]
    async fn test_delete_operation() {
        initialize();
        
        // Create mock database
        let peers = vec![1, 2, 3];
        let mut db = MockDatabase::new(true, peers.clone());
        
        // Insert data
        let put_cmd = KVCommand::Put("delete_test_key".to_string(), "test_value".to_string());
        db.handle_command(put_cmd).await;
        
        // Verify data exists
        let get_cmd = KVCommand::Get {
            key: "delete_test_key".to_string(),
            consistency: ConsistencyLevel::Local,
        };
        
        let result = db.handle_command(get_cmd.clone()).await;
        assert_eq!(result, Some(Some("test_value".to_string())));
        
        // Delete the data
        let delete_cmd = KVCommand::Delete("delete_test_key".to_string());
        db.handle_command(delete_cmd).await;
        
        // Verify data is deleted
        let result = db.handle_command(get_cmd).await;
        assert_eq!(result, Some(None));
    }
}
