use omnipaxos_kv::common::kv::{KVCommand, ConsistencyLevel};
use omnipaxos_kv::server::database::Database; 
use std::sync::Arc;
use tokio::sync::Mutex;

/// Helper function to create a test database instance
async fn create_test_database(is_leader: bool, peers: Vec<u64>) -> Database {
    let database_url = "postgres://user:password@postgres/mydatabase";
    let pool = sqlx::PgPoolOptions::new()
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

    Database {
        pool,
        is_leader,
        peers,
    }
}

/// Test writing and reading with leader consistency
#[tokio::test]
async fn test_write_and_read_leader_consistency() {
    let db = Arc::new(Mutex::new(create_test_database(true, vec![1, 2, 3]).await));

    // Write a value
    let write_command = KVCommand::Put("key1".to_string(), "value1".to_string());
    db.lock().await.handle_command(write_command).await;

    // Read with leader consistency
    let read_command = KVCommand::Get {
        key: "key1".to_string(),
        consistency: ConsistencyLevel::Leader,
    };
    let result = db.lock().await.handle_command(read_command).await;

    assert_eq!(result, Some(Some("value1".to_string())));
}

/// Test writing and reading with local consistency
#[tokio::test]
async fn test_write_and_read_local_consistency() {
    let db = Arc::new(Mutex::new(create_test_database(false, vec![1, 2, 3]).await));

    // Write a value
    let write_command = KVCommand::Put("key2".to_string(), "value2".to_string());
    db.lock().await.handle_command(write_command).await;

    // Read with local consistency
    let read_command = KVCommand::Get {
        key: "key2".to_string(),
        consistency: ConsistencyLevel::Local,
    };
    let result = db.lock().await.handle_command(read_command).await;

    // Local reads may return None or stale data
    assert!(result == Some(Some("value2".to_string())) || result == None);
}

/// Test writing and reading with linearizable consistency
#[tokio::test]
async fn test_write_and_read_linearizable_consistency() {
    let db = Arc::new(Mutex::new(create_test_database(false, vec![1, 2, 3]).await));

    // Write a value
    let write_command = KVCommand::Put("key3".to_string(), "value3".to_string());
    db.lock().await.handle_command(write_command).await;

    // Read with linearizable consistency
    let read_command = KVCommand::Get {
        key: "key3".to_string(),
        consistency: ConsistencyLevel::Linearizable,
    };
    let result = db.lock().await.handle_command(read_command).await;

    assert_eq!(result, Some(Some("value3".to_string())));
}