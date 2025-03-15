use omnipaxos_kv::common::kv::{KVCommand, ConsistencyLevel};
use omnipaxos_kv::common::messages::{ClientMessage, ServerMessage};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::Framed;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_leader_consistency() {
    // Connect to the leader server (s1)
    let stream = TcpStream::connect("s1:8000").await.unwrap();
    let transport = Framed::new(stream, Bincode::default());

    // Send a Put command
    let put_cmd = KVCommand::Put("test_key".to_string(), "test_value".to_string());
    let put_msg = ClientMessage::Append(1, put_cmd);
    transport.send(put_msg).await.unwrap();

    // Wait for replication
    sleep(Duration::from_secs(1)).await;

    // Send a Get command with Leader consistency
    let get_cmd = KVCommand::Get {
        key: "test_key".to_string(),
        consistency: ConsistencyLevel::Leader,
    };
    let get_msg = ClientMessage::Append(2, get_cmd);
    transport.send(get_msg).await.unwrap();

    // Receive the response
    let response: ServerMessage = transport.try_next().await.unwrap().unwrap();
    assert_eq!(response, ServerMessage::Read(2, Some("test_value".to_string())));
}

#[tokio::test]
async fn test_local_consistency() {
    // Connect to a non-leader server (s2)
    let stream = TcpStream::connect("s2:8000").await.unwrap();
    let transport = Framed::new(stream, Bincode::default());

    // Send a Get command with Local consistency
    let get_cmd = KVCommand::Get {
        key: "test_key".to_string(),
        consistency: ConsistencyLevel::Local,
    };
    let get_msg = ClientMessage::Append(3, get_cmd);
    transport.send(get_msg).await.unwrap();

    // Receive the response
    let response: ServerMessage = transport.try_next().await.unwrap().unwrap();
    println!("Local Get Response: {:?}", response);
}

#[tokio::test]
async fn test_linearizable_consistency() {
    // Connect to any server (s1)
    let stream = TcpStream::connect("s1:8000").await.unwrap();
    let transport = Framed::new(stream, Bincode::default());

    // Send a Get command with Linearizable consistency
    let get_cmd = KVCommand::Get {
        key: "test_key".to_string(),
        consistency: ConsistencyLevel::Linearizable,
    };
    let get_msg = ClientMessage::Append(4, get_cmd);
    transport.send(get_msg).await.unwrap();

    // Receive the response
    let response: ServerMessage = transport.try_next().await.unwrap().unwrap();
    assert_eq!(response, ServerMessage::Read(4, Some("test_value".to_string())));
}