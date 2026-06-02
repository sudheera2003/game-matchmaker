use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

// define the incoming message struct
#[derive(Deserialize, Serialize, Debug)]
struct ClientMessage {
    action: String,
    player_id: String,
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/ws", get(ws_handler));
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Gateway running on ws://127.0.0.1:3000/ws");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    println!("A player connected!");

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            // parse the JSON string into Rust Struct
            match serde_json::from_str::<ClientMessage>(&text) {
                Ok(parsed_data) => {
                    println!(
                        "Player {} wants to {}",
                        parsed_data.player_id, parsed_data.action
                    );

                    // send a JSON response back
                    let response = format!(
                        r#"{{"status": "success", "message": "Queueing {}"}}"#,
                        parsed_data.player_id
                    );
                    let _ = socket.send(Message::Text(response)).await;
                }
                Err(e) => {
                    // the client sent bad JSON
                    println!("Failed to parse JSON: {}", e);
                    let _ = socket
                        .send(Message::Text(
                            r#"{"status": "error", "message": "Invalid format"}"#.to_string(),
                        ))
                        .await;
                }
            }
        }
    }
}
