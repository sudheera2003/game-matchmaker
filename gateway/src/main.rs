use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

#[derive(Deserialize, Serialize, Debug)]
struct ClientMessage {
    action: String,
    player_id: String,
}

// Application State
// Axum will share a clone of this with every new player connection.
#[derive(Clone)]
struct AppState {
    redis_client: redis::Client,
}

#[tokio::main]
async fn main() {
    // Connect to local Docker Redis
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let state = AppState { redis_client };

    // Pass the state to Router
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state); // Injecting Redis into the app

    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Gateway running. Connected to Redis.");
    axum::serve(listener, app).await.unwrap();
}

// The Handler to extract the state
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

// the Game Loop pushes to Redis
async fn handle_socket(mut socket: WebSocket, state: AppState) {
    println!("A player connected!");

    // Open a connection to Redis for this specific socket
    let mut redis_conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            match serde_json::from_str::<ClientMessage>(&text) {
                Ok(parsed_data) => {
                    println!("Queueing player: {}", parsed_data.player_id);

                    // Push the raw JSON string directly into a Redis List named 'player_queue'
                    // LPUSH
                    let _: () = redis_conn.lpush("player_queue", &text).await.unwrap();

                    let response = format!(
                        r#"{{"status": "queued", "id": "{}"}}"#,
                        parsed_data.player_id
                    );
                    let _ = socket.send(Message::Text(response)).await;
                }
                Err(_) => {
                    let _ = socket
                        .send(Message::Text(r#"{"status": "error"}"#.to_string()))
                        .await;
                }
            }
        }
    }
}
