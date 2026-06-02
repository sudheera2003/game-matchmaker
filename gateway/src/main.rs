use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures::{sink::SinkExt, stream::StreamExt}; // Allows splitting the socket
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc}; // mpsc = Multi-Producer, Single-Consumer channels

#[derive(Deserialize, Serialize, Debug)]
struct ClientMessage {
    action: String,
    player_id: String,
}

// global player registry
// map the player's ID to a "Sender" pipe.
// use Arc and Mutex to make it thread-safe.
type PlayerRegistry = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<String>>>>;

#[derive(Clone)]
struct AppState {
    redis_client: redis::Client,
    players: PlayerRegistry,
}

#[tokio::main]
async fn main() {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_client = redis::Client::open(redis_url).unwrap();

    // initialize the empty, locked dictionary
    let players: PlayerRegistry = Arc::new(Mutex::new(HashMap::new()));

    let state = AppState {
        redis_client: redis_client.clone(),
        players: players.clone(),
    };

    // spawn the Background Radio Listener
    tokio::spawn(listen_for_matches(redis_client.clone(), players.clone()));

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Gateway running. Connected to Redis. Global Registry active.");
    axum::serve(listener, app).await.unwrap();
}

// the Redis Pub/Sub Listener
async fn listen_for_matches(client: redis::Client, players: PlayerRegistry) {
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("match_events").await.unwrap();
    let mut stream = pubsub.on_message();

    println!("Background Task: Listening for 'match_events' broadcast from Go...");

    while let Some(msg) = stream.next().await {
        let payload: String = msg.get_payload().unwrap();

        // When Go sends a message, parse it to find the player_id
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&payload) {
            if let Some(player_id) = parsed.get("player_id").and_then(|v| v.as_str()) {
                // Lock the dictionary, look for the player, and send them the message
                let map = players.lock().await;
                if let Some(sender) = map.get(player_id) {
                    let _ = sender.send(payload.clone());
                }
            }
        }
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    // Split the WebSocket into a sender and a receiver
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Create an internal pipe (mpsc channel) for this specific player
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Background Task: Read from the internal pipe and push to the actual WebSocket
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let mut current_player_id = String::new();
    let mut redis_conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    // the main loop: Listen to the player
    while let Some(Ok(msg)) = ws_receiver.next().await {
        if let Message::Text(text) = msg {
            if let Ok(parsed_data) = serde_json::from_str::<ClientMessage>(&text) {
                // Register the player in the Global Registry on their first message
                if current_player_id.is_empty() {
                    current_player_id = parsed_data.player_id.clone();

                    // Lock the Mutex, insert the player's pipe, and unlock it instantly
                    let mut map = state.players.lock().await;
                    map.insert(current_player_id.clone(), tx.clone());
                    println!("Memory Lock: Registered {} internally.", current_player_id);
                }

                // Push to the Redis Queue for Go
                let _: () = redis_conn.lpush("player_queue", &text).await.unwrap();
            }
        }
    }

    // If the loop breaks (player closes the tab), remove them from memory
    if !current_player_id.is_empty() {
        let mut map = state.players.lock().await;
        map.remove(&current_player_id);
        println!(
            "Memory Lock: Player {} disconnected. Removed from memory.",
            current_player_id
        );
    }
}
