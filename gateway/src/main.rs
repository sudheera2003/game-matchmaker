use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // router
    let app = Router::new().route("/ws", get(ws_handler));

    // bind the server to a local port
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Gateway running on ws://127.0.0.1:3000/ws");

    // start the server
    axum::serve(listener, app).await.unwrap();
}

// the route handler that upgrades an HTTP request to a WebSocket
async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

// the game loop for a connected player
async fn handle_socket(mut socket: WebSocket) {
    println!("A player connected!");

    // wait for messages from the player
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            println!("Received from player: {}", text);

            // echo the message back to prove it works
            let response = format!("Server received: {}", text);
            if socket.send(Message::Text(response)).await.is_err() {
                println!("Player disconnected.");
                break;
            }
        }
    }
}
