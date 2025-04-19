use axum::{
    extract::{ws::{WebSocketUpgrade, WebSocket, Message}, ConnectInfo, State},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use fluvio::{Fluvio, Offset};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{sync::{broadcast, Mutex}, task};
use tower_http::cors::CorsLayer;
use uuid::Uuid;
use futures_util::{StreamExt, SinkExt};

const TOPIC_NAME: &str = "demo-topic-1";
const WEBHOOK_URL: &str = "https://infinyon.cloud/webhooks/v1/LHac7AZWw8oQ6xGTd7hyUjx8RhM7B3SA3doPSxG4vQxr1zbeZzYuiWoKJOZMxQDf";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EditEvent {
    doc_id: String,
    user_id: String,
    operation: String,
    position: usize,
    character: Option<String>,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    Edit(EditEvent),
    Typing { doc_id: String, user_id: String, is_typing: bool },
    Cursor { doc_id: String, user_id: String, position: usize },
    Join { doc_id: String, user_id: String },
    Leave { doc_id: String, user_id: String },
}

#[derive(Clone)]
struct AppState {
    fluvio: Fluvio,
    tx: broadcast::Sender<EditEvent>,
    rooms: Arc<Mutex<HashMap<String, Vec<String>>>>, // doc_id -> user_ids
}

#[tokio::main]
async fn main() {
    let fluvio = Fluvio::connect().await.expect("Failed to connect to Fluvio");
    let (tx, _) = broadcast::channel(100);
    let state = Arc::new(AppState {
        fluvio,
        tx,
        rooms: Arc::new(Mutex::new(HashMap::new())),
    });

    let consumer_state = state.clone();
    task::spawn(async move {
        consume_and_forward(consumer_state).await;
    });

    let app = Router::new()
        .route("/send", post(handle_send))
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Server running at http://{}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_send(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EditEvent>,
) -> impl IntoResponse {
    let producer = state
        .fluvio
        .topic_producer(TOPIC_NAME)
        .await
        .expect("Failed to get producer");

    let json = serde_json::to_string(&payload).unwrap();
    let _ = producer.send(&payload.doc_id, json).await;

    "Message sent"
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    println!("New WebSocket connection from {}", addr);
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.tx.subscribe();
    let id = Uuid::new_v4().to_string();
    println!("WebSocket {} connected", id);

    let mut current_doc = String::new();
    let mut user_id = String::new();

    loop {
        tokio::select! {
            Some(Ok(Message::Text(msg))) = socket.next() => {
                if let Ok(message) = serde_json::from_str::<ClientMessage>(&msg) {
                    match message {
                        ClientMessage::Edit(edit) => {
                            let producer = state.fluvio.topic_producer(TOPIC_NAME).await.unwrap();
                            let json = serde_json::to_string(&edit).unwrap();
                            let _ = producer.send(&edit.doc_id, json).await;
                        },
                        ClientMessage::Typing { doc_id, user_id, is_typing } => {
                            let indicator = format!("{{\"type\": \"typing\", \"user_id\": \"{}\", \"is_typing\": {}}}", user_id, is_typing);
                            let _ = socket.send(Message::Text(indicator)).await;
                        },
                        ClientMessage::Cursor { doc_id, user_id, position } => {
                            let cursor = format!("{{\"type\": \"cursor\", \"user_id\": \"{}\", \"position\": {}}}", user_id, position);
                            let _ = socket.send(Message::Text(cursor)).await;
                        },
                        ClientMessage::Join { doc_id, user_id: uid } => {
                            current_doc = doc_id.clone();
                            user_id = uid.clone();
                            let mut rooms = state.rooms.lock().await;
                            rooms.entry(doc_id.clone()).or_default().push(uid.clone());
                            println!("User {} joined {}", uid, doc_id);
                        },
                        ClientMessage::Leave { doc_id, user_id: uid } => {
                            let mut rooms = state.rooms.lock().await;
                            if let Some(users) = rooms.get_mut(&doc_id) {
                                users.retain(|u| u != &uid);
                            }
                            println!("User {} left {}", uid, doc_id);
                        }
                    }
                }
            },
            Ok(event) = rx.recv() => {
                let json = serde_json::to_string(&event).unwrap();
                if socket.send(Message::Text(json)).await.is_err() {
                    println!("WS {}: send failed, disconnecting", id);
                    break;
                }
            }
        }
    }

    // On disconnect
    if !current_doc.is_empty() && !user_id.is_empty() {
        let mut rooms = state.rooms.lock().await;
        if let Some(users) = rooms.get_mut(&current_doc) {
            users.retain(|u| u != &user_id);
        }
        println!("User {} disconnected from {}", user_id, current_doc);
    }
}

async fn consume_and_forward(state: Arc<AppState>) {
    let consumer = state
        .fluvio
        .partition_consumer(TOPIC_NAME, 0)
        .await
        .expect("Consumer error");

    let mut stream = consumer.stream(Offset::beginning()).await.unwrap();
    println!("Listening to Fluvio topic...");

    while let Some(Ok(record)) = stream.next().await {
        let value = record.value_string().unwrap();
        if let Ok(event) = serde_json::from_str::<EditEvent>(&value) {
            println!("Forwarding: {}", value);
            let _ = forward_to_webhook(&event).await;
            let _ = state.tx.send(event);
        }
    }
}

async fn forward_to_webhook(event: &EditEvent) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client.post(WEBHOOK_URL).json(&event).send().await?;
    if !res.status().is_success() {
        println!("Webhook failed with status: {}", res.status());
    }
    Ok(())
}
