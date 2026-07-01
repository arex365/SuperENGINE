mod config;
mod engine;
mod trade_manager;

use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use engine::{RealtimeEngine, Side};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

struct AppState {
    engine: Arc<RealtimeEngine>,
}

// ─── Routes ───────────────────────────────────────────────────────────────

async fn dashboard(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(state.engine.generate_dashboard())
}

async fn subs_page(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(state.engine.generate_subs_page())
}

async fn state_page(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(state.engine.generate_state_page())
}

async fn add_long_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let symbol = params
        .get("symbol")
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_uppercase();
    if symbol.is_empty() {
        return Json(json!({"error": "No symbol provided"})).into_response();
    }
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    let ok = state.engine.add_tracker(&resolved, Side::Long);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok})).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn add_long_path(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    let ok = state.engine.add_tracker(&resolved, Side::Long);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok})).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn add_short_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let symbol = params
        .get("symbol")
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_uppercase();
    if symbol.is_empty() {
        return Json(json!({"error": "No symbol provided"})).into_response();
    }
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    let ok = state.engine.add_tracker(&resolved, Side::Short);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok})).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn add_short_path(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    let ok = state.engine.add_tracker(&resolved, Side::Short);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok})).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn subscribe_long(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let sub_id: i32 = params
        .get("subID")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let size: f64 = params.get("size").and_then(|s| s.parse().ok()).unwrap_or(5.0);
    if sub_id < 0 {
        return Json(json!({"error": "Invalid subID"})).into_response();
    }
    state.engine.add_subscriber(sub_id, Side::Long, size);
    if params.get("redirect").map(|s| s == "subs").unwrap_or(false) {
        return Html(state.engine.generate_subs_page()).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn subscribe_short(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let sub_id: i32 = params
        .get("subID")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let size: f64 = params.get("size").and_then(|s| s.parse().ok()).unwrap_or(5.0);
    if sub_id < 0 {
        return Json(json!({"error": "Invalid subID"})).into_response();
    }
    state.engine.add_subscriber(sub_id, Side::Short, size);
    if params.get("redirect").map(|s| s == "subs").unwrap_or(false) {
        return Html(state.engine.generate_subs_page()).into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn remove_subscriber(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let sub_id: i32 = params
        .get("subID")
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);
    if sub_id >= 0 {
        state.engine.remove_subscriber(sub_id);
        println!("[Dashboard] Removed subscriber #{sub_id}");
    }
    Html(state.engine.generate_dashboard())
}

async fn remove_symbol(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    state.engine.remove_tracker(&symbol.to_uppercase());
    Html(state.engine.generate_dashboard())
}

async fn clear_all(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.engine.trackers.write().unwrap().clear();
    state.engine.subscribers.write().unwrap().clear();
    Html(
        "<html><body style=\"font-family:monospace;background:#0d1117;color:#c9d1d9;padding:20px\">\
         <h2>All trackers & subscriptions cleared</h2><a href=\"/\">Back</a></body></html>",
    )
}

async fn flush(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.engine.trackers.write().unwrap().clear();
    state.engine.subscribers.write().unwrap().clear();
    Html(
        "<html><body style=\"font-family:monospace;background:#0d1117;color:#c9d1d9;padding:20px\">\
         <h2>All trackers & subscriptions cleared. WS will reconnect.</h2><a href=\"/\">Back</a></body></html>",
    )
}

async fn view_trades(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    match state.engine.generate_view_trades_page(&resolved) {
        Some(html) => Html(html).into_response(),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            format!("<h2>Symbol {resolved} not found</h2><a href=\"/\">Back</a>"),
        )
            .into_response(),
    }
}

async fn ping(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let trackers = state.engine.all_trackers();
    let mut data = serde_json::Map::new();
    for (sym, tr) in &trackers {
        let mut m = serde_json::Map::new();
        m.insert("direction".to_string(), json!(tr.direction.upper()));
        m.insert("entry_price".to_string(), json!(tr.entry_price));
        m.insert("current_price".to_string(), json!(tr.current_price));
        m.insert("in_position".to_string(), json!(tr.in_position));
        m.insert("klines".to_string(), json!(tr.klines.len()));
        m.insert("closed_trades".to_string(), json!(tr.closed_trades.len()));
        data.insert(sym.to_string(), json!(m));
    }
    Json(json!(data))
}

// ─── Main ─────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let port = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5555);

    let engine = RealtimeEngine::new();

    let ws_engine = engine.clone();
    tokio::spawn(async move {
        ws_engine.run_ws().await;
    });

    let app_state = Arc::new(AppState { engine });

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/subs", get(subs_page))
        .route("/subs/", get(subs_page))
        .route("/state", get(state_page))
        .route("/state/", get(state_page))
        .route("/addLong", get(add_long_route))
        .route("/addLong/", get(add_long_route))
        .route("/addLong/:symbol", get(add_long_path))
        .route("/addShort", get(add_short_route))
        .route("/addShort/", get(add_short_route))
        .route("/addShort/:symbol", get(add_short_path))
        .route("/subscribeLong", get(subscribe_long))
        .route("/subscribeLong/", get(subscribe_long))
        .route("/subscribeShort", get(subscribe_short))
        .route("/subscribeShort/", get(subscribe_short))
        .route("/removeSubscriber", get(remove_subscriber))
        .route("/removeSubscriber/", get(remove_subscriber))
        .route("/removeSymbol/:symbol", get(remove_symbol))
        .route("/removeSymbol/:symbol/", get(remove_symbol))
        .route("/clear", get(clear_all))
        .route("/clear/", get(clear_all))
        .route("/flush", get(flush))
        .route("/flush/", get(flush))
        .route("/viewtrades/:symbol", get(view_trades))
        .route("/viewtrades/:symbol/", get(view_trades))
        .route("/ping", get(ping))
        .with_state(app_state);

    let addr = format!("0.0.0.0:{port}");
    println!("Starting HFT Engine on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
