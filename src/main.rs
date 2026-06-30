mod config;
mod engine;
mod trade_manager;

use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use engine::RealtimeEngine;
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

async fn watch_route(
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
    let ok = state.engine.add_tracker(&resolved);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok}))
            .into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn watch_path(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let resolved = RealtimeEngine::resolve_symbol(&symbol);
    let ok = state.engine.add_tracker(&resolved);
    if params.get("format").map(|s| s == "json").unwrap_or(false) {
        return Json(json!({"symbol": resolved, "original": symbol, "added": ok}))
            .into_response();
    }
    Html(state.engine.generate_dashboard()).into_response()
}

async fn subscribe_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let sub_id: i32 = params
        .get("subID")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let size: f64 = params
        .get("size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(5.0);
    if sub_id < 0 {
        return Json(json!({"error": "Invalid subID"})).into_response();
    }
    state.engine.add_subscriber(sub_id, size);
    if params
        .get("redirect")
        .map(|s| s == "subs")
        .unwrap_or(false)
    {
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

async fn get_recommended_coins(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    let symbols = match trade_manager::fetch_active_perps().await {
        Ok(s) => s,
        Err(e) => return Json(json!({"error": e})).into_response(),
    };

    // filter to top 100 by volume (approximate — the ticker list is already sorted)
    let candidates: Vec<String> = symbols.into_iter().take(100).collect();

    let mut handles = Vec::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));

    for sym in candidates {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let sym_clone = sym.clone();
        let handle = tokio::spawn(async move {
            let _permit = permit;
            let mut score = engine::GridScore {
                symbol: sym_clone.clone(),
                base_price: 0.0,
                price_range_pct: 0.0,
                level_crosses: 0,
                oscillation_score: 0.0,
                total_trades: 0,
                wins: 0,
                losses: 0,
                total_pnl_pct: 0.0,
                win_rate: 0.0,
                suitability: 0.0,
            };
            match trade_manager::fetch_historical_klines(&sym_clone, 5, 576).await {
                Ok(klines) if klines.len() >= 50 => {
                    score = engine::RealtimeEngine::score_symbol_grid(&klines);
                    score.symbol = sym_clone;
                }
                _ => {}
            }
            score
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for h in handles {
        match h.await {
            Ok(score) => results.push(score),
            Err(_) => {}
        }
    }

    results.sort_by(|a, b| b.suitability.partial_cmp(&a.suitability).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(20);

    Json(json!(results)).into_response()
}
async fn ping(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let trackers = state.engine.all_trackers();
    let mut data = serde_json::Map::new();
    for (sym, tr) in &trackers {
        let mut m = serde_json::Map::new();
        m.insert("base_price".to_string(), json!(tr.base_price));
        m.insert("current_price".to_string(), json!(tr.current_price));
        m.insert(
            "in_position".to_string(),
            json!(tr.active_trade.is_some()),
        );
        m.insert("klines".to_string(), json!(tr.klines.len()));
        m.insert("closed_trades".to_string(), json!(tr.closed_trades.len()));
        if let Some(ref t) = tr.active_trade {
            m.insert("active_level".to_string(), json!(t.level));
            m.insert("direction".to_string(), json!(t.direction.as_str()));
            m.insert("entry_price".to_string(), json!(t.entry_price));
            m.insert("sl".to_string(), json!(t.sl));
            m.insert("tp".to_string(), json!(t.tp));
        }
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
        .route("/watch", get(watch_route))
        .route("/watch/", get(watch_route))
        .route("/watch/:symbol", get(watch_path))
        .route("/subscribe", get(subscribe_route))
        .route("/subscribe/", get(subscribe_route))
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
        .route("/getRecommendedCoins", get(get_recommended_coins))
        .route("/ping", get(ping))
        .with_state(app_state);

    let addr = format!("127.0.0.1:{port}");
    println!("Starting Grid Trading Engine on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
