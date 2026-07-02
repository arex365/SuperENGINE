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

async fn get_recommended_coins(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let symbols = match trade_manager::fetch_active_perps().await {
        Ok(s) => s,
        Err(e) => return Json(json!({"error": e})).into_response(),
    };

    // filter to top 100 by volume (approximate — the ticker list is already sorted)
    let candidates: Vec<String> = symbols.into_iter().take(100).collect();

    let (sl_pct, tp_pct, _, _) = state.engine.get_config();

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
                    score = engine::RealtimeEngine::score_symbol_grid_with_config(&klines, sl_pct, tp_pct);
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
async fn config_page(State(state): State<Arc<AppState>>) -> Html<String> {
    let (sl, tp, rev, ghost) = state.engine.get_config();
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Config — Grid Engine</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:30px; max-width:700px; margin:0 auto }}
  h1,h2 {{ color:#f0f6fc; margin:24px 0 12px }}
  a {{ color:#58a6ff; text-decoration:none }} a:hover {{ text-decoration:underline }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:20px; margin:16px 0 }}
  .card h3 {{ margin:0 0 16px }}
  form {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap }}
  label {{ color:#8b949e; font-size:0.9em }}
  input[type=number] {{ padding:8px 12px; border-radius:6px; border:1px solid #30363d; background:#21262d; color:#c9d1d9; font-size:0.95em; width:80px }}
  button {{ padding:8px 16px; border-radius:6px; border:1px solid #30363d; background:#21262d; color:#c9d1d9; cursor:pointer; font-weight:600 }}
  button:hover {{ background:#30363d }}
  .btn-green {{ border-color:#4caf50; color:#4caf50 }}
  .btn-red {{ border-color:#ef5350; color:#ef5350 }}
  .nav {{ display:flex; gap:16px; margin-bottom:20px }}
  .nav a {{ padding:6px 0; border-bottom:2px solid transparent }}
  .nav a.active {{ border-color:#58a6ff }}
  .current {{ background:#0d1117; border:1px solid #30363d; border-radius:6px; padding:14px 18px; margin:12px 0 }}
  .current .row {{ display:flex; justify-content:space-between; padding:6px 0 }}
  .current .val {{ color:#f0f6fc; font-weight:600 }}
  .toggle {{ display:flex; align-items:center; gap:10px }}
  .toggle input {{ width:20px; height:20px; cursor:pointer }}
</style>
</head>
<body>
<div class="nav">
  <a href="/">Dashboard</a>
  <a href="/subs">Manage</a>
  <a href="/config" class="active">Config</a>
  <a href="/state">State</a>
</div>
<h1>Engine Configuration</h1>

<div class="current">
  <div class="row"><span>Stop Loss</span><span class="val">{sl}%</span></div>
  <div class="row"><span>Take Profit</span><span class="val">{tp}%</span></div>
  <div class="row"><span>Reverse Mode</span><span class="val">{rev}</span></div>
  <div class="row"><span>Ghost Trades</span><span class="val">{ghost}</span></div>
</div>

<div class="card">
  <h3>Set SL / TP Percentages</h3>
  <form action="/setConfig" method="get">
    <label>SL %:</label><input type="number" name="sl" step="0.1" min="0.1" value="{sl}">
    <label>TP %:</label><input type="number" name="tp" step="0.1" min="0.1" value="{tp}">
    <button type="submit" class="btn-green">Apply</button>
  </form>
</div>

<div class="card">
  <h3>Reverse Mode</h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    When enabled, entry direction is inverted: a long signal becomes short and vice versa.
  </p>
  <div class="toggle">
    <form action="/setReverse" method="get">
      <input type="hidden" name="enabled" value="{toggle_to}">
      <button type="submit" class="{btn_cls}">{btn_txt}</button>
    </form>
    <span style="color:#8b949e;font-size:0.85em">{rev_status}</span>
  </div>
</div>

<div class="card">
  <h3>Ghost Mode</h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    After a real loss, the next N trades are paper-traded (ghost) to avoid revenge trading.
    Set to 0 to disable ghost mode entirely.
  </p>
  <form action="/setGhost" method="get">
    <label>Ghost trades after loss:</label><input type="number" name="val" min="0" max="50" value="{ghost}" style="width:70px">
    <button type="submit" class="btn-green">Apply</button>
  </form>
</div>

<p><a href="/">← Back to dashboard</a></p>
</body></html>"#,
        sl = sl,
        tp = tp,
        rev = if rev { "ON" } else { "OFF" },
        toggle_to = if rev { "false" } else { "true" },
        btn_cls = if rev { "btn-red" } else { "btn-green" },
        btn_txt = if rev { "Disable Reverse" } else { "Enable Reverse" },
        rev_status = if rev { "Reverse mode is active — entry directions are inverted." } else { "Normal mode — standard mean-reversion logic." },
        ghost = ghost,
    );
    Html(html)
}

async fn set_config_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let sl: f64 = params.get("sl").and_then(|s| s.parse().ok()).unwrap_or(1.5);
    let tp: f64 = params.get("tp").and_then(|s| s.parse().ok()).unwrap_or(0.5);
    state.engine.set_config(sl.clamp(0.1, 50.0), tp.clamp(0.1, 50.0));
    Html(state.engine.generate_dashboard())
}

async fn set_reverse_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let enabled = params.get("enabled").map(|s| s == "true").unwrap_or(false);
    state.engine.set_reverse_mode(enabled);
    Html(state.engine.generate_dashboard())
}

async fn set_ghost_route(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let val: i32 = params.get("val").and_then(|s| s.parse().ok()).unwrap_or(0).max(0);
    state.engine.set_ghost_threshold(val);
    Html(state.engine.generate_dashboard())
}

async fn ping(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let trackers = state.engine.all_trackers();
    let mut data = serde_json::Map::new();
    for (sym, tr) in &trackers {
        let mut m = serde_json::Map::new();
        m.insert("current_price".to_string(), json!(tr.current_price));
        m.insert(
            "in_position".to_string(),
            json!(!tr.active_trades.is_empty()),
        );
        m.insert("active_trades".to_string(), json!(tr.active_trades.len()));
        m.insert("klines".to_string(), json!(tr.klines.len()));
        m.insert("closed_trades".to_string(), json!(tr.closed_trades.len()));
        if let Some(t) = tr.active_trades.first() {
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
        .route("/config", get(config_page))
        .route("/config/", get(config_page))
        .route("/setConfig", get(set_config_route))
        .route("/setConfig/", get(set_config_route))
        .route("/setReverse", get(set_reverse_route))
        .route("/setReverse/", get(set_reverse_route))
        .route("/setGhost", get(set_ghost_route))
        .route("/setGhost/", get(set_ghost_route))
        .route("/getRecommendedCoins", get(get_recommended_coins))
        .route("/ping", get(ping))
        .with_state(app_state);

    let addr = format!("127.0.0.1:{port}");
    println!("Starting Grid Trading Engine on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
