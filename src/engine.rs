use crate::trade_manager;
use chrono::{DateTime, Duration, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ─── Types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TradeDirection {
    Long,
    Short,
}

impl TradeDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeDirection::Long => "long",
            TradeDirection::Short => "short",
        }
    }
    pub fn upper(&self) -> &'static str {
        match self {
            TradeDirection::Long => "LONG",
            TradeDirection::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone)]
pub struct KlineData {
    pub ts: i64,
    pub datetime: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub sub_id: i32,
    pub size: f64,
}

#[derive(Debug, Clone)]
pub struct GridTrade {
    pub symbol: String,
    pub level: i32,
    pub direction: TradeDirection,
    pub entry_time: DateTime<Utc>,
    pub entry_price: f64,
    pub sl: f64,
    pub tp: f64,
    pub exit_time: Option<DateTime<Utc>>,
    pub exit_price: Option<f64>,
    pub status: TradeStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradeStatus {
    Open,
    Closed,
}

impl GridTrade {
    fn pnl_pct(&self) -> f64 {
        if self.status == TradeStatus::Open || self.exit_price.is_none() {
            return 0.0;
        }
        let exit = self.exit_price.unwrap();
        match self.direction {
            TradeDirection::Long => (exit - self.entry_price) / self.entry_price * 100.0,
            TradeDirection::Short => (self.entry_price - exit) / self.entry_price * 100.0,
        }
    }

    fn duration(&self) -> Option<Duration> {
        self.exit_time.map(|t| t - self.entry_time)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GridScore {
    pub symbol: String,
    pub base_price: f64,
    pub price_range_pct: f64,
    pub level_crosses: u32,
    pub oscillation_score: f64,
    pub total_trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub total_pnl_pct: f64,
    pub win_rate: f64,
    pub suitability: f64,
}

// ─── SymbolTracker ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SymbolTracker {
    pub symbol: String,
    pub base_price: f64,
    pub level_step: f64,
    pub last_close_level: Option<i32>,
    pub klines: Vec<KlineData>,
    pub current_price: Option<f64>,
    pub active_trade: Option<GridTrade>,
    pub closed_trades: Vec<GridTrade>,
}

impl SymbolTracker {
    pub fn new(symbol: String, base_price: f64) -> Self {
        SymbolTracker {
            symbol,
            base_price,
            level_step: base_price * 0.005,
            last_close_level: None,
            klines: Vec::new(),
            current_price: Some(base_price),
            active_trade: None,
            closed_trades: Vec::new(),
        }
    }

    fn level_to_price(&self, level: i32) -> f64 {
        self.base_price * (1.0 + level as f64 * 0.005)
    }

    fn price_to_level(&self, price: f64) -> i32 {
        ((price / self.base_price - 1.0) / 0.005).round() as i32
    }

    pub fn total_attempts(&self) -> usize {
        self.closed_trades.len() + if self.active_trade.is_some() { 1 } else { 0 }
    }

    pub fn process_kline(&mut self, kline: &KlineData) -> Vec<TrackerEvent> {
        let close = kline.close;
        let high = kline.high;
        let low = kline.low;
        let dt = kline.datetime;

        self.klines.push(kline.clone());
        if self.klines.len() > 5000 {
            self.klines.remove(0);
        }
        self.current_price = Some(close);

        let mut events = Vec::new();

        let curr_level = self.price_to_level(close);
        let prev_level = self.last_close_level.unwrap_or(curr_level);
        self.last_close_level = Some(curr_level);

        // ── Check exit (SL / TP) ──────────────────────────────────
        if let Some(ref trade) = self.active_trade {
            let (should_exit, exit_price) = match trade.direction {
                TradeDirection::Long => {
                    if low <= trade.sl {
                        (true, low) // SL fill at worst price (low breached)
                    } else if high >= trade.tp {
                        (true, trade.tp) // TP limit fills at target
                    } else {
                        (false, 0.0)
                    }
                }
                TradeDirection::Short => {
                    if high >= trade.sl {
                        (true, high) // SL fill at worst price (high breached)
                    } else if low <= trade.tp {
                        (true, trade.tp) // TP limit fills at target
                    } else {
                        (false, 0.0)
                    }
                }
            };
            if should_exit {
                let mut t = self.active_trade.take().unwrap();
                t.exit_time = Some(dt);
                t.exit_price = Some(exit_price);
                t.status = TradeStatus::Closed;
                self.closed_trades.push(t.clone());
                events.push(TrackerEvent::Exited {
                    symbol: self.symbol.clone(),
                    direction: t.direction,
                    level: t.level,
                });
            }
        }

        // ── Check entry (level crossing, at most 1 active trade) ──
        if self.active_trade.is_none() && curr_level != prev_level {
            if curr_level > prev_level {
                // price moved up → enter SHORT (mean reversion)
                let entry_price = close;
                let sl = entry_price * 1.015; // -1.5%
                let tp = entry_price * 0.995; // +0.5%
                self.active_trade = Some(GridTrade {
                    symbol: self.symbol.clone(),
                    level: curr_level,
                    direction: TradeDirection::Short,
                    entry_time: dt,
                    entry_price,
                    sl,
                    tp,
                    exit_time: None,
                    exit_price: None,
                    status: TradeStatus::Open,
                });
                events.push(TrackerEvent::Entered {
                    symbol: self.symbol.clone(),
                    direction: TradeDirection::Short,
                    price: entry_price,
                    level: curr_level,
                    sl,
                    tp,
                });
            } else {
                // price moved down → enter LONG (mean reversion)
                let entry_price = close;
                let sl = entry_price * 0.985; // -1.5%
                let tp = entry_price * 1.005; // +0.5%
                self.active_trade = Some(GridTrade {
                    symbol: self.symbol.clone(),
                    level: curr_level,
                    direction: TradeDirection::Long,
                    entry_time: dt,
                    entry_price,
                    sl,
                    tp,
                    exit_time: None,
                    exit_price: None,
                    status: TradeStatus::Open,
                });
                events.push(TrackerEvent::Entered {
                    symbol: self.symbol.clone(),
                    direction: TradeDirection::Long,
                    price: entry_price,
                    level: curr_level,
                    sl,
                    tp,
                });
            }
        }

        events
    }

    pub fn update_ticker(&mut self, bid: f64, ask: f64) -> Vec<TrackerEvent> {
        self.current_price = Some((bid + ask) / 2.0); // mid for display
        let mut events = Vec::new();

        if let Some(ref trade) = self.active_trade {
            let (should_exit, exit_price) = match trade.direction {
                TradeDirection::Long => {
                    // LONG sells at bid
                    if bid <= trade.sl {
                        (true, bid)
                    } else if bid >= trade.tp {
                        (true, trade.tp)
                    } else {
                        (false, 0.0)
                    }
                }
                TradeDirection::Short => {
                    // SHORT buys back at ask
                    if ask >= trade.sl {
                        (true, ask)
                    } else if ask <= trade.tp {
                        (true, trade.tp)
                    } else {
                        (false, 0.0)
                    }
                }
            };
            if should_exit {
                let mut t = self.active_trade.take().unwrap();
                t.exit_time = Some(Utc::now());
                t.exit_price = Some(exit_price);
                t.status = TradeStatus::Closed;
                self.closed_trades.push(t.clone());
                events.push(TrackerEvent::Exited {
                    symbol: self.symbol.clone(),
                    direction: t.direction,
                    level: t.level,
                });
            }
        }

        // Entry on ticker (no active trade + direction from level change)
        if self.active_trade.is_none() {
            let mid = (bid + ask) / 2.0;
            let curr = self.price_to_level(mid);
            if let Some(prev) = self.last_close_level {
                if curr > prev {
                    // SHORT: sell at bid
                    let entry_price = bid;
                    let sl = entry_price * 1.015;
                    let tp = entry_price * 0.995;
                    self.active_trade = Some(GridTrade {
                        symbol: self.symbol.clone(),
                        level: curr,
                        direction: TradeDirection::Short,
                        entry_time: Utc::now(),
                        entry_price,
                        sl,
                        tp,
                        exit_time: None,
                        exit_price: None,
                        status: TradeStatus::Open,
                    });
                    events.push(TrackerEvent::Entered {
                        symbol: self.symbol.clone(),
                        direction: TradeDirection::Short,
                        price: entry_price,
                        level: curr,
                        sl,
                        tp,
                    });
                    self.last_close_level = Some(curr);
                } else if curr < prev {
                    // LONG: buy at ask
                    let entry_price = ask;
                    let sl = entry_price * 0.985;
                    let tp = entry_price * 1.005;
                    self.active_trade = Some(GridTrade {
                        symbol: self.symbol.clone(),
                        level: curr,
                        direction: TradeDirection::Long,
                        entry_time: Utc::now(),
                        entry_price,
                        sl,
                        tp,
                        exit_time: None,
                        exit_price: None,
                        status: TradeStatus::Open,
                    });
                    events.push(TrackerEvent::Entered {
                        symbol: self.symbol.clone(),
                        direction: TradeDirection::Long,
                        price: entry_price,
                        level: curr,
                        sl,
                        tp,
                    });
                    self.last_close_level = Some(curr);
                }
            } else {
                self.last_close_level = Some(curr);
            }
        }

        events
    }
}

// ─── Events ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TrackerEvent {
    Entered {
        symbol: String,
        direction: TradeDirection,
        price: f64,
        level: i32,
        sl: f64,
        tp: f64,
    },
    Exited {
        symbol: String,
        direction: TradeDirection,
        level: i32,
    },
}

// ─── WS Commands ──────────────────────────────────────────────────────────

pub enum WsCommand {
    Subscribe(String),
    Unsubscribe(String),
    ResubscribeAll(Vec<String>),
}

// ─── RealtimeEngine ───────────────────────────────────────────────────────

pub struct RealtimeEngine {
    pub trackers: RwLock<HashMap<String, SymbolTracker>>,
    pub subscribers: RwLock<HashMap<i32, Subscriber>>,
    pub ws_sender: Mutex<Option<mpsc::UnboundedSender<WsCommand>>>,
    pub running: AtomicBool,
}

impl RealtimeEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(RealtimeEngine {
            trackers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            ws_sender: Mutex::new(None),
            running: AtomicBool::new(true),
        })
    }

    pub fn add_tracker(self: &Arc<Self>, symbol: &str) -> bool {
        let symbol = symbol.to_uppercase();

        let price = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(trade_manager::fetch_ticker(&symbol))
                .ok()
        });

        let price = match price {
            Some(p) if p > 0.0 => p,
            _ => return false,
        };

        {
            let mut trackers = self.trackers.write().unwrap();
            if trackers.contains_key(&symbol) {
                return false;
            }
            let tr = SymbolTracker::new(symbol.clone(), price);
            trackers.insert(symbol.clone(), tr);
        }

        self.send_ws_cmd(WsCommand::Subscribe(symbol));
        true
    }

    pub fn remove_tracker(self: &Arc<Self>, symbol: &str) -> bool {
        let sym = symbol.to_uppercase();
        self.trackers.write().unwrap().remove(&sym);
        self.send_ws_cmd(WsCommand::Unsubscribe(sym));
        true
    }

    pub fn get_tracker(&self, symbol: &str) -> Option<SymbolTracker> {
        self.trackers.read().unwrap().get(symbol).cloned()
    }

    pub fn all_trackers(&self) -> Vec<(String, SymbolTracker)> {
        let trackers = self.trackers.read().unwrap();
        let mut v: Vec<_> = trackers.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn add_subscriber(&self, sub_id: i32, size: f64) -> bool {
        let mut subs = self.subscribers.write().unwrap();
        subs.insert(sub_id, Subscriber { sub_id, size });
        println!("[Engine] Subscriber #{sub_id} added: size {size} USDT");
        true
    }

    pub fn remove_subscriber(&self, sub_id: i32) -> bool {
        self.subscribers.write().unwrap().remove(&sub_id).is_some()
    }

    pub fn get_subscribers(&self) -> Vec<Subscriber> {
        let subs = self.subscribers.read().unwrap();
        let mut v: Vec<_> = subs.values().cloned().collect();
        v.sort_by(|a, b| a.sub_id.cmp(&b.sub_id));
        v
    }

    fn send_ws_cmd(&self, cmd: WsCommand) {
        let sender = self.ws_sender.lock().unwrap();
        if let Some(ref tx) = *sender {
            let _ = tx.send(cmd);
        }
    }

    fn on_enter_position(
        self: &Arc<Self>,
        symbol: String,
        direction: TradeDirection,
        price: f64,
        sl: f64,
        tp: f64,
    ) {
        let subs = {
            let subs = self.subscribers.read().unwrap();
            subs.values().cloned().collect::<Vec<_>>()
        };
        for sub in subs {
            let qty = if price > 0.0 { sub.size / price } else { sub.size };
            let sym = symbol.clone();
            let sid = sub.sub_id;
            match direction {
                TradeDirection::Long => trade_manager::open_long(sym, qty, sid, price, sl, tp),
                TradeDirection::Short => trade_manager::open_short(sym, qty, sid, price, sl, tp),
            }
        }
    }

    fn on_exit_position(self: &Arc<Self>, symbol: String, _direction: TradeDirection) {
        let subs = {
            let subs = self.subscribers.read().unwrap();
            subs.values().cloned().collect::<Vec<_>>()
        };
        for sub in subs {
            let sym = symbol.clone();
            let sid = sub.sub_id;
            trade_manager::cancel_old_orders(sym, sid);
        }
    }

    pub fn resolve_symbol(sym: &str) -> String {
        let s = sym.trim().to_uppercase();
        if s.contains('/') {
            return s
                .replace("/USDT:USDT", "")
                .replace("/USDT", "")
                .replace("/USDC:USDC", "")
                .replace("/USDC", "");
        }
        if s.ends_with("USDT") && s.len() > 4 {
            return s;
        }
        if s.ends_with("USDC") && s.len() > 4 {
            return s;
        }
        format!("{s}USDT")
    }

    // ─── WS Handling ──────────────────────────────────────────────────

    pub async fn handle_message(self: &Arc<Self>, raw: &str) {
        let msg: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(_) => return,
        };

        if msg.get("success").and_then(|s| s.as_bool()) == Some(false) {
            println!(
                "[WS] Subscribe error: {}",
                msg.get("ret_msg").and_then(|r| r.as_str()).unwrap_or("")
            );
            return;
        }

        let msg_type = msg.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if msg_type != "snapshot" && msg_type != "delta" {
            return;
        }

        let data = match msg.get("data") {
            Some(Value::Object(_)) => msg["data"].clone(),
            _ => return,
        };

        let topic = msg.get("topic").and_then(|t| t.as_str()).unwrap_or("");

        if let Some(sym) = topic.strip_prefix("kline.1.") {
            if data.get("confirm").and_then(|c| c.as_bool()) == Some(true) {
                let kline = self.parse_kline(&data);
                if let Some(kl) = kline {
                    let events = {
                        let mut trackers = self.trackers.write().unwrap();
                        trackers
                            .get_mut(sym)
                            .map(|tr| tr.process_kline(&kl))
                            .unwrap_or_default()
                    };
                    for ev in events {
                        match ev {
                            TrackerEvent::Entered {
                                symbol: s,
                                direction: d,
                                price,
                                sl,
                                tp,
                                ..
                            } => {
                                self.on_enter_position(s, d, price, sl, tp);
                            }
                            TrackerEvent::Exited { symbol: s, direction: d, .. } => {
                                self.on_exit_position(s, d);
                            }
                        }
                    }
                }
            }
        } else if let Some(sym) = topic.strip_prefix("tickers.") {
            let bid = data
                .get("bid1Price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let ask = data
                .get("ask1Price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());

            if let (Some(b), Some(a)) = (bid, ask) {
                let events = {
                    let mut trackers = self.trackers.write().unwrap();
                    trackers
                        .get_mut(sym)
                        .map(|tr| tr.update_ticker(b, a))
                        .unwrap_or_default()
                };
                for ev in events {
                    match ev {
                        TrackerEvent::Entered {
                            symbol: s,
                            direction: d,
                            price,
                            sl,
                            tp,
                            ..
                        } => {
                            self.on_enter_position(s, d, price, sl, tp);
                        }
                        TrackerEvent::Exited { symbol: s, direction: d, .. } => {
                            self.on_exit_position(s, d);
                        }
                    }
                }
            }
        }
    }

    fn parse_kline(&self, data: &Value) -> Option<KlineData> {
        let ts = data.get("start")?.as_i64()?;
        let open = data.get("open")?.as_str()?.parse::<f64>().ok()?;
        let high = data.get("high")?.as_str()?.parse::<f64>().ok()?;
        let low = data.get("low")?.as_str()?.parse::<f64>().ok()?;
        let close = data.get("close")?.as_str()?.parse::<f64>().ok()?;
        let volume = data
            .get("volume")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let secs = ts / 1000;
        let nsecs = ((ts % 1000) * 1_000_000) as u32;
        let datetime = DateTime::from_timestamp(secs, nsecs)
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());

        Some(KlineData {
            ts,
            datetime,
            open,
            high,
            low,
            close,
            volume,
        })
    }

    pub async fn run_ws(self: Arc<Self>) {
        let ws_url = "wss://stream.bybit.com/v5/public/linear";
        let mut retry_delay = 1u64;

        while self.running.load(Ordering::Relaxed) {
            let (tx, mut rx) = mpsc::unbounded_channel::<WsCommand>();
            {
                *self.ws_sender.lock().unwrap() = Some(tx);
            }

            match connect_async(ws_url).await {
                Ok((ws, _)) => {
                    println!("[WS] Connected");
                    retry_delay = 1;

                    let (write, read) = ws.split();
                    let write = Arc::new(tokio::sync::Mutex::new(Some(write)));

                    // resubscribe
                    {
                        let syms: Vec<String> = {
                            let trackers = self.trackers.read().unwrap();
                            trackers.keys().cloned().collect()
                        };
                        if !syms.is_empty() {
                            let args: Vec<String> = syms
                                .iter()
                                .flat_map(|s| {
                                    vec![format!("kline.1.{s}"), format!("tickers.{s}")]
                                })
                                .collect();
                            let sub = serde_json::json!({"op": "subscribe", "args": args});
                            let mut lock = write.lock().await;
                            if let Some(ref mut writer) = *lock {
                                let msg = serde_json::to_string(&sub).unwrap();
                                let _ = writer.send(Message::Text(msg.into())).await;
                            }
                        }
                    }

                    // forward channel commands to ws
                    let write_clone = write.clone();
                    let cmd_task = tokio::spawn(async move {
                        while let Some(cmd) = rx.recv().await {
                            let msg = match cmd {
                                WsCommand::Subscribe(sym) => serde_json::json!({
                                    "op": "subscribe",
                                    "args": [
                                        format!("kline.1.{sym}"),
                                        format!("tickers.{sym}")
                                    ]
                                }),
                                WsCommand::Unsubscribe(sym) => serde_json::json!({
                                    "op": "unsubscribe",
                                    "args": [
                                        format!("kline.1.{sym}"),
                                        format!("tickers.{sym}")
                                    ]
                                }),
                                WsCommand::ResubscribeAll(syms) => {
                                    let args: Vec<String> = syms
                                        .iter()
                                        .flat_map(|s| {
                                            vec![
                                                format!("kline.1.{s}"),
                                                format!("tickers.{s}"),
                                            ]
                                        })
                                        .collect();
                                    serde_json::json!({"op": "subscribe", "args": args})
                                }
                            };
                            let mut lock = write_clone.lock().await;
                            if let Some(ref mut writer) = *lock {
                                let text = serde_json::to_string(&msg).unwrap();
                                let _ = writer.send(Message::Text(text.into())).await;
                            }
                        }
                    });

                    // read messages
                    let engine = self.clone();
                    let read_task = tokio::spawn(async move {
                        let mut stream = read;
                        while let Some(Ok(msg)) = stream.next().await {
                            if let Message::Text(text) = msg {
                                engine.handle_message(&text).await;
                            }
                        }
                    });

                    tokio::select! {
                        _ = cmd_task => {}
                        _ = read_task => {}
                    }

                    {
                        let mut lock = write.lock().await;
                        *lock = None;
                    }
                }
                Err(e) => {
                    println!("[WS] Connection error: {e}. Retrying in {retry_delay}s...");
                }
            }

            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(retry_delay)).await;
            retry_delay = (retry_delay * 2).min(30);
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

// ─── Dashboard ────────────────────────────────────────────────────────────

fn dir_color(d: TradeDirection) -> &'static str {
    match d {
        TradeDirection::Long => "green",
        TradeDirection::Short => "red",
    }
}

fn dir_label(d: TradeDirection) -> &'static str {
    d.upper()
}

fn pnl_class(pnl: f64) -> &'static str {
    if pnl >= 0.0 {
        "green"
    } else {
        "red"
    }
}

fn pnl_color(pnl: f64) -> &'static str {
    if pnl >= 0.0 {
        "#4caf50"
    } else {
        "#ef5350"
    }
}

#[derive(Default)]
struct DashboardSummary {
    active: i32,
    total_closed: i32,
    wins: i32,
    losses: i32,
    total_pnl: f64,
    unrealized_pnl: f64,
}

impl RealtimeEngine {
    pub fn generate_dashboard(&self) -> String {
        let trackers = self.all_trackers();
        let subscribers = self.get_subscribers();

        let mut active_rows = String::new();
        let mut closed_rows = String::new();
        let mut all_closed: Vec<GridTrade> = Vec::new();
        let mut summary = DashboardSummary::default();

        for (_sym, tr) in &trackers {
            if let Some(ref t) = tr.active_trade {
                let live_price = tr.current_price.unwrap_or(t.entry_price);
                let unrealized = match t.direction {
                    TradeDirection::Long => {
                        (live_price - t.entry_price) / t.entry_price * 100.0
                    }
                    TradeDirection::Short => {
                        (t.entry_price - live_price) / t.entry_price * 100.0
                    }
                };
                let et = t.entry_time.format("%H:%M:%S");
                let d_cls = dir_color(t.direction);
                let d_lbl = dir_label(t.direction);
                active_rows.push_str(&format!(
                    "<tr>\
                     <td>{}</td>\
                     <td class=\"{}\">{}</td>\
                     <td>L{}</td>\
                     <td>{}</td>\
                     <td>{:.4}</td>\
                     <td>{:.4}</td>\
                     <td>{:.4}</td>\
                     <td style=\"color:{}\">{:+.2}%</td>\
                     <td><a href=\"/viewtrades/{}\">Chart</a></td>\
                     </tr>\n",
                    t.symbol,
                    d_cls,
                    d_lbl,
                    t.level,
                    et,
                    t.entry_price,
                    t.sl,
                    t.tp,
                    pnl_color(unrealized),
                    unrealized,
                    t.symbol,
                ));
                summary.active += 1;
                summary.unrealized_pnl += unrealized;
            }

            for t in &tr.closed_trades {
                all_closed.push(t.clone());
                summary.total_closed += 1;
                if t.pnl_pct() > 0.0 {
                    summary.wins += 1;
                } else {
                    summary.losses += 1;
                }
                summary.total_pnl += t.pnl_pct();
            }
        }

        all_closed.sort_by(|a, b| {
            let a_t = a
                .exit_time
                .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
            let b_t = b
                .exit_time
                .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
            b_t.cmp(&a_t)
        });

        for t in all_closed.iter().take(50) {
            let et = t.entry_time.format("%H:%M:%S");
            let ext = t
                .exit_time
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let d_cls = dir_color(t.direction);
            let d_lbl = dir_label(t.direction);
            let pnl = t.pnl_pct();
            let pnl_c = pnl_color(pnl);
            closed_rows.push_str(&format!(
                "<tr>\
                 <td>{}</td>\
                 <td class=\"{}\">{}</td>\
                 <td>L{}</td>\
                 <td>{}</td>\
                 <td>{}</td>\
                 <td>{:.4}</td>\
                 <td>{}</td>\
                 <td style=\"color:{}\">{:+.2}%</td>\
                 </tr>\n",
                t.symbol,
                d_cls,
                d_lbl,
                t.level,
                et,
                ext,
                t.entry_price,
                t.exit_price.unwrap_or(0.0),
                pnl_c,
                pnl,
            ));
        }

        let wr = if summary.total_closed > 0 {
            summary.wins as f64 / summary.total_closed as f64 * 100.0
        } else {
            0.0
        };
        let combined = summary.total_pnl + summary.unrealized_pnl;

        let sub_cards: String = subscribers
            .iter()
            .map(|sub| {
                format!(
                    "<span class=\"summary-card\" style=\"padding:8px 16px;min-width:100px\">\
                     <div class=\"num\" style=\"font-size:1.2em\">#{}</div>\
                     <div class=\"lbl\">{} USDT</div></span>",
                    sub.sub_id, sub.size
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let sub_cards = if sub_cards.is_empty() {
            "<span style=\"color:#8b949e;font-size:0.9em;padding:8px 0\">No subscriptions</span>"
                .to_string()
        } else {
            sub_cards
        };

        let sub_table_rows: String = subscribers
            .iter()
            .map(|sub| {
                format!(
                    "<tr><td>#{}</td>\
                     <td>{} USDT</td>\
                     <td><a href=\"/removeSubscriber?subID={}\" style=\"color:#ef5350\">remove</a></td></tr>\n",
                    sub.sub_id, sub.size, sub.sub_id
                )
            })
            .collect();

        let symlist: String = trackers
            .iter()
            .map(|(sym, tr)| {
                let pos_str = if let Some(ref t) = tr.active_trade {
                    let d = dir_label(t.direction);
                    let u = tr
                        .current_price
                        .zip(Some(t.entry_price))
                        .map(|(p, e)| match t.direction {
                            TradeDirection::Long => (p - e) / e * 100.0,
                            TradeDirection::Short => (e - p) / e * 100.0,
                        })
                        .map(|pnl| {
                            let c = pnl_class(pnl);
                            format!("<span class=\"{c}\">{pnl:+.2}%</span>")
                        })
                        .unwrap_or_else(|| "<span style=\"color:#8b949e\">flat</span>".to_string());
                    format!("{d} L{} {}", t.level, u)
                } else {
                    "<span style=\"color:#8b949e\">flat</span>".to_string()
                };
                format!(
                    "<li>\
                     <b>{sym}</b> base={base:.2} step={step:.3}% \
                     | pos: {pos_str} \
                     | trades: {attempts} \
                     | <a href=\"/viewtrades/{sym}\">chart</a> \
                     | <a href=\"/removeSymbol/{sym}\" style=\"color:#ef5350;font-size:0.8em\">remove</a>\
                     </li>",
                    base = tr.base_price,
                    step = 0.5,
                    attempts = tr.total_attempts(),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let symlist = if symlist.is_empty() {
            "<li style=\"color:#8b949e\">None</li>".to_string()
        } else {
            symlist
        };

        let sub_table = if sub_table_rows.is_empty() {
            "<tr><td colspan=\"3\" style=\"text-align:center;color:#8b949e\">No subscriptions active</td></tr>"
                .to_string()
        } else {
            sub_table_rows
        };

        let active_display = if active_rows.is_empty() {
            "<tr><td colspan=\"10\" style=\"text-align:center;color:#8b949e\">No active trades</td></tr>"
                .to_string()
        } else {
            active_rows
        };

        let closed_display = if closed_rows.is_empty() {
            "<tr><td colspan=\"8\" style=\"text-align:center;color:#8b949e\">No closed trades</td></tr>"
                .to_string()
        } else {
            closed_rows
        };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta http-equiv="refresh" content="5">
<title>Grid Trading Engine</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:20px }}
  h1,h2,h3 {{ color:#f0f6fc }}
  .summary {{ display:flex; gap:16px; flex-wrap:wrap; margin:16px 0 24px }}
  .summary-card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px 22px; min-width:120px }}
  .summary-card .num {{ font-size:1.6em; font-weight:700 }}
  .summary-card .lbl {{ font-size:0.8em; color:#8b949e }}
  table {{ width:100%; border-collapse:collapse; margin:10px 0 24px; font-size:0.85em }}
  th {{ background:#21262d; color:#8b949e; text-align:left; padding:8px 10px; border:1px solid #30363d }}
  td {{ padding:6px 10px; border:1px solid #30363d }}
  tr:hover td {{ background:#1c2128 }}
  .green {{ color:#4caf50; font-weight:600 }}
  .red {{ color:#ef5350; font-weight:600 }}
  a {{ color:#58a6ff; text-decoration:none }}
  a:hover {{ text-decoration:underline }}
  form {{ display:inline }}
  .actions {{ margin:16px 0; display:flex; gap:8px; flex-wrap:wrap }}
  input,button {{ padding:6px 12px; border-radius:4px; border:1px solid #30363d; background:#21262d; color:#c9d1d9 }}
  button {{ cursor:pointer; font-weight:600 }}
  button:hover {{ background:#30363d }}
  .btn-green {{ border-color:#4caf50; color:#4caf50 }}
  .btn-red {{ border-color:#ef5350; color:#ef5350 }}
  hr {{ border:none; border-top:1px solid #30363d; margin:24px 0 }}
</style>
</head>
<body>
<h1>Grid Trading Engine &mdash; 1:3 R:R, 0.5% Grid</h1>

<div class="summary">
  <div class="summary-card"><div class="num">{active}</div><div class="lbl">Active</div></div>
  <div class="summary-card"><div class="num">{total_closed}</div><div class="lbl">Closed</div></div>
  <div class="summary-card"><div class="num">{wins}</div><div class="lbl">Wins</div></div>
  <div class="summary-card"><div class="num">{losses}</div><div class="lbl">Losses</div></div>
  <div class="summary-card"><div class="num">{wr:.1}%</div><div class="lbl">Win Rate</div></div>
  <div class="summary-card"><div class="num" style="color:{total_pnl_cls}">{total_pnl:+.2}%</div><div class="lbl">Realized PnL</div></div>
  <div class="summary-card"><div class="num" style="color:{upnl_cls}">{unrealized_pnl:+.2}%</div><div class="lbl">Unrealized PnL</div></div>
  <div class="summary-card"><div class="num" style="color:{combined_cls}">{combined:+.2}%</div><div class="lbl">Total</div></div>
</div>

<h3>Subscriptions</h3>
<div class="summary">{sub_cards}</div>

<h3>Subscription Details</h3>
<table><thead><tr><th>ID</th><th>Size</th><th></th></tr></thead><tbody>
{sub_table}
</tbody></table>

<div class="actions" style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 16px;margin:12px 0">
  <h4>Manage Subscriptions</h4>
  <form action="/subscribe" method="get">
    <input name="subID" placeholder="SubID" style="width:60px" required>
    <input name="size" placeholder="USDT" style="width:70px" value="5">
    <button type="submit" class="btn-green">Subscribe</button>
  </form>
  <form action="/removeSubscriber" method="get">
    <input name="subID" placeholder="SubID" style="width:60px" required>
    <button type="submit" style="color:#ef5350;border-color:#ef5350">Remove Sub</button>
  </form>
</div>

<div class="actions">
  <form action="/watch" method="get">
    <input name="symbol" placeholder="BTCUSDT" style="width:110px" required>
    <button type="submit" class="btn-green">+ Watch</button>
  </form>
  <a href="/clear"><button type="button" style="color:#ef5350;border-color:#ef5350">Clear All</button></a>
  <a href="/flush"><button type="button" style="color:#8b949e;border-color:#8b949e">Reset WS</button></a>
</div>

<h2>Active Trades</h2>
<table><thead><tr><th>Symbol</th><th>Dir</th><th>Level</th><th>Entry Time</th><th>Entry $</th><th>SL</th><th>TP</th><th>Unrealized</th><th>View</th></tr></thead><tbody>
{active_display}
</tbody></table>

<h2>Closed Trades <span style="font-size:0.7em;color:#8b949e">(last 50)</span></h2>
<table><thead><tr><th>Symbol</th><th>Dir</th><th>Level</th><th>Entry</th><th>Exit</th><th>Entry $</th><th>Exit $</th><th>PnL</th></tr></thead><tbody>
{closed_display}
</tbody></table>

<hr>
<h3>Watched Symbols</h3>
<ul>
{symlist}
</ul>
</body></html>"#,
            active = summary.active,
            total_closed = summary.total_closed,
            wins = summary.wins,
            losses = summary.losses,
            wr = wr,
            total_pnl = summary.total_pnl,
            total_pnl_cls = pnl_class(summary.total_pnl),
            unrealized_pnl = summary.unrealized_pnl,
            upnl_cls = pnl_class(summary.unrealized_pnl),
            combined = combined,
            combined_cls = pnl_class(combined),
            sub_cards = sub_cards,
            sub_table = sub_table,
            active_display = active_display,
            closed_display = closed_display,
            symlist = symlist,
        )
    }

    pub fn generate_subs_page(&self) -> String {
        let subscribers = self.get_subscribers();
        let trackers = self.all_trackers();

        let sub_rows: String = subscribers
            .iter()
            .map(|sub| {
                format!(
                    "<tr><td>#{}</td>\
                     <td>{} USDT</td>\
                     <td><a href=\"/removeSubscriber?subID={}\" style=\"color:#ef5350\">remove</a></td></tr>\n",
                    sub.sub_id, sub.size, sub.sub_id
                )
            })
            .collect();

        let sym_rows: String = trackers
            .iter()
            .map(|(sym, tr)| {
                let state = if tr.active_trade.is_some() {
                    "IN"
                } else {
                    "flat"
                };
                let state_color = if tr.active_trade.is_some() {
                    "#4caf50"
                } else {
                    "#8b949e"
                };
                format!(
                    "<tr>\
                     <td><a href=\"/viewtrades/{}\">{}</a></td>\
                     <td>{:.2}</td>\
                     <td style=\"color:{}\">{}</td>\
                     <td>{} trades</td>\
                     <td><a href=\"/removeSymbol/{}\" style=\"color:#ef5350\">remove</a></td>\
                     </tr>\n",
                    sym,
                    sym,
                    tr.base_price,
                    state_color,
                    state,
                    tr.total_attempts(),
                    sym,
                )
            })
            .collect();

        let sub_display = if sub_rows.is_empty() {
            "<tr><td colspan=\"3\" style=\"text-align:center;color:#8b949e\">No subscriptions</td></tr>".to_string()
        } else {
            sub_rows
        };

        let sym_display = if sym_rows.is_empty() {
            "<tr><td colspan=\"5\" style=\"text-align:center;color:#8b949e\">No symbols watched</td></tr>"
                .to_string()
        } else {
            sym_rows
        };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Manage — Grid Engine</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:30px; max-width:900px; margin:0 auto }}
  h1,h2,h3 {{ color:#f0f6fc; margin:24px 0 12px }}
  a {{ color:#58a6ff; text-decoration:none }} a:hover {{ text-decoration:underline }}
  table {{ width:100%; border-collapse:collapse; margin:10px 0 24px; font-size:0.9em }}
  th {{ background:#21262d; color:#8b949e; text-align:left; padding:8px 12px; border:1px solid #30363d }}
  td {{ padding:6px 12px; border:1px solid #30363d }}
  tr:hover td {{ background:#1c2128 }}
  .green {{ color:#4caf50; font-weight:600 }}
  .red {{ color:#ef5350; font-weight:600 }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px 20px; margin:16px 0 }}
  .card h3 {{ margin:0 0 12px }}
  form {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:8px 0 }}
  input,button {{ padding:8px 14px; border-radius:6px; border:1px solid #30363d; background:#21262d; color:#c9d1d9; font-size:0.9em }}
  button {{ cursor:pointer; font-weight:600; min-width:130px }}
  button:hover {{ background:#30363d }}
  .btn-green {{ border-color:#4caf50; color:#4caf50 }}
  .btn-red {{ border-color:#ef5350; color:#ef5350 }}
  .nav {{ display:flex; gap:16px; margin-bottom:20px }}
  .nav a {{ padding:6px 0; border-bottom:2px solid transparent }}
  .nav a.active {{ border-color:#58a6ff }}
  .badge {{ background:#21262d; border-radius:10px; padding:1px 8px; font-size:0.8em; margin-left:6px }}
</style>
</head>
<body>
<div class="nav">
  <a href="/">Dashboard</a>
  <a href="/subs" class="active">Manage</a>
  <a href="/state">State</a>
</div>
<h1>Manage Grid Engine</h1>

<div class="card">
  <h3>Subscribe to Signals <span class="badge">{subs_count} active</span></h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    Subscribers follow all grid signals automatically (both Long and Short).
    When any watched symbol enters a grid position, all subscribers execute
    the same direction with their configured USDT size.
  </p>
  <form action="/subscribe" method="get">
    <input type="hidden" name="redirect" value="subs">
    <label>Sub ID:</label><input name="subID" placeholder="e.g. 1" style="width:70px" required>
    <label>USDT:</label><input name="size" placeholder="5" style="width:70px" value="5">
    <button type="submit" class="btn-green">Subscribe</button>
  </form>
  <form action="/removeSubscriber" method="get">
    <label>Sub ID:</label><input name="subID" placeholder="e.g. 1" style="width:70px" required>
    <button type="submit" style="color:#ef5350;border-color:#ef5350">Remove Subscriber</button>
  </form>
</div>

<h3>Active Subscriptions</h3>
<table><thead><tr><th>ID</th><th>Size</th><th></th></tr></thead><tbody>
{sub_display}
</tbody></table>

<div class="card">
  <h3>Watch Symbols <span class="badge">{syms_count} watching</span></h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    Add symbols to watch. Price at subscription becomes the base level.
    Grid levels are spaced at 0.5% intervals. The engine opens 1:3 R:R trades
    when price crosses levels.
  </p>
  <form action="/watch" method="get">
    <label>Symbol:</label><input name="symbol" placeholder="BTCUSDT" style="width:120px" required>
    <button type="submit" class="btn-green">Watch</button>
  </form>
</div>

<h3>Watched Symbols</h3>
<table><thead><tr><th>Symbol</th><th>Base</th><th>State</th><th>Trades</th><th></th></tr></thead><tbody>
{sym_display}
</tbody></table>

<div class="card">
  <a href="/clear" style="color:#ef5350;font-weight:600">Clear All</a>
</div>
</body></html>"#,
            subs_count = subscribers.len(),
            syms_count = trackers.len(),
            sub_display = sub_display,
            sym_display = sym_display,
        )
    }

    pub fn generate_view_trades_page(&self, symbol: &str) -> Option<String> {
        let tr = self.get_tracker(symbol)?;
        let chart = generate_chart_svg(&tr);
        let total_pnl: f64 = tr.closed_trades.iter().map(|t| t.pnl_pct()).sum();
        let (state_bold, state_cls) = if tr.active_trade.is_some() {
            ("IN POSITION", "green")
        } else {
            ("FLAT", "red")
        };
        let entry_str = tr
            .active_trade
            .as_ref()
            .map(|t| format!("{:.4} (L{})", t.entry_price, t.level))
            .unwrap_or_else(|| "-".to_string());
        let sl_str = tr
            .active_trade
            .as_ref()
            .map(|t| format!("{:.4}", t.sl))
            .unwrap_or_else(|| "-".to_string());
        let tp_str = tr
            .active_trade
            .as_ref()
            .map(|t| format!("{:.4}", t.tp))
            .unwrap_or_else(|| "-".to_string());

        let mut closed_rows = String::new();
        for (i, t) in tr.closed_trades.iter().enumerate() {
            let et = t.entry_time.format("%H:%M:%S");
            let ext = t
                .exit_time
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let dur = t
                .duration()
                .map(|d| {
                    let secs = d.num_seconds();
                    format!("{}m{}s", secs / 60, secs % 60)
                })
                .unwrap_or_else(|| "-".to_string());
            let pnl = t.pnl_pct();
            let clr = pnl_class(pnl);
            closed_rows.push_str(&format!(
                "<tr>\
                 <td>{}</td>\
                 <td>{}</td>\
                 <td>L{}</td>\
                 <td>{et}</td>\
                 <td>{ext}</td>\
                 <td>{:.4}</td>\
                 <td>{:.4}</td>\
                 <td class=\"{clr}\">{pnl:+.2}%</td>\
                 <td>{dur}</td>\
                 </tr>\n",
                i + 1,
                dir_label(t.direction),
                t.level,
                t.entry_price,
                t.exit_price.unwrap_or(0.0),
            ));
        }
        if closed_rows.is_empty() {
            closed_rows = "<tr><td colspan=\"9\" style=\"color:#8b949e;text-align:center\">No closed trades</td></tr>".to_string();
        }

        Some(format!(
            r#"<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{symbol} Grid Trades</title>
<style>
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:20px }}
  h1,h2 {{ color:#f0f6fc }}
  table {{ border-collapse:collapse; margin:12px 0 }}
  th,td {{ padding:6px 12px; border:1px solid #30363d; text-align:left }}
  th {{ background:#21262d; color:#8b949e }}
  .green {{ color:#4caf50 }} .red {{ color:#ef5350 }}
  a {{ color:#58a6ff }}
</style></head><body>
<h1>{symbol} &mdash; Grid Bot</h1>
<p>Base: {base:.4} | Step: 0.5% | State: <b class="{state_cls}">{state_bold}</b>
 | Active Entry: {entry_str}
 | SL: {sl_str}
 | TP: {tp_str}
 | Total Closed PnL: <span class="{pnl_cls}">{total_pnl:+.2}%</span></p>

{chart}

<h2>Closed Trades</h2>
<table><thead><tr><th>#</th><th>Dir</th><th>Level</th><th>Entry Time</th><th>Exit Time</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Duration</th></tr></thead><tbody>
{closed_rows}
</tbody></table>
<p><a href="/">Back to dashboard</a></p>
</body></html>"#,
            symbol = symbol,
            base = tr.base_price,
            state_cls = state_cls,
            state_bold = state_bold,
            entry_str = entry_str,
            sl_str = sl_str,
            tp_str = tp_str,
            pnl_cls = pnl_class(total_pnl),
            total_pnl = total_pnl,
            chart = chart,
            closed_rows = closed_rows,
        ))
    }

    pub fn generate_state_page(&self) -> String {
        let trackers = self.all_trackers();
        let ws_connected = self.ws_sender.lock().unwrap().is_some();
        let mut lines = Vec::new();
        for (sym, tr) in &trackers {
            let active = tr
                .active_trade
                .as_ref()
                .map(|t| {
                    format!(
                        "L{} {} EP={} SL={} TP={}",
                        t.level,
                        t.direction.upper(),
                        t.entry_price,
                        t.sl,
                        t.tp
                    )
                })
                .unwrap_or_else(|| "flat".to_string());
            lines.push(format!(
                "<b>{sym}</b> base={bp:.4} step=0.5% | current={cp} | active: {active} | klines={kc} | closed_trades={ct} | ws={ws}",
                bp = tr.base_price,
                cp = tr.current_price.map(|p| format!("{p}")).unwrap_or_else(|| "None".to_string()),
                kc = tr.klines.len(),
                ct = tr.closed_trades.len(),
                ws = ws_connected,
            ));
        }
        if lines.is_empty() {
            lines.push("No trackers.".to_string());
        }
        format!(
            "<html><body style=\"font-family:monospace;background:#0d1117;color:#c9d1d9;padding:20px\">{}</body></html>",
            lines.join("<br>"),
        )
    }

    pub fn score_symbol_grid(klines: &[KlineData]) -> GridScore {
        if klines.len() < 10 {
            return GridScore {
                symbol: String::new(),
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
        }

        let base_price = klines[0].close;
        let _step_pct = 0.005;

        // price_range
        let max_price = klines.iter().map(|k| k.high).fold(f64::MIN, f64::max);
        let min_price = klines.iter().map(|k| k.low).fold(f64::MAX, f64::min);
        let price_range_pct = (max_price - min_price) / base_price * 100.0;

        fn level_of(price: f64, base: f64) -> i32 {
            ((price / base - 1.0) / 0.005).round() as i32
        }
        fn price_for_level(level: i32, base: f64) -> f64 {
            base * (1.0 + level as f64 * 0.005)
        }

        // level crossing count + oscillation analysis
        let mut level_crosses: u32 = 0;
        let mut direction_changes: u32 = 0;
        let mut last_dir: i32 = 0;
        let mut last_level = level_of(klines[0].close, base_price);

        for k in klines.iter().skip(1) {
            let curr = level_of(k.close, base_price);
            if curr != last_level {
                level_crosses += 1;
                let dir = curr - last_level;
                if dir.signum() != last_dir.signum() && last_dir != 0 {
                    direction_changes += 1;
                }
                last_dir = dir;
            }
            last_level = curr;
        }

        let oscillation_score = if level_crosses > 0 {
            (direction_changes as f64 / level_crosses as f64) * 100.0
        } else {
            0.0
        };

        // Backtest the 1:3 R:R grid strategy over all klines
        let mut total_trades: u32 = 0;
        let mut wins: u32 = 0;
        let mut losses: u32 = 0;
        let mut total_pnl_pct: f64 = 0.0;
        let mut active: Option<(TradeDirection, f64, f64, f64)> = None; // (dir, entry, sl, tp)

        let mut prev_level = level_of(klines[0].close, base_price);

        for k in klines.iter().skip(1) {
            let high = k.high;
            let low = k.low;
            let curr_level = level_of(k.close, base_price);

            // check exit
            if let Some((dir, _entry, sl, tp)) = active {
                let hit_sl = match dir {
                    TradeDirection::Long => low <= sl,
                    TradeDirection::Short => high >= sl,
                };
                let hit_tp = match dir {
                    TradeDirection::Long => high >= tp,
                    TradeDirection::Short => low <= tp,
                };
                if hit_sl || hit_tp {
                    let exit_price = if hit_sl {
                        match dir {
                            TradeDirection::Long => low,
                            TradeDirection::Short => high,
                        }
                    } else {
                        tp
                    };
                    let pnl = match dir {
                        TradeDirection::Long => (exit_price - _entry) / _entry * 100.0,
                        TradeDirection::Short => (_entry - exit_price) / _entry * 100.0,
                    };
                    if hit_tp {
                        wins += 1;
                    } else {
                        losses += 1;
                    }
                    total_pnl_pct += pnl;
                    active = None;
                }
            }

            // check entry (level crossed, only if no active trade)
            if active.is_none() && curr_level != prev_level {
                if curr_level > prev_level {
                    // short (mean reversion) — risk 1.5%, reward 0.5%
                    let entry = k.close;
                    let sl = entry * 1.015;
                    let tp = entry * 0.995;
                    active = Some((TradeDirection::Short, entry, sl, tp));
                    total_trades += 1;
                } else {
                    // long (mean reversion) — risk 1.5%, reward 0.5%
                    let entry = k.close;
                    let sl = entry * 0.985;
                    let tp = entry * 1.005;
                    active = Some((TradeDirection::Long, entry, sl, tp));
                    total_trades += 1;
                }
            }

            prev_level = curr_level;
        }

        let win_rate = if total_trades > 0 {
            wins as f64 / total_trades as f64 * 100.0
        } else {
            0.0
        };

        // suitability composite: oscillation 40%, level cross count 20%, range 20%, PnL 20%
        let cross_norm = (level_crosses as f64 / klines.len() as f64 * 100.0).min(100.0);
        let range_norm = price_range_pct.min(100.0);
        let osc_norm = oscillation_score.min(100.0);
        let pnl_norm = (total_pnl_pct + 10.0).max(0.0).min(100.0);

        let suitability =
            osc_norm * 0.40 + cross_norm * 0.20 + range_norm * 0.20 + pnl_norm * 0.20;

        GridScore {
            symbol: String::new(),
            base_price,
            price_range_pct,
            level_crosses,
            oscillation_score,
            total_trades,
            wins,
            losses,
            total_pnl_pct,
            win_rate,
            suitability,
        }
    }
}

// ─── SVG Chart Generation ────────────────────────────────────────────────

fn generate_chart_svg(tr: &SymbolTracker) -> String {
    let klines = &tr.klines;
    if klines.len() < 2 {
        return "<p style=\"color:gray\">Not enough data yet.</p>".to_string();
    }

    let base = tr.base_price;
    let _step = tr.level_step;

    let chart_w = 800.0;
    let chart_h = 350.0;
    let pnl_h = 130.0;
    let margin_l = 70.0;
    let margin_r = 20.0;
    let margin_t = 30.0;
    let margin_b = 30.0;
    let plot_w = chart_w - margin_l - margin_r;
    let plot_h = chart_h - margin_t - margin_b;

    let mut min_price = f64::MAX;
    let mut max_price = f64::MIN;
    for k in klines {
        if k.low < min_price {
            min_price = k.low;
        }
        if k.high > max_price {
            max_price = k.high;
        }
    }
    let price_pad = (max_price - min_price) * 0.05;
    min_price -= price_pad;
    max_price += price_pad;
    if (max_price - min_price).abs() < 1e-10 {
        max_price = min_price + 1.0;
    }

    let to_x = |i: usize| -> f64 {
        if klines.len() <= 1 {
            return margin_l + plot_w / 2.0;
        }
        margin_l + (i as f64 / (klines.len() - 1) as f64) * plot_w
    };
    let to_y = |price: f64| -> f64 {
        margin_t + plot_h * (1.0 - (price - min_price) / (max_price - min_price))
    };

    // Compute realized PnL at each kline index
    let mut pnl_line: Vec<f64> = vec![0.0; klines.len()];
    let mut _active_trade_range: Option<(usize, usize, GridTrade)> = None;

    // Reconstruct trade timeline: find each closed trade's range
    for t in &tr.closed_trades {
        let entry_i = klines
            .iter()
            .position(|k| k.datetime >= t.entry_time)
            .unwrap_or(0);
        let exit_i = t
            .exit_time
            .and_then(|ext| klines.iter().position(|k| k.datetime >= ext))
            .unwrap_or(klines.len() - 1);
        for i in entry_i..=exit_i {
            if i < klines.len() {
                let entry = t.entry_price;
                let price = klines[i].close;
                pnl_line[i] = match t.direction {
                    TradeDirection::Long => (price - entry) / entry * 100.0,
                    TradeDirection::Short => (entry - price) / entry * 100.0,
                };
            }
        }
    }

    // Active trade
    if let Some(ref t) = tr.active_trade {
        let entry_i = klines
            .iter()
            .position(|k| k.datetime >= t.entry_time)
            .unwrap_or(klines.len() - 1);
        _active_trade_range = Some((entry_i, klines.len() - 1, t.clone()));
        for i in entry_i..klines.len() {
            let price = klines[i].close;
            pnl_line[i] = match t.direction {
                TradeDirection::Long => (price - t.entry_price) / t.entry_price * 100.0,
                TradeDirection::Short => (t.entry_price - price) / t.entry_price * 100.0,
            };
        }
    }

    let pnl_min = pnl_line.iter().cloned().fold(f64::MAX, f64::min);
    let pnl_max = pnl_line.iter().cloned().fold(f64::MIN, f64::max);
    let pnl_range = (pnl_max - pnl_min).max(0.1);
    let pnl_pad = pnl_range * 0.1;
    let pnl_min = pnl_min - pnl_pad;
    let pnl_max = pnl_max + pnl_pad;
    let pnl_to_y = |p: f64| -> f64 {
        chart_h + 20.0 + pnl_h * (1.0 - (p - pnl_min) / (pnl_max - pnl_min))
    };

    let tw = chart_w + 50.0;
    let th = chart_h + pnl_h + 50.0;

    let mut s = String::new();
    s.push_str(&format!(
        r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {tw} {th}" style="width:100%;max-width:900px;background:#0d1117;border-radius:6px">
<style>
  .candle-up {{ fill:#26a69a;stroke:#26a69a }}
  .candle-dn {{ fill:#ef5350;stroke:#ef5350 }}
  .wick-up {{ stroke:#26a69a }}
  .wick-dn {{ stroke:#ef5350 }}
  .grid-line {{ stroke:#30363d;stroke-width:0.5 }}
  .grid-base {{ stroke:#f0f6fc;stroke-width:1;stroke-dasharray:6,3 }}
  .grid-level {{ stroke:#30363d;stroke-width:0.5;stroke-dasharray:2,4 }}
  .marker-entry-long {{ fill:#4caf50;stroke:#000;stroke-width:0.5 }}
  .marker-entry-short {{ fill:#ef5350;stroke:#000;stroke-width:0.5 }}
  .marker-exit {{ fill:#f0883e;stroke:#000;stroke-width:0.5 }}
  .pnl-line {{ fill:none;stroke:#c9d1d9;stroke-width:1 }}
  .pnl-fill-p {{ fill:rgba(76,175,80,0.3) }}
  .pnl-fill-n {{ fill:rgba(239,83,80,0.3) }}
  .axis-text {{ fill:#8b949e;font-size:10px;font-family:monospace }}
</style>
"###,
    ));

    // grid lines (time)
    let grid_step = (klines.len() / 10).max(1);
    for i in (0..klines.len()).step_by(grid_step) {
        let x = to_x(i);
        s.push_str(&format!(
            r###"<line x1="{x}" y1="{ty}" x2="{x}" y2="{by}" class="grid-line"/>"###,
            ty = margin_t,
            by = margin_t + plot_h
        ));
    }

    // price axis labels
    for pct in [0.0, 0.25, 0.5, 0.75, 1.0] {
        let price = min_price + (max_price - min_price) * pct;
        let y = to_y(price);
        s.push_str(&format!(
            r###"<line x1="{ml}" y1="{y}" x2="{mr}" y2="{y}" class="grid-line"/>"###,
            ml = margin_l,
            mr = margin_l + plot_w
        ));
        s.push_str(&format!(
            r###"<text x="{tx}" y="{y}" class="axis-text" text-anchor="end" dominant-baseline="middle">{price:.2}</text>"###,
            tx = margin_l - 5.0,
        ));
    }

    // grid level lines (0.5% intervals)
    for lvl_offset in -100i32..=100i32 {
        let price = base * (1.0 + lvl_offset as f64 * 0.005);
        if price < min_price || price > max_price {
            continue;
        }
        let y = to_y(price);
        let cls = if lvl_offset == 0 {
            "grid-base"
        } else {
            "grid-level"
        };
        s.push_str(&format!(
            r###"<line x1="{ml}" y1="{y}" x2="{mr}" y2="{y}" class="{cls}"/>"###,
            ml = margin_l,
            mr = margin_l + plot_w
        ));
        s.push_str(&format!(
            r###"<text x="{tx}" y="{y}" fill="#8b949e" font-size="8" font-family="monospace" text-anchor="start" dominant-baseline="middle">L{level}</text>"###,
            tx = margin_l + plot_w + 3.0,
            level = lvl_offset,
        ));
    }

    // candlesticks
    let max_w = if klines.len() > 1 {
        (plot_w / klines.len() as f64) * 0.6
    } else {
        5.0
    };
    let bw = max_w.max(1.0).min(6.0);

    for (i, k) in klines.iter().enumerate() {
        let x = to_x(i);
        let is_up = k.close >= k.open;
        let cls = if is_up { "up" } else { "dn" };
        let y_top = to_y(k.high);
        let y_bot = to_y(k.low);
        let y_open = to_y(k.open);
        let y_close = to_y(k.close);
        let y_body_top = y_open.min(y_close);
        let y_body_bot = y_open.max(y_close);
        let body_h = (y_body_bot - y_body_top).max(1.0);

        s.push_str(&format!(
            r###"<line x1="{x}" y1="{y_top}" x2="{x}" y2="{y_bot}" class="wick-{cls}" stroke-width="0.8"/>"###
        ));
        s.push_str(&format!(
            r###"<rect x="{rx}" y="{y_body_top}" width="{bw}" height="{body_h}" class="candle-{cls}"/>"###,
            rx = x - bw / 2.0
        ));
    }

    // trade markers
    for t in &tr.closed_trades {
        let entry_i = klines
            .iter()
            .position(|k| k.datetime >= t.entry_time);
        if let Some(ei) = entry_i {
            let ex = to_x(ei);
            let ey = to_y(t.entry_price);
            match t.direction {
                TradeDirection::Long => {
                    let y1 = ey - 7.0;
                    let y2 = ey + 2.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="marker-entry-long"/>"###,
                        xl = ex - 5.0,
                        xr = ex + 5.0,
                    ));
                }
                TradeDirection::Short => {
                    let y1 = ey + 7.0;
                    let y2 = ey - 2.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="marker-entry-short"/>"###,
                        xl = ex - 5.0,
                        xr = ex + 5.0,
                    ));
                }
            }
        }
        if let Some(ext) = t.exit_time {
            if let Some(xi) = klines.iter().position(|k| k.datetime >= ext) {
                let x = to_x(xi);
                let y = to_y(t.exit_price.unwrap_or(t.entry_price));
                let x1 = x - 4.0;
                let y1 = y - 4.0;
                let x2 = x + 4.0;
                let y2 = y + 4.0;
                s.push_str(&format!(
                    r###"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="marker-exit" stroke-width="1.5"/>
<line x1="{x2}" y1="{y1}" x2="{x1}" y2="{y2}" class="marker-exit" stroke-width="1.5"/>"###
                ));
            }
        }
    }

    // active trade marker
    if let Some(ref t) = tr.active_trade {
        let entry_i = klines.iter().position(|k| k.datetime >= t.entry_time);
        if let Some(ei) = entry_i {
            let ex = to_x(ei);
            let ey = to_y(t.entry_price);
            match t.direction {
                TradeDirection::Long => {
                    let y1 = ey - 9.0;
                    let y2 = ey + 3.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="marker-entry-long" stroke-width="1.5" stroke="#fff"/>"###,
                        xl = ex - 6.0,
                        xr = ex + 6.0,
                    ));
                }
                TradeDirection::Short => {
                    let y1 = ey + 9.0;
                    let y2 = ey - 3.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="marker-entry-short" stroke-width="1.5" stroke="#fff"/>"###,
                        xl = ex - 6.0,
                        xr = ex + 6.0,
                    ));
                }
            }
        }
    }

    // PnL panel
    let pnl_zero_y = pnl_to_y(0.0);
    s.push_str(&format!(
        r###"<line x1="{ml}" y1="{pnl_zero_y}" x2="{mr}" y2="{pnl_zero_y}" stroke="#30363d" stroke-width="0.5"/>"###,
        ml = margin_l,
        mr = margin_l + plot_w
    ));

    for i in 0..klines.len().saturating_sub(1) {
        let x1 = to_x(i);
        let x2 = to_x(i + 1);
        let y1 = pnl_to_y(pnl_line[i]);
        let y2 = pnl_to_y(pnl_line[i + 1]);
        let zy = pnl_to_y(0.0);
        let fill_cls = if pnl_line[i] >= 0.0 {
            "pnl-fill-p"
        } else {
            "pnl-fill-n"
        };
        s.push_str(&format!(
            r###"<polygon points="{x1},{y1} {x2},{y2} {x2},{zy} {x1},{zy}" class="{fill_cls}"/>"###,
        ));
    }

    let points: Vec<String> = pnl_line
        .iter()
        .enumerate()
        .map(|(i, &p)| format!("{},{}", to_x(i), pnl_to_y(p)))
        .collect();
    s.push_str(&format!(
        r###"<polyline points="{pts}" class="pnl-line"/>"###,
        pts = points.join(" ")
    ));

    for pct in [0.0, 0.25, 0.5, 0.75, 1.0] {
        let p = pnl_min + (pnl_max - pnl_min) * pct;
        let y = pnl_to_y(p);
        s.push_str(&format!(
            r###"<text x="{tx}" y="{y}" class="axis-text" text-anchor="end" dominant-baseline="middle">{p:+.1}%</text>"###,
            tx = margin_l - 5.0
        ));
    }

    s.push_str("</svg>");
    s
}
