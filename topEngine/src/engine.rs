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
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Long => "long",
            Side::Short => "short",
        }
    }
    pub fn upper(&self) -> &'static str {
        match self {
            Side::Long => "LONG",
            Side::Short => "SHORT",
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
    pub side: Side,
    pub size: f64,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub symbol: String,
    pub direction: Side,
    pub entry_time: DateTime<Utc>,
    pub entry_price: f64,
    pub exit_time: Option<DateTime<Utc>>,
    pub exit_price: Option<f64>,
    pub status: TradeStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradeStatus {
    Open,
    Closed,
}

impl Trade {
    fn pnl_pct(&self) -> f64 {
        if self.status == TradeStatus::Open || self.exit_price.is_none() {
            return 0.0;
        }
        let exit = self.exit_price.unwrap();
        match self.direction {
            Side::Long => (exit - self.entry_price) / self.entry_price * 100.0,
            Side::Short => (self.entry_price - exit) / self.entry_price * 100.0,
        }
    }

    fn duration(&self) -> Option<Duration> {
        self.exit_time.map(|t| t - self.entry_time)
    }
}

// ─── SymbolTracker ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SymbolTracker {
    pub symbol: String,
    pub direction: Side,
    pub entry_price: Option<f64>,
    pub sl: Option<f64>,
    pub in_position: bool,
    pub waiting_for_reentry: bool,
    pub klines: Vec<KlineData>,
    pub closed_trades: Vec<Trade>,
    pub active_trade: Option<Trade>,
    pub current_price: Option<f64>,
    pub current_bid: Option<f64>,
    pub current_ask: Option<f64>,
}

impl SymbolTracker {
    pub fn new(symbol: String, direction: Side) -> Self {
        SymbolTracker {
            symbol,
            direction,
            entry_price: None,
            sl: None,
            in_position: false,
            waiting_for_reentry: false,
            klines: Vec::new(),
            closed_trades: Vec::new(),
            active_trade: None,
            current_price: None,
            current_bid: None,
            current_ask: None,
        }
    }

    pub fn total_attempts(&self) -> usize {
        self.closed_trades.len() + if self.active_trade.is_some() { 1 } else { 0 }
    }

    pub fn process_kline(&mut self, kline: &KlineData) -> TrackerEvent {
        let close = kline.close;
        let high = kline.high;
        let low = kline.low;
        let dt = kline.datetime;

        self.klines.push(kline.clone());
        if self.klines.len() > 5000 {
            self.klines.remove(0);
        }

        if self.entry_price.is_none() {
            let ask = self.current_ask.unwrap_or(close);
            let bid = self.current_bid.unwrap_or(close);
            let (entry, sl_val) = match self.direction {
                Side::Long => (ask, ask * 0.995),
                Side::Short => (bid, bid * 1.005),
            };
            self.entry_price = Some(entry);
            self.sl = Some(sl_val);
            self.in_position = true;
            self.active_trade = Some(Trade {
                symbol: self.symbol.clone(),
                direction: self.direction,
                entry_time: dt,
                entry_price: entry,
                exit_time: None,
                exit_price: None,
                status: TradeStatus::Open,
            });
            return TrackerEvent::Entered {
                symbol: self.symbol.clone(),
                direction: self.direction,
                price: entry,
            };
        }

        match self.direction {
            Side::Long => self.process_long(close, low, dt),
            Side::Short => self.process_short(close, high, dt),
        }
    }

    fn process_long(&mut self, close: f64, low: f64, dt: DateTime<Utc>) -> TrackerEvent {
        let mut event = TrackerEvent::None;
        let ask = self.current_ask.unwrap_or(close);
        let bid = self.current_bid.unwrap_or(low);

        if !self.in_position && !self.waiting_for_reentry {
            self.in_position = true;
            self.active_trade = Some(Trade {
                symbol: self.symbol.clone(),
                direction: Side::Long,
                entry_time: dt,
                entry_price: ask,
                exit_time: None,
                exit_price: None,
                status: TradeStatus::Open,
            });
            event = TrackerEvent::Entered {
                symbol: self.symbol.clone(),
                direction: Side::Long,
                price: ask,
            };
        } else if !self.in_position && self.waiting_for_reentry {
            if let Some(ep) = self.entry_price {
                if ask > ep {
                    self.in_position = true;
                    self.waiting_for_reentry = false;
                    self.active_trade = Some(Trade {
                        symbol: self.symbol.clone(),
                        direction: Side::Long,
                        entry_time: dt,
                        entry_price: ask,
                        exit_time: None,
                        exit_price: None,
                        status: TradeStatus::Open,
                    });
                    event = TrackerEvent::Entered {
                        symbol: self.symbol.clone(),
                        direction: Side::Long,
                        price: ask,
                    };
                }
            }
        }

        if self.in_position {
            if let Some(sl) = self.sl {
                if bid <= sl {
                    self.in_position = false;
                    self.waiting_for_reentry = true;
                    if let Some(mut t) = self.active_trade.take() {
                        t.exit_time = Some(dt);
                        t.exit_price = Some(sl);
                        t.status = TradeStatus::Closed;
                        self.closed_trades.push(t);
                    }
                    event = TrackerEvent::Exited {
                        symbol: self.symbol.clone(),
                    };
                }
            }
        }

        event
    }

    fn process_short(&mut self, close: f64, high: f64, dt: DateTime<Utc>) -> TrackerEvent {
        let mut event = TrackerEvent::None;
        let ask = self.current_ask.unwrap_or(high);
        let bid = self.current_bid.unwrap_or(close);

        if !self.in_position && !self.waiting_for_reentry {
            self.in_position = true;
            self.active_trade = Some(Trade {
                symbol: self.symbol.clone(),
                direction: Side::Short,
                entry_time: dt,
                entry_price: bid,
                exit_time: None,
                exit_price: None,
                status: TradeStatus::Open,
            });
            event = TrackerEvent::Entered {
                symbol: self.symbol.clone(),
                direction: Side::Short,
                price: bid,
            };
        } else if !self.in_position && self.waiting_for_reentry {
            if let Some(ep) = self.entry_price {
                if bid < ep {
                    self.in_position = true;
                    self.waiting_for_reentry = false;
                    self.active_trade = Some(Trade {
                        symbol: self.symbol.clone(),
                        direction: Side::Short,
                        entry_time: dt,
                        entry_price: bid,
                        exit_time: None,
                        exit_price: None,
                        status: TradeStatus::Open,
                    });
                    event = TrackerEvent::Entered {
                        symbol: self.symbol.clone(),
                        direction: Side::Short,
                        price: bid,
                    };
                }
            }
        }

        if self.in_position {
            if let Some(sl) = self.sl {
                if ask >= sl {
                    self.in_position = false;
                    self.waiting_for_reentry = true;
                    if let Some(mut t) = self.active_trade.take() {
                        t.exit_time = Some(dt);
                        t.exit_price = Some(sl);
                        t.status = TradeStatus::Closed;
                        self.closed_trades.push(t);
                    }
                    event = TrackerEvent::Exited {
                        symbol: self.symbol.clone(),
                    };
                }
            }
        }

        event
    }

    pub fn update_ticker(&mut self, bid: f64, ask: f64) -> TrackerEvent {
        self.current_bid = Some(bid);
        self.current_ask = Some(ask);
        self.current_price = Some(match self.direction {
            Side::Long => bid,
            Side::Short => ask,
        });
        let mut event = TrackerEvent::None;
        let mut exited = false;

        if self.entry_price.is_none() {
            let (entry, sl_val) = match self.direction {
                Side::Long => (ask, ask * 0.995),
                Side::Short => (bid, bid * 1.005),
            };
            self.entry_price = Some(entry);
            self.sl = Some(sl_val);
            self.in_position = true;
            self.active_trade = Some(Trade {
                symbol: self.symbol.clone(),
                direction: self.direction,
                entry_time: Utc::now(),
                entry_price: entry,
                exit_time: None,
                exit_price: None,
                status: TradeStatus::Open,
            });
            event = TrackerEvent::Entered {
                symbol: self.symbol.clone(),
                direction: self.direction,
                price: entry,
            };
        }

        if self.in_position {
            if let Some(sl) = self.sl {
                let hit = match self.direction {
                    Side::Long => bid <= sl,
                    Side::Short => ask >= sl,
                };
                if hit {
                    self.in_position = false;
                    self.waiting_for_reentry = true;
                    if let Some(mut t) = self.active_trade.take() {
                        t.exit_time = Some(Utc::now());
                        t.exit_price = Some(sl);
                        t.status = TradeStatus::Closed;
                        self.closed_trades.push(t);
                    }
                    exited = true;
                    event = TrackerEvent::Exited {
                        symbol: self.symbol.clone(),
                    };
                }
            }
        }

        if !self.in_position && self.waiting_for_reentry && !exited {
            if let Some(ep) = self.entry_price {
                let reenter = match self.direction {
                    Side::Long => ask > ep,
                    Side::Short => bid < ep,
                };
                if reenter {
                    self.in_position = true;
                    self.waiting_for_reentry = false;
                    self.active_trade = Some(Trade {
                        symbol: self.symbol.clone(),
                        direction: self.direction,
                        entry_time: Utc::now(),
                        entry_price: match self.direction {
                            Side::Long => ask,
                            Side::Short => bid,
                        },
                        exit_time: None,
                        exit_price: None,
                        status: TradeStatus::Open,
                    });
                    event = TrackerEvent::Entered {
                        symbol: self.symbol.clone(),
                        direction: self.direction,
                        price: match self.direction {
                            Side::Long => ask,
                            Side::Short => bid,
                        },
                    };
                }
            }
        }

        event
    }

    pub fn unrealized_pnl(&self) -> Option<f64> {
        match (self.current_price, self.entry_price, self.in_position) {
            (Some(price), Some(entry), true) => Some(match self.direction {
                Side::Long => (price - entry) / entry * 100.0,
                Side::Short => (entry - price) / entry * 100.0,
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum TrackerEvent {
    Entered { symbol: String, direction: Side, price: f64 },
    Exited { symbol: String },
    None,
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

    pub fn add_tracker(self: &Arc<Self>, symbol: &str, direction: Side) -> bool {
        let symbol = symbol.to_uppercase();

        let (bid, ask) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(trade_manager::fetch_ticker(&symbol))
                .ok()
        }).unwrap_or((0.0, 0.0));

        let now = Utc::now();
        let entered = {
            let mut trackers = self.trackers.write().unwrap();
            if let Some(old) = trackers.get(&symbol) {
                if old.direction == direction {
                    return false;
                }
            }
            let mut tr = SymbolTracker::new(symbol.clone(), direction);
            let entered = if bid > 0.0 && ask > 0.0 {
                let (entry, sl_val) = match direction {
                    Side::Long => (ask, ask * 0.995),
                    Side::Short => (bid, bid * 1.005),
                };
                tr.entry_price = Some(entry);
                tr.sl = Some(sl_val);
                tr.in_position = true;
                tr.current_price = Some(match direction {
                    Side::Long => bid,
                    Side::Short => ask,
                });
                tr.current_bid = Some(bid);
                tr.current_ask = Some(ask);
                tr.active_trade = Some(Trade {
                    symbol: symbol.clone(),
                    direction,
                    entry_time: now,
                    entry_price: entry,
                    exit_time: None,
                    exit_price: None,
                    status: TradeStatus::Open,
                });
                tr.klines.push(KlineData {
                    ts: now.timestamp_millis(),
                    datetime: now,
                    open: entry,
                    high: entry,
                    low: entry,
                    close: entry,
                    volume: 0.0,
                });
                true
            } else {
                false
            };
            trackers.insert(symbol.clone(), tr);
            entered
        };

        if entered {
            let ep = match direction {
                Side::Long => ask,
                Side::Short => bid,
            };
            self.on_enter_position(symbol.clone(), direction, ep);
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

    pub fn add_subscriber(&self, sub_id: i32, side: Side, size: f64) -> bool {
        let mut subs = self.subscribers.write().unwrap();
        subs.insert(sub_id, Subscriber { sub_id, side, size });
        println!("[Engine] Subscriber #{sub_id} added: {} {}", side.upper(), size);
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

    fn on_enter_position(self: &Arc<Self>, symbol: String, direction: Side, price: f64) {
        let subs = {
            let subs = self.subscribers.read().unwrap();
            subs.values().cloned().collect::<Vec<_>>()
        };
        let tr = self.get_tracker(&symbol);
        let price = tr.as_ref().and_then(|t| t.current_price).unwrap_or(price);
        for sub in subs {
            if sub.side == direction {
                let qty = if price > 0.0 { sub.size / price } else { sub.size };
                let sym = symbol.clone();
                let sid = sub.sub_id;
                tokio::spawn(async move {
                    match direction {
                        Side::Long => trade_manager::open_long(sym, qty, sid).await,
                        Side::Short => trade_manager::open_short(sym, qty, sid).await,
                    }
                });
            }
        }
    }

    fn on_exit_position(self: &Arc<Self>, symbol: String) {
        let subs = {
            let subs = self.subscribers.read().unwrap();
            subs.values().cloned().collect::<Vec<_>>()
        };
        for sub in subs {
            let sym = symbol.clone();
            let sid = sub.sub_id;
            tokio::spawn(async move {
                trade_manager::close_all_positions(sym, sid).await;
            });
        }
    }

    pub fn resolve_symbol(sym: &str) -> String {
        let s = sym.trim().to_uppercase();
        if s.contains('/') {
            return s
                .replace("/USDT:USDT", "")
                .replace("/USDT", "");
        }
        if s.ends_with("USDT") && s.len() > 4 {
            s
        } else {
            format!("{s}USDT")
        }
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
                    let event = {
                        let mut trackers = self.trackers.write().unwrap();
                        trackers
                            .get_mut(sym)
                            .map(|tr| tr.process_kline(&kl))
                            .unwrap_or(TrackerEvent::None)
                    };
                    match event {
                        TrackerEvent::Entered { symbol: s, direction: d, price } => {
                            self.on_enter_position(s, d, price);
                        }
                        TrackerEvent::Exited { symbol: s } => {
                            self.on_exit_position(s);
                        }
                        TrackerEvent::None => {}
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

            let (bid, ask) = match (bid, ask) {
                (Some(b), Some(a)) => (b, a),
                _ => {
                    let mp = data
                        .get("markPrice")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    match mp {
                        Some(p) => (p, p),
                        None => return,
                    }
                }
            };

            let event = {
                let mut trackers = self.trackers.write().unwrap();
                trackers
                    .get_mut(sym)
                    .map(|tr| tr.update_ticker(bid, ask))
                    .unwrap_or(TrackerEvent::None)
            };
            match event {
                TrackerEvent::Entered { symbol: s, direction: d, price } => {
                    self.on_enter_position(s, d, price);
                }
                TrackerEvent::Exited { symbol: s } => {
                    self.on_exit_position(s);
                }
                TrackerEvent::None => {}
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
        let datetime =
            DateTime::from_timestamp(secs, nsecs).unwrap_or(DateTime::from_timestamp(0, 0).unwrap());

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
                                .flat_map(|s| vec![format!("kline.1.{s}"), format!("tickers.{s}")])
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
                                    "args": [format!("kline.1.{sym}"), format!("tickers.{sym}")]
                                }),
                                WsCommand::Unsubscribe(sym) => serde_json::json!({
                                    "op": "unsubscribe",
                                    "args": [format!("kline.1.{sym}"), format!("tickers.{sym}")]
                                }),
                                WsCommand::ResubscribeAll(syms) => {
                                    let args: Vec<String> = syms
                                        .iter()
                                        .flat_map(|s| vec![format!("kline.1.{s}"), format!("tickers.{s}")])
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

fn side_color(s: &str) -> &'static str {
    match s {
        "LONG" => "green",
        "SHORT" => "red",
        _ => "",
    }
}

fn pnl_class(pnl: f64) -> &'static str {
    if pnl >= 0.0 { "green" } else { "red" }
}

fn pnl_color(pnl: f64) -> &'static str {
    if pnl >= 0.0 { "#4caf50" } else { "#ef5350" }
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
        let mut all_closed: Vec<Trade> = Vec::new();
        let mut summary = DashboardSummary::default();

        for (sym, tr) in &trackers {
            if let Some(ref t) = tr.active_trade {
                let live_price = tr.current_price.unwrap_or(t.entry_price);
                let unrealized = match t.direction {
                    Side::Long => (live_price - t.entry_price) / t.entry_price * 100.0,
                    Side::Short => (t.entry_price - live_price) / t.entry_price * 100.0,
                };
                let et = t.entry_time.format("%H:%M:%S");
                let dir_cls = side_color(t.direction.upper());
                active_rows.push_str(&format!(
                    "<tr><td>{sym}</td><td class=\"{dir_cls}\">{}</td><td>{et}</td><td>{:.4}</td><td style=\"color:{}\">{:+.2}%</td><td><a href=\"/viewtrades/{sym}\">Chart</a></td></tr>\n",
                    t.direction.upper(),
                    t.entry_price,
                    pnl_color(unrealized),
                    unrealized,
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
            let a_t = a.exit_time.unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
            let b_t = b.exit_time.unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
            b_t.cmp(&a_t)
        });

        for t in all_closed.iter().take(50) {
            let et = t.entry_time.format("%H:%M:%S");
            let ext = t
                .exit_time
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let dir_cls = side_color(t.direction.upper());
            let pnl = t.pnl_pct();
            let pnl_c = pnl_color(pnl);
            closed_rows.push_str(&format!(
                "<tr><td>{}</td><td class=\"{dir_cls}\">{}</td><td>{et}</td><td>{ext}</td><td>{:.4}</td><td>{:.4}</td><td style=\"color:{pnl_c}\">{pnl:+.2}%</td></tr>\n",
                t.symbol,
                t.direction.upper(),
                t.entry_price,
                t.exit_price.unwrap_or(0.0),
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
                    "<span class=\"summary-card\" style=\"padding:8px 16px;min-width:100px\"><div class=\"num\" style=\"font-size:1.2em\">#{}</div><div class=\"lbl\">{} {}</div></span>",
                    sub.sub_id, sub.side.upper(), sub.size
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let sub_cards = if sub_cards.is_empty() {
            "<span style=\"color:#8b949e;font-size:0.9em;padding:8px 0\">No subscriptions</span>".to_string()
        } else {
            sub_cards
        };

        let sub_table_rows: String = subscribers
            .iter()
            .map(|sub| {
                let cls = side_color(sub.side.upper());
                format!(
                    "<tr><td>#{}</td><td class=\"{cls}\">{}</td><td>{} USDT</td><td><a href=\"/removeSubscriber?subID={}\" style=\"color:#ef5350\">remove</a></td></tr>\n",
                    sub.sub_id, sub.side.upper(), sub.size, sub.sub_id
                )
            })
            .collect();

        let symlist: String = trackers
            .iter()
            .map(|(sym, tr)| {
                let pnl_str = if tr.in_position {
                    if let Some(upnl) = tr.unrealized_pnl() {
                        let cls = pnl_class(upnl);
                        format!("<span class=\"{cls}\">{upnl:+.2}%</span>")
                    } else {
                        "<span style=\"color:#8b949e\">flat</span>".to_string()
                    }
                } else {
                    "<span style=\"color:#8b949e\">flat</span>".to_string()
                };
                format!(
                    "<li><b>{sym}</b> ({}) pnl: {pnl_str} | attempts: {} | <a href=\"/viewtrades/{sym}\">chart</a> | <a href=\"/removeSymbol/{sym}\" style=\"color:#ef5350;font-size:0.8em\">remove</a></li>",
                    tr.direction.upper(),
                    tr.total_attempts(),
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
            "<tr><td colspan=\"4\" style=\"text-align:center;color:#8b949e\">No subscriptions active</td></tr>".to_string()
        } else {
            sub_table_rows
        };

        let active_display = if active_rows.is_empty() {
            "<tr><td colspan=\"6\" style=\"text-align:center;color:#8b949e\">No active trades</td></tr>".to_string()
        } else {
            active_rows
        };

        let closed_display = if closed_rows.is_empty() {
            "<tr><td colspan=\"7\" style=\"text-align:center;color:#8b949e\">No closed trades</td></tr>".to_string()
        } else {
            closed_rows
        };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta http-equiv="refresh" content="5">
<title>Realtime Engine Dashboard</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:20px }}
  h1,h2,h3 {{ color:#f0f6fc }}
  .summary {{ display:flex; gap:16px; flex-wrap:wrap; margin:16px 0 24px }}
  .summary-card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px 22px; min-width:120px }}
  .summary-card .num {{ font-size:1.6em; font-weight:700 }}
  .summary-card .lbl {{ font-size:0.8em; color:#8b949e }}
  table {{ width:100%; border-collapse:collapse; margin:10px 0 24px; font-size:0.9em }}
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
  .btn-amber {{ border-color:#f0883e; color:#f0883e }}
  hr {{ border:none; border-top:1px solid #30363d; margin:24px 0 }}
</style>
</head>
<body>
<h1>Realtime Trade Engine</h1>

<div class="summary">
  <div class="summary-card"><div class="num">{active}</div><div class="lbl">Active</div></div>
  <div class="summary-card"><div class="num">{total_closed}</div><div class="lbl">Closed</div></div>
  <div class="summary-card"><div class="num">{wins}</div><div class="lbl">Wins</div></div>
  <div class="summary-card"><div class="num">{losses}</div><div class="lbl">Losses</div></div>
  <div class="summary-card"><div class="num">{wr:.1}%</div><div class="lbl">Win Rate</div></div>
  <div class="summary-card"><div class="num" style="color:{total_pnl_cls}">{total_pnl:+.2}%</div><div class="lbl">Realized PnL</div></div>
  <div class="summary-card"><div class="num" style="color:{upnl_cls}">{unrealized_pnl:+.2}%</div><div class="lbl">Unrealized PnL</div></div>
  <div class="summary-card"><div class="num" style="color:{combined_cls}">{combined:+.2}%</div><div class="lbl">Total (Realized+Unreal)</div></div>
</div>

<h3>Subscriptions</h3>
<div class="summary">{sub_cards}</div>

<h3>Subscription Details</h3>
<table><thead><tr><th>ID</th><th>Side</th><th>Size</th><th></th></tr></thead><tbody>
{sub_table}
</tbody></table>

<div class="actions" style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 16px;margin:12px 0">
  <h4>Manage Subscriptions</h4>
  <form action="/subscribeLong" method="get">
    <input name="subID" placeholder="SubID" style="width:60px" required>
    <input name="size" placeholder="USDT" style="width:70px" value="5">
    <button type="submit" class="btn-green">Subscribe Long</button>
  </form>
  <form action="/subscribeShort" method="get">
    <input name="subID" placeholder="SubID" style="width:60px" required>
    <input name="size" placeholder="USDT" style="width:70px" value="5">
    <button type="submit" class="btn-red">Subscribe Short</button>
  </form>
  <form action="/removeSubscriber" method="get">
    <input name="subID" placeholder="SubID" style="width:60px" required>
    <button type="submit" style="color:#ef5350;border-color:#ef5350">Remove Sub</button>
  </form>
</div>

<div class="actions">
  <form action="/addLong" method="get">
    <input name="symbol" placeholder="BTCUSDT" style="width:110px" required>
    <button type="submit" class="btn-green">+ Watch Long</button>
  </form>
  <form action="/addShort" method="get">
    <input name="symbol" placeholder="BTCUSDT" style="width:110px" required>
    <button type="submit" class="btn-red">+ Watch Short</button>
  </form>
  <a href="/clear"><button type="button" style="color:#ef5350;border-color:#ef5350">Clear All</button></a>
  <a href="/flush"><button type="button" style="color:#8b949e;border-color:#8b949e">Reset WS</button></a>
</div>

<h2>Active Trades</h2>
<table><thead><tr><th>Symbol</th><th>Dir</th><th>Entry Time</th><th>Entry</th><th>Unrealized</th><th>View</th></tr></thead><tbody>
{active_display}
</tbody></table>

<h2>Closed Trades <span style="font-size:0.7em;color:#8b949e">(last 50)</span></h2>
<table><thead><tr><th>Symbol</th><th>Dir</th><th>Entry</th><th>Exit</th><th>Entry $</th><th>Exit $</th><th>PnL</th></tr></thead><tbody>
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
                let cls = side_color(sub.side.upper());
                format!(
                    "<tr><td>#{}</td><td class=\"{cls}\">{}</td><td>{} USDT</td><td><a href=\"/removeSubscriber?subID={}\" style=\"color:#ef5350\">remove</a></td></tr>\n",
                    sub.sub_id, sub.side.upper(), sub.size, sub.sub_id
                )
            })
            .collect();

        let sym_rows: String = trackers
            .iter()
            .map(|(sym, tr)| {
                let d_cls = side_color(tr.direction.upper());
                let state = if tr.in_position { "IN" } else { "flat" };
                let state_color = if tr.in_position { "#4caf50" } else { "#8b949e" };
                format!(
                    "<tr><td><a href=\"/viewtrades/{sym}\">{sym}</a></td><td class=\"{d_cls}\">{}</td><td style=\"color:{state_color}\">{state}</td><td>{} attempts</td><td><a href=\"/removeSymbol/{sym}\" style=\"color:#ef5350\">remove</a></td></tr>\n",
                    tr.direction.upper(),
                    tr.total_attempts(),
                )
            })
            .collect();

        let sub_display = if sub_rows.is_empty() {
            "<tr><td colspan=\"4\" style=\"text-align:center;color:#8b949e\">No subscriptions</td></tr>".to_string()
        } else {
            sub_rows
        };

        let sym_display = if sym_rows.is_empty() {
            "<tr><td colspan=\"5\" style=\"text-align:center;color:#8b949e\">No symbols watched</td></tr>".to_string()
        } else {
            sym_rows
        };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Manage — Realtime Engine</title>
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
  .btn-amber {{ border-color:#f0883e; color:#f0883e }}
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
<h1>Manage Engine</h1>

<div class="card">
  <h3>Subscribe to Signals <span class="badge">{subs_count} active</span></h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    Subscribers are symbol-independent. When any watched symbol enters a position,
    all matching subscribers place orders on that symbol with their configured USDT size.
  </p>
  <form action="/subscribeLong" method="get">
    <input type="hidden" name="redirect" value="subs">
    <label>Sub ID:</label><input name="subID" placeholder="e.g. 1" style="width:70px" required>
    <label>USDT:</label><input name="size" placeholder="5" style="width:70px" value="5">
    <button type="submit" class="btn-green">Subscribe Long</button>
  </form>
  <form action="/subscribeShort" method="get">
    <input type="hidden" name="redirect" value="subs">
    <label>Sub ID:</label><input name="subID" placeholder="e.g. 1" style="width:70px" required>
    <label>USDT:</label><input name="size" placeholder="5" style="width:70px" value="5">
    <button type="submit" class="btn-red">Subscribe Short</button>
  </form>
  <form action="/removeSubscriber" method="get">
    <label>Sub ID:</label><input name="subID" placeholder="e.g. 1" style="width:70px" required>
    <button type="submit" style="color:#ef5350;border-color:#ef5350">Remove Subscriber</button>
  </form>
</div>

<h3>Active Subscriptions</h3>
<table><thead><tr><th>ID</th><th>Side</th><th>Size</th><th></th></tr></thead><tbody>
{sub_display}
</tbody></table>

<div class="card">
  <h3>Watch Symbols <span class="badge">{syms_count} watching</span></h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    Add symbols to watch. The engine tracks price action and fires signals.
  </p>
  <form action="/addLong" method="get">
    <label>Symbol:</label><input name="symbol" placeholder="BTCUSDT" style="width:120px" required>
    <button type="submit" class="btn-green">Watch Long</button>
  </form>
  <form action="/addShort" method="get">
    <label>Symbol:</label><input name="symbol" placeholder="BTCUSDT" style="width:120px" required>
    <button type="submit" class="btn-red">Watch Short</button>
  </form>
</div>

<h3>Watched Symbols</h3>
<table><thead><tr><th>Symbol</th><th>Direction</th><th>State</th><th>Trades</th><th></th></tr></thead><tbody>
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
        let state_bold = if tr.in_position { "IN POSITION" } else { "FLAT" };
        let state_cls = if tr.in_position { "green" } else { "red" };
        let entry_str = tr
            .entry_price
            .map(|p| format!("{:.4}", p))
            .unwrap_or_else(|| "-".to_string());
        let sl_str = tr
            .sl
            .map(|p| format!("{:.4}", p))
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
                "<tr><td>{}</td><td>{et}</td><td>{ext}</td><td>{:.4}</td><td>{:.4}</td><td class=\"{clr}\">{pnl:+.2}%</td><td>{dur}</td></tr>\n",
                i + 1,
                t.entry_price,
                t.exit_price.unwrap_or(0.0),
            ));
        }
        if closed_rows.is_empty() {
            closed_rows = "<tr><td colspan=\"7\" style=\"color:#8b949e;text-align:center\">No closed trades</td></tr>".to_string();
        }

        Some(format!(
            r#"<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{symbol} Trades</title>
<style>
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:20px }}
  h1,h2 {{ color:#f0f6fc }}
  table {{ border-collapse:collapse; margin:12px 0 }}
  th,td {{ padding:6px 12px; border:1px solid #30363d; text-align:left }}
  th {{ background:#21262d; color:#8b949e }}
  .green {{ color:#4caf50 }} .red {{ color:#ef5350 }}
  a {{ color:#58a6ff }}
</style></head><body>
<h1>{symbol} &mdash; {dir}</h1>
<p>State: <b class="{state_cls}">{state_bold}</b>
 | Entry: {entry_str}
 | SL: {sl_str}
 | Attempts: {attempts}
 | Total PnL: <span class="{pnl_cls}">{total_pnl:+.2}%</span></p>

{chart}

<h2>Closed Trades</h2>
<table><thead><tr><th>#</th><th>Entry Time</th><th>Exit Time</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Duration</th></tr></thead><tbody>
{closed_rows}
</tbody></table>
<p><a href="/">Back to dashboard</a></p>
</body></html>"#,
            symbol = symbol,
            dir = tr.direction.upper(),
            state_cls = state_cls,
            state_bold = state_bold,
            entry_str = entry_str,
            sl_str = sl_str,
            attempts = tr.total_attempts(),
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
            lines.push(format!(
                "<b>{sym}</b> ({}) | entry={} | current={} | sl={} | in_pos={} | waiting={} | klines={} | closed_trades={} | ws_connected={}",
                tr.direction.upper(),
                tr.entry_price.map(|p| format!("{p}")).unwrap_or_else(|| "None".to_string()),
                tr.current_price.map(|p| format!("{p}")).unwrap_or_else(|| "None".to_string()),
                tr.sl.map(|p| format!("{p}")).unwrap_or_else(|| "None".to_string()),
                tr.in_position,
                tr.waiting_for_reentry,
                tr.klines.len(),
                tr.closed_trades.len(),
                ws_connected,
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
}

// ─── SVG Chart Generation ────────────────────────────────────────────────

fn generate_chart_svg(tr: &SymbolTracker) -> String {
    let klines = &tr.klines;
    if klines.len() < 2 {
        return "<p style=\"color:gray\">Not enough data yet.</p>".to_string();
    }

    let direction = tr.direction;
    let entry_price = tr.entry_price.unwrap_or(klines[0].close);
    let sl = entry_price * if direction == Side::Long { 0.995 } else { 1.005 };

    let mut entries: Vec<usize> = Vec::new();
    let mut stops: Vec<usize> = Vec::new();
    let mut in_pos = false;
    let mut waiting = false;
    let mut ep = klines[0].close;
    let mut current_sl = ep * if direction == Side::Long { 0.995 } else { 1.005 };
    let mut pos_state = vec![false; klines.len()];

    for i in 0..klines.len() {
        let low = klines[i].low;
        let high = klines[i].high;
        let close = klines[i].close;

        if direction == Side::Long {
            if i == 0 || (!in_pos && !waiting) {
                in_pos = true;
                entries.push(i);
                ep = close;
                current_sl = ep * 0.995;
            } else if !in_pos && waiting && close > ep {
                in_pos = true;
                waiting = false;
                entries.push(i);
                ep = close;
                current_sl = ep * 0.995;
            }
            if in_pos && low <= current_sl {
                in_pos = false;
                waiting = true;
                stops.push(i);
            }
        } else {
            if i == 0 || (!in_pos && !waiting) {
                in_pos = true;
                entries.push(i);
                ep = close;
                current_sl = ep * 1.005;
            } else if !in_pos && waiting && close < ep {
                in_pos = true;
                waiting = false;
                entries.push(i);
                ep = close;
                current_sl = ep * 1.005;
            }
            if in_pos && high >= current_sl {
                in_pos = false;
                waiting = true;
                stops.push(i);
            }
        }
        pos_state[i] = in_pos;
    }

    let chart_w = 800.0;
    let chart_h = 350.0;
    let pnl_h = 130.0;
    let margin_l = 60.0;
    let margin_r = 20.0;
    let margin_t = 30.0;
    let margin_b = 30.0;
    let plot_w = chart_w - margin_l - margin_r;
    let plot_h = chart_h - margin_t - margin_b;

    let mut min_price = f64::MAX;
    let mut max_price = f64::MIN;
    for k in klines {
        if k.low < min_price { min_price = k.low; }
        if k.high > max_price { max_price = k.high; }
    }
    let price_pad = (max_price - min_price) * 0.05;
    min_price -= price_pad;
    max_price += price_pad;
    if (max_price - min_price).abs() < 1e-10 {
        max_price = min_price + 1.0;
    }

    let to_x = |i: usize| -> f64 {
        if klines.len() <= 1 { return margin_l + plot_w / 2.0; }
        margin_l + (i as f64 / (klines.len() - 1) as f64) * plot_w
    };
    let to_y = |price: f64| -> f64 {
        margin_t + plot_h * (1.0 - (price - min_price) / (max_price - min_price))
    };

    let ep0 = tr.entry_price.unwrap_or(klines[0].close);
    let mut pnl_line: Vec<f64> = Vec::new();
    for i in 0..klines.len() {
        let pnl = if pos_state[i] {
            match direction {
                Side::Long => (klines[i].close - ep0) / ep0 * 100.0,
                Side::Short => (ep0 - klines[i].close) / ep0 * 100.0,
            }
        } else {
            *pnl_line.last().unwrap_or(&0.0)
        };
        pnl_line.push(pnl);
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
  .line-entry {{ stroke:#2196f3;stroke-dasharray:4,3;stroke-width:1 }}
  .line-sl {{ stroke:#ff9800;stroke-dasharray:4,3;stroke-width:1 }}
  .marker-entry {{ fill:#4caf50;stroke:#000;stroke-width:0.5 }}
  .marker-entry-s {{ fill:#ef5350;stroke:#000;stroke-width:0.5 }}
  .marker-stop {{ fill:#ef5350;stroke:#000;stroke-width:0.5 }}
  .pnl-line {{ fill:none;stroke:#c9d1d9;stroke-width:1 }}
  .pnl-fill-p {{ fill:rgba(76,175,80,0.3) }}
  .pnl-fill-n {{ fill:rgba(239,83,80,0.3) }}
  .axis-text {{ fill:#8b949e;font-size:10px;font-family:monospace }}
</style>
"###,
    ));

    // grid lines
    let step = (klines.len() / 10).max(1);
    for i in (0..klines.len()).step_by(step) {
        let x = to_x(i);
        s.push_str(&format!(
            r###"<line x1="{x}" y1="{ty}" x2="{x}" y2="{by}" stroke="#30363d" stroke-width="0.5"/>"###,
            ty = margin_t,
            by = margin_t + plot_h
        ));
    }
    for pct in [0.0, 0.25, 0.5, 0.75, 1.0] {
        let price = min_price + (max_price - min_price) * pct;
        let y = to_y(price);
        s.push_str(&format!(
            r###"<line x1="{ml}" y1="{y}" x2="{mr}" y2="{y}" stroke="#30363d" stroke-width="0.5"/>"###,
            ml = margin_l,
            mr = margin_l + plot_w
        ));
        s.push_str(&format!(
            r###"<text x="{tx}" y="{y}" class="axis-text" text-anchor="end" dominant-baseline="middle">{price:.2}</text>"###,
            tx = margin_l - 5.0,
        ));
    }

    // candlesticks
    let max_w = if klines.len() > 1 {
        (plot_w / klines.len() as f64) * 0.6
    } else {
        5.0
    };
    let bw = max_w.max(1.0).min(10.0);

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

    // entry line
    let entry_y = to_y(entry_price);
    s.push_str(&format!(
        r###"<line x1="{ml}" y1="{entry_y}" x2="{mr}" y2="{entry_y}" class="line-entry"/>"###,
        ml = margin_l,
        mr = margin_l + plot_w
    ));
    s.push_str(&format!(
        r###"<text x="{tx}" y="{entry_y}" fill="#2196f3" font-size="9" font-family="monospace" dominant-baseline="hanging">{entry_price:.2}</text>"###,
        tx = margin_l + plot_w + 3.0
    ));

    // sl line
    let sl_y = to_y(sl);
    s.push_str(&format!(
        r###"<line x1="{ml}" y1="{sl_y}" x2="{mr}" y2="{sl_y}" class="line-sl"/>"###,
        ml = margin_l,
        mr = margin_l + plot_w
    ));

    // entry markers
    for &ei in &entries {
        let x = to_x(ei);
        let p = klines[ei].close;
        let y = to_y(p);
        let marker_cls = if direction == Side::Long { "marker-entry" } else { "marker-entry-s" };
        if direction == Side::Long {
            let y1 = y - 8.0;
            let xl = x - 5.0;
            let y2 = y + 2.0;
            let xr = x + 5.0;
            s.push_str(&format!(r###"<polygon points="{x},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}"/>"###));
        } else {
            let y1 = y + 8.0;
            let xl = x - 5.0;
            let y2 = y - 2.0;
            let xr = x + 5.0;
            s.push_str(&format!(r###"<polygon points="{x},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}"/>"###));
        }
    }

    // stop markers
    for &si in &stops {
        let x = to_x(si);
        let y = to_y(current_sl);
        let x1 = x - 4.0;
        let y1 = y - 4.0;
        let x2 = x + 4.0;
        let y2 = y + 4.0;
        s.push_str(&format!(
            r###"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="marker-stop" stroke-width="1.5"/>
<line x1="{x2}" y1="{y1}" x2="{x1}" y2="{y2}" class="marker-stop" stroke-width="1.5"/>"###
        ));
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
        let fill_cls = if pnl_line[i] >= 0.0 { "pnl-fill-p" } else { "pnl-fill-n" };
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
