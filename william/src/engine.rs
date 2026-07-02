use crate::trade_manager;
use chrono::{DateTime, Duration, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ─── Alligator Constants ─────────────────────────────────────────────────

const JAW_PERIOD: usize = 13;
const TEETH_PERIOD: usize = 8;
const LIPS_PERIOD: usize = 5;
const JAW_DISPLACEMENT: usize = 8;
const TEETH_DISPLACEMENT: usize = 5;
const LIPS_DISPLACEMENT: usize = 3;
const MAX_CLOSE_POOL: usize = 500;

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
    pub direction: TradeDirection,
    pub entry_time: DateTime<Utc>,
    pub entry_price: f64,
    pub sl: f64,
    pub tp: f64,
    pub exit_time: Option<DateTime<Utc>>,
    pub exit_price: Option<f64>,
    pub status: TradeStatus,
    pub is_ghost: bool,
    pub signal_label: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradeStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AlligatorAlignment {
    Bullish,
    Bearish,
    Mixed,
}

impl AlligatorAlignment {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlligatorAlignment::Bullish => "BULLISH",
            AlligatorAlignment::Bearish => "BEARISH",
            AlligatorAlignment::Mixed => "MIXED",
        }
    }
}

impl GridTrade {
    fn pnl_pct(&self) -> f64 {
        if self.is_ghost || self.status == TradeStatus::Open || self.exit_price.is_none() {
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
pub struct AlligatorScore {
    pub symbol: String,
    pub base_price: f64,
    pub total_signals: u32,
    pub total_trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub total_pnl_pct: f64,
    pub win_rate: f64,
    pub max_drawdown_pct: f64,
    pub suitability: f64,
}

// ─── SMMA (Smoothed Moving Average) ──────────────────────────────────────

fn calc_smma(values: &VecDeque<f64>, period: usize) -> VecDeque<f64> {
    if values.len() < period {
        return VecDeque::new();
    }
    let mut result = VecDeque::with_capacity(values.len() - period + 1);
    let sum: f64 = values.iter().take(period).sum();
    result.push_back(sum / period as f64);
    for i in period..values.len() {
        let prev = result.back().copied().unwrap();
        let val = (prev * (period as f64 - 1.0) + values[i]) / period as f64;
        result.push_back(val);
    }
    result
}

fn get_displaced(smma: &VecDeque<f64>, displacement: usize) -> Option<f64> {
    if smma.len() <= displacement {
        return None;
    }
    smma.get(smma.len() - 1 - displacement).copied()
}

fn determine_alignment(lips: f64, teeth: f64, jaw: f64) -> AlligatorAlignment {
    if lips > teeth && teeth > jaw {
        AlligatorAlignment::Bullish
    } else if lips < teeth && teeth < jaw {
        AlligatorAlignment::Bearish
    } else {
        AlligatorAlignment::Mixed
    }
}

// ─── SymbolTracker ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SymbolTracker {
    pub symbol: String,
    pub base_price: f64,
    pub klines: Vec<KlineData>,
    pub current_price: Option<f64>,
    pub active_trade: Option<GridTrade>,
    pub closed_trades: Vec<GridTrade>,
    pub ghost_remaining: i32,
    pub ghost_triggered: bool,
    pub close_prices: VecDeque<f64>,
    pub jaw_values: VecDeque<f64>,
    pub teeth_values: VecDeque<f64>,
    pub lips_values: VecDeque<f64>,
    pub alignment: AlligatorAlignment,
    pub prev_alignment: AlligatorAlignment,
    pub alignment_changed: bool,
    pub jaw_val: f64,
    pub teeth_val: f64,
    pub lips_val: f64,
}

impl SymbolTracker {
    pub fn new(symbol: String, base_price: f64) -> Self {
        SymbolTracker {
            symbol,
            base_price,
            klines: Vec::new(),
            current_price: Some(base_price),
            active_trade: None,
            closed_trades: Vec::new(),
            ghost_remaining: 0,
            ghost_triggered: false,
            close_prices: VecDeque::with_capacity(MAX_CLOSE_POOL),
            jaw_values: VecDeque::new(),
            teeth_values: VecDeque::new(),
            lips_values: VecDeque::new(),
            alignment: AlligatorAlignment::Mixed,
            prev_alignment: AlligatorAlignment::Mixed,
            alignment_changed: false,
            jaw_val: 0.0,
            teeth_val: 0.0,
            lips_val: 0.0,
        }
    }

    pub fn total_attempts(&self) -> usize {
        let closed = self.closed_trades.iter().filter(|t| !t.is_ghost).count();
        let active = self.active_trade.as_ref().map(|t| if t.is_ghost { 0 } else { 1 }).unwrap_or(0);
        closed + active
    }

    fn recalc_alligator(&mut self) {
        if self.close_prices.len() < JAW_PERIOD {
            return;
        }
        let jaw_raw = calc_smma(&self.close_prices, JAW_PERIOD);
        let teeth_raw = calc_smma(&self.close_prices, TEETH_PERIOD);
        let lips_raw = calc_smma(&self.close_prices, LIPS_PERIOD);

        self.jaw_values = jaw_raw;
        self.teeth_values = teeth_raw;
        self.lips_values = lips_raw;

        self.jaw_val = get_displaced(&self.jaw_values, JAW_DISPLACEMENT).unwrap_or(0.0);
        self.teeth_val = get_displaced(&self.teeth_values, TEETH_DISPLACEMENT).unwrap_or(0.0);
        self.lips_val = get_displaced(&self.lips_values, LIPS_DISPLACEMENT).unwrap_or(0.0);

        let has_displaced = self.jaw_values.len() > JAW_DISPLACEMENT
            && self.teeth_values.len() > TEETH_DISPLACEMENT
            && self.lips_values.len() > LIPS_DISPLACEMENT;

        if has_displaced {
            let new_alignment = determine_alignment(self.lips_val, self.teeth_val, self.jaw_val);
            self.prev_alignment = self.alignment;
            self.alignment = new_alignment;
            self.alignment_changed = self.prev_alignment != self.alignment;
        }
    }

    pub fn process_kline(&mut self, kline: &KlineData, sl_pct: f64, tp_pct: f64, _reverse: bool, ghost_threshold: i32) -> Vec<TrackerEvent> {
        let high = kline.high;
        let low = kline.low;
        let close = kline.close;
        let dt = kline.datetime;

        self.klines.push(kline.clone());
        if self.klines.len() > 5000 {
            self.klines.remove(0);
        }
        self.current_price = Some(close);

        self.close_prices.push_back(close);
        if self.close_prices.len() > MAX_CLOSE_POOL {
            self.close_prices.pop_front();
        }

        let mut events = Vec::new();

        // ── SL/TP exit check (price-based, always runs) ────────────
        if let Some(ref trade) = self.active_trade {
            let (should_exit, exit_price) = match trade.direction {
                TradeDirection::Long => {
                    if low <= trade.sl {
                        (true, low)
                    } else if high >= trade.tp {
                        (true, trade.tp)
                    } else {
                        (false, 0.0)
                    }
                }
                TradeDirection::Short => {
                    if high >= trade.sl {
                        (true, high)
                    } else if low <= trade.tp {
                        (true, trade.tp)
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
                if !t.is_ghost {
                    let was_loss = match t.direction {
                        TradeDirection::Long => exit_price < t.entry_price,
                        TradeDirection::Short => exit_price > t.entry_price,
                    };
                    if ghost_threshold > 0 && was_loss && !self.ghost_triggered {
                        self.ghost_remaining = ghost_threshold;
                        self.ghost_triggered = true;
                    } else if !was_loss {
                        self.ghost_remaining = 0;
                        self.ghost_triggered = false;
                    }
                }
                self.closed_trades.push(t.clone());
                events.push(TrackerEvent::Exited {
                    symbol: self.symbol.clone(),
                    direction: t.direction,
                    is_ghost: t.is_ghost,
                });
            }
        }

        // ── Recalculate alligator SMMAs ────────────────────────────
        self.recalc_alligator();

        // ── Alignment-based exit ───────────────────────────────────
        if self.alignment_changed && self.active_trade.is_some() {
            let should_exit = match self.active_trade.as_ref().unwrap().direction {
                TradeDirection::Long => self.alignment != AlligatorAlignment::Bullish,
                TradeDirection::Short => self.alignment != AlligatorAlignment::Bearish,
            };
            if should_exit {
                let mut t = self.active_trade.take().unwrap();
                t.exit_time = Some(dt);
                t.exit_price = Some(close);
                t.status = TradeStatus::Closed;
                if !t.is_ghost {
                    let was_loss = match t.direction {
                        TradeDirection::Long => close < t.entry_price,
                        TradeDirection::Short => close > t.entry_price,
                    };
                    if ghost_threshold > 0 && was_loss && !self.ghost_triggered {
                        self.ghost_remaining = ghost_threshold;
                        self.ghost_triggered = true;
                    } else if !was_loss {
                        self.ghost_remaining = 0;
                        self.ghost_triggered = false;
                    }
                }
                self.closed_trades.push(t.clone());
                events.push(TrackerEvent::Exited {
                    symbol: self.symbol.clone(),
                    direction: t.direction,
                    is_ghost: t.is_ghost,
                });
            }
        }

        // ── Alignment-based entry ──────────────────────────────────
        if self.alignment_changed && self.active_trade.is_none() {
            let should_enter = match self.alignment {
                AlligatorAlignment::Bullish => true,
                AlligatorAlignment::Bearish => true,
                AlligatorAlignment::Mixed => false,
            };
            if should_enter {
                let direction = match self.alignment {
                    AlligatorAlignment::Bullish => TradeDirection::Long,
                    AlligatorAlignment::Bearish => TradeDirection::Short,
                    AlligatorAlignment::Mixed => unreachable!(),
                };
                let is_ghost = self.ghost_remaining > 0;
                if is_ghost {
                    self.ghost_remaining -= 1;
                }
                let entry_price = close;
                let (sl, tp) = match direction {
                    TradeDirection::Long => {
                        (entry_price * (1.0 - sl_pct / 100.0), entry_price * (1.0 + tp_pct / 100.0))
                    }
                    TradeDirection::Short => {
                        (entry_price * (1.0 + sl_pct / 100.0), entry_price * (1.0 - tp_pct / 100.0))
                    }
                };
                let label = format!("{}→{}", self.prev_alignment.as_str(), self.alignment.as_str());
                self.active_trade = Some(GridTrade {
                    symbol: self.symbol.clone(),
                    direction,
                    entry_time: dt,
                    entry_price,
                    sl,
                    tp,
                    exit_time: None,
                    exit_price: None,
                    status: TradeStatus::Open,
                    is_ghost,
                    signal_label: label,
                });
                events.push(TrackerEvent::Entered {
                    symbol: self.symbol.clone(),
                    direction,
                    price: entry_price,
                    sl,
                    tp,
                    is_ghost,
                });
            }
        }

        events
    }

    pub fn update_ticker(&mut self, bid: f64, ask: f64, _sl_pct: f64, _tp_pct: f64, _reverse: bool, _ghost_threshold: i32) -> Vec<TrackerEvent> {
        self.current_price = Some((bid + ask) / 2.0);
        let mut events = Vec::new();

        // SL/TP exit on ticker (safety net — faster than waiting for kline close)
        if let Some(ref trade) = self.active_trade {
            let (should_exit, exit_price) = match trade.direction {
                TradeDirection::Long => {
                    if bid <= trade.sl {
                        (true, bid)
                    } else if bid >= trade.tp {
                        (true, trade.tp)
                    } else {
                        (false, 0.0)
                    }
                }
                TradeDirection::Short => {
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
                if !t.is_ghost {
                    let _was_loss = match t.direction {
                        TradeDirection::Long => exit_price < t.entry_price,
                        TradeDirection::Short => exit_price > t.entry_price,
                    };
                    // ticker-only ghosts handled here (ghost_threshold not passed — use stored)
                    // We keep ghost state from previous kline-based logic
                }
                self.closed_trades.push(t.clone());
                events.push(TrackerEvent::Exited {
                    symbol: self.symbol.clone(),
                    direction: t.direction,
                    is_ghost: t.is_ghost,
                });
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
        sl: f64,
        tp: f64,
        is_ghost: bool,
    },
    Exited {
        symbol: String,
        direction: TradeDirection,
        is_ghost: bool,
    },
}

// ─── WS Commands ──────────────────────────────────────────────────────────

pub enum WsCommand {
    Subscribe(String, i32),
    Unsubscribe(String, i32),
    ResubscribeAll(Vec<String>, i32),
}

// ─── RealtimeEngine ───────────────────────────────────────────────────────

pub struct RealtimeEngine {
    pub trackers: RwLock<HashMap<String, SymbolTracker>>,
    pub subscribers: RwLock<HashMap<i32, Subscriber>>,
    pub ws_sender: Mutex<Option<mpsc::UnboundedSender<WsCommand>>>,
    pub running: AtomicBool,
    pub sl_percent: RwLock<f64>,
    pub tp_percent: RwLock<f64>,
    pub reverse_mode: RwLock<bool>,
    pub ghost_threshold: RwLock<i32>,
    pub interval_minutes: RwLock<i32>,
}

impl RealtimeEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(RealtimeEngine {
            trackers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            ws_sender: Mutex::new(None),
            running: AtomicBool::new(true),
            sl_percent: RwLock::new(1.5),
            tp_percent: RwLock::new(0.5),
            reverse_mode: RwLock::new(false),
            ghost_threshold: RwLock::new(3),
            interval_minutes: RwLock::new(1),
        })
    }

    pub fn set_config(&self, sl: f64, tp: f64) {
        *self.sl_percent.write().unwrap() = sl;
        *self.tp_percent.write().unwrap() = tp;
        println!("[Config] SL={sl}% TP={tp}%");
    }

    pub fn set_reverse_mode(&self, enabled: bool) {
        *self.reverse_mode.write().unwrap() = enabled;
        println!("[Config] Reverse mode={enabled}");
    }

    pub fn set_ghost_threshold(&self, val: i32) {
        *self.ghost_threshold.write().unwrap() = val;
        println!("[Config] Ghost threshold={val}");
    }

    pub fn set_interval(&self, minutes: i32) {
        *self.interval_minutes.write().unwrap() = minutes;
        println!("[Config] Interval set to {minutes}m");
        // trigger resubscribe to all tracked symbols
        let syms: Vec<String> = {
            let trackers = self.trackers.read().unwrap();
            trackers.keys().cloned().collect()
        };
        if !syms.is_empty() {
            self.send_ws_cmd(WsCommand::ResubscribeAll(syms, minutes));
        }
    }

    pub fn get_config(&self) -> (f64, f64, bool, i32, i32) {
        let sl = *self.sl_percent.read().unwrap();
        let tp = *self.tp_percent.read().unwrap();
        let rev = *self.reverse_mode.read().unwrap();
        let ghost = *self.ghost_threshold.read().unwrap();
        let interval = *self.interval_minutes.read().unwrap();
        (sl, tp, rev, ghost, interval)
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

        let interval = *self.interval_minutes.read().unwrap();
        self.send_ws_cmd(WsCommand::Subscribe(symbol, interval));
        true
    }

    pub fn remove_tracker(self: &Arc<Self>, symbol: &str) -> bool {
        let sym = symbol.to_uppercase();
        self.trackers.write().unwrap().remove(&sym);
        let interval = *self.interval_minutes.read().unwrap();
        self.send_ws_cmd(WsCommand::Unsubscribe(sym, interval));
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
        is_ghost: bool,
    ) {
        if is_ghost {
            println!("[Engine] Ghost trade {symbol} {d} — skipped subscription", d = direction.as_str());
            return;
        }
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

    fn on_exit_position(self: &Arc<Self>, symbol: String, _direction: TradeDirection, is_ghost: bool) {
        if is_ghost {
            return;
        }
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

        let (sl_pct, tp_pct, reverse, ghost_threshold) = (
            *self.sl_percent.read().unwrap(),
            *self.tp_percent.read().unwrap(),
            *self.reverse_mode.read().unwrap(),
            *self.ghost_threshold.read().unwrap(),
        );

        if topic.starts_with("kline.") {
            let parts: Vec<&str> = topic.splitn(3, '.').collect();
            if parts.len() != 3 {
                return;
            }
            let sym = parts[2];
            if data.get("confirm").and_then(|c| c.as_bool()) == Some(true) {
                let kline = self.parse_kline(&data);
                if let Some(kl) = kline {
                    let events = {
                        let mut trackers = self.trackers.write().unwrap();
                        trackers
                            .get_mut(sym)
                            .map(|tr| tr.process_kline(&kl, sl_pct, tp_pct, reverse, ghost_threshold))
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
                                is_ghost,
                                ..
                            } => {
                                self.on_enter_position(s, d, price, sl, tp, is_ghost);
                            }
                            TrackerEvent::Exited { symbol: s, direction: d, is_ghost, .. } => {
                                self.on_exit_position(s, d, is_ghost);
                            }
                        }
                    }
                }
            }
        } else if topic.starts_with("tickers.") {
            let sym = &topic[8..];
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
                        .map(|tr| tr.update_ticker(b, a, sl_pct, tp_pct, reverse, ghost_threshold))
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
                            is_ghost,
                            ..
                        } => {
                            self.on_enter_position(s, d, price, sl, tp, is_ghost);
                        }
                        TrackerEvent::Exited { symbol: s, direction: d, is_ghost, .. } => {
                            self.on_exit_position(s, d, is_ghost);
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
                        let interval = *self.interval_minutes.read().unwrap();
                        let syms: Vec<String> = {
                            let trackers = self.trackers.read().unwrap();
                            trackers.keys().cloned().collect()
                        };
                        if !syms.is_empty() {
                            let args: Vec<String> = syms
                                .iter()
                                .flat_map(|s| {
                                    vec![format!("kline.{interval}.{s}"), format!("tickers.{s}")]
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
                                WsCommand::Subscribe(sym, interval) => serde_json::json!({
                                    "op": "subscribe",
                                    "args": [
                                        format!("kline.{interval}.{sym}"),
                                        format!("tickers.{sym}")
                                    ]
                                }),
                                WsCommand::Unsubscribe(sym, interval) => serde_json::json!({
                                    "op": "unsubscribe",
                                    "args": [
                                        format!("kline.{interval}.{sym}"),
                                        format!("tickers.{sym}")
                                    ]
                                }),
                                WsCommand::ResubscribeAll(syms, interval) => {
                                    let args: Vec<String> = syms
                                        .iter()
                                        .flat_map(|s| {
                                            vec![
                                                format!("kline.{interval}.{s}"),
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

fn align_color(a: AlligatorAlignment) -> &'static str {
    match a {
        AlligatorAlignment::Bullish => "green",
        AlligatorAlignment::Bearish => "red",
        AlligatorAlignment::Mixed => "#8b949e",
    }
}

fn align_label(a: AlligatorAlignment) -> &'static str {
    match a {
        AlligatorAlignment::Bullish => "🐂 BULL",
        AlligatorAlignment::Bearish => "🐻 BEAR",
        AlligatorAlignment::Mixed => "— MIXED",
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

                if t.is_ghost {
                    active_rows.push_str(&format!(
                        "<tr class=\"ghost-row\">\
                         <td>{}</td>\
                         <td class=\"orange\">{}</td>\
                         <td>{}</td>\
                         <td>{:.4}</td>\
                         <td>{:.4}</td>\
                         <td>{:.4}</td>\
                         <td style=\"color:orange\">GHOST</td>\
                         <td><a href=\"/viewtrades/{}\">Chart</a></td>\
                         </tr>\n",
                        t.symbol, d_lbl, et, t.entry_price,
                        t.sl, t.tp, t.symbol,
                    ));
                } else {
                    active_rows.push_str(&format!(
                        "<tr>\
                         <td>{}</td>\
                         <td class=\"{}\">{}</td>\
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
            }

            for t in &tr.closed_trades {
                all_closed.push(t.clone());
                if t.is_ghost {
                    continue;
                }
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
            let d_lbl = dir_label(t.direction);
            if t.is_ghost {
                closed_rows.push_str(&format!(
                    "<tr class=\"ghost-row\">\
                     <td>{}</td>\
                     <td class=\"orange\">{}</td>\
                     <td>{}</td>\
                     <td>{}</td>\
                     <td>{:.4}</td>\
                     <td>{}</td>\
                     <td style=\"color:orange\">GHOST</td>\
                     </tr>\n",
                    t.symbol, d_lbl, et, ext,
                    t.entry_price, t.exit_price.unwrap_or(0.0),
                ));
            } else {
                let d_cls = dir_color(t.direction);
                let pnl = t.pnl_pct();
                let pnl_c = pnl_color(pnl);
                closed_rows.push_str(&format!(
                    "<tr>\
                     <td>{}</td>\
                     <td class=\"{}\">{}</td>\
                     <td>{}</td>\
                     <td>{}</td>\
                     <td>{:.4}</td>\
                     <td>{}</td>\
                     <td style=\"color:{}\">{:+.2}%</td>\
                     </tr>\n",
                    t.symbol,
                    d_cls,
                    d_lbl,
                    et,
                    ext,
                    t.entry_price,
                    t.exit_price.unwrap_or(0.0),
                    pnl_c,
                    pnl,
                ));
            }
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
                let align_str = format!(
                    "<span style=\"color:{}\">{}</span>",
                    align_color(tr.alignment),
                    align_label(tr.alignment),
                );
                let pos_str = if let Some(ref t) = tr.active_trade {
                    let d = dir_label(t.direction);
                    if t.is_ghost {
                        format!("{} <span class=\"orange\">GHOST</span>", d)
                    } else {
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
                        format!("{} {}", d, u)
                    }
                } else {
                    "<span style=\"color:#8b949e\">flat</span>".to_string()
                };
                let ghost_ct = tr.closed_trades.iter().filter(|t| t.is_ghost).count();
                let ghost_suffix = if ghost_ct > 0 { format!(" ({} ghost)", ghost_ct) } else { String::new() };
                let smma_str = if tr.jaw_val > 0.0 {
                    format!("J:{:.2} T:{:.2} L:{:.2}", tr.jaw_val, tr.teeth_val, tr.lips_val)
                } else {
                    "warming up...".to_string()
                };
                format!(
                    "<li>\
                     <b>{sym}</b> {align_str} \
                     | pos: {pos_str} \
                     | trades: {attempts}{ghost_suffix} \
                     | {smma_str} \
                     | <a href=\"/viewtrades/{sym}\">chart</a> \
                     | <a href=\"/removeSymbol/{sym}\" style=\"color:#ef5350;font-size:0.8em\">remove</a>\
                     </li>",
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
            "<tr><td colspan=\"9\" style=\"text-align:center;color:#8b949e\">No active trades</td></tr>"
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

        let (sl_pct, tp_pct, rev, _ghost, interval) = (
            *self.sl_percent.read().unwrap(),
            *self.tp_percent.read().unwrap(),
            *self.reverse_mode.read().unwrap(),
            *self.ghost_threshold.read().unwrap(),
            *self.interval_minutes.read().unwrap(),
        );
        let rev_tag = if rev { " <span style=\"color:#ef5350;font-weight:700\">[REVERSE]</span>" } else { "" };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta http-equiv="refresh" content="5">
<title>🐊 Alligator Trading Engine</title>
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
  .orange {{ color:#ff9800; font-weight:600 }}
  .ghost-row {{ opacity:0.65 }}
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
  .legend {{ display:flex; gap:20px; font-size:0.8em; margin:6px 0 }}
  .legend-item {{ display:flex; align-items:center; gap:6px }}
  .legend-dot {{ width:12px; height:3px; border-radius:2px }}
</style>
</head>
<body>
<h1>🐊 Alligator Trading Engine{rev_tag}</h1>
<p style="color:#8b949e;margin:4px 0 12px;font-size:0.9em">
  Interval: {interval}m &middot; SL: {sl_pct}% &middot; TP: {tp_pct}%
  &middot; <a href="/config">Config</a>
  &middot; Jaw(13) <span style="color:#2196f3">━</span>
  Teeth(8) <span style="color:#f44336">━</span>
  Lips(5) <span style="color:#4caf50">━</span>
</p>

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
<table><thead><tr><th>Symbol</th><th>Dir</th><th>Entry Time</th><th>Entry $</th><th>SL</th><th>TP</th><th>Unrealized</th><th>View</th></tr></thead><tbody>
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
            rev_tag = rev_tag,
            interval = interval,
            sl_pct = sl_pct,
            tp_pct = tp_pct,
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
                let align_str = format!(
                    "<span style=\"color:{}\">{}</span>",
                    align_color(tr.alignment),
                    tr.alignment.as_str(),
                );
                format!(
                    "<tr>\
                     <td><a href=\"/viewtrades/{}\">{}</a></td>\
                     <td>{:.2}</td>\
                     <td>{align_str}</td>\
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
            "<tr><td colspan=\"6\" style=\"text-align:center;color:#8b949e\">No symbols watched</td></tr>"
                .to_string()
        } else {
            sym_rows
        };

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Manage — 🐊 Alligator Engine</title>
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
  <a href="/config">Config</a>
  <a href="/state">State</a>
</div>
<h1>Manage 🐊 Alligator Engine</h1>

<div class="card">
  <h3>Subscribe to Signals <span class="badge">{subs_count} active</span></h3>
  <p style="color:#8b949e;font-size:0.85em;margin-bottom:12px">
    Subscribers follow all alligator signals automatically (both Long and Short).
    When a watched symbol triggers an alligator alignment transition, all subscribers execute
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
    The engine uses Williams Alligator (SMMA 13/8/5) to detect alignment transitions
    and opens 1:3 R:R trades on signal changes.
  </p>
  <form action="/watch" method="get">
    <label>Symbol:</label><input name="symbol" placeholder="BTCUSDT" style="width:120px" required>
    <button type="submit" class="btn-green">Watch</button>
  </form>
</div>

<h3>Watched Symbols</h3>
<table><thead><tr><th>Symbol</th><th>Base</th><th>Alligator</th><th>State</th><th>Trades</th><th></th></tr></thead><tbody>
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
        let total_pnl: f64 = tr.closed_trades.iter().filter(|t| !t.is_ghost).map(|t| t.pnl_pct()).sum();
        let ghost_count = tr.closed_trades.iter().filter(|t| t.is_ghost).count();
        let (state_bold, state_cls) = if tr.active_trade.is_some() {
            if tr.active_trade.as_ref().map(|t| t.is_ghost).unwrap_or(false) {
                ("GHOST", "orange")
            } else {
                ("IN POSITION", "green")
            }
        } else {
            ("FLAT", "red")
        };
        let align_str = format!(
            "<span style=\"color:{}\">{}</span>",
            align_color(tr.alignment),
            align_label(tr.alignment),
        );
        let entry_str = tr
            .active_trade
            .as_ref()
            .map(|t| format!("{:.4}", t.entry_price))
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
        let signal_str = tr
            .active_trade
            .as_ref()
            .map(|t| t.signal_label.clone())
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
            if t.is_ghost {
                closed_rows.push_str(&format!(
                    "<tr class=\"ghost-row\">\
                     <td>{}</td>\
                     <td class=\"orange\">{}</td>\
                     <td>{et}</td>\
                     <td>{ext}</td>\
                     <td>{:.4}</td>\
                     <td>{:.4}</td>\
                     <td class=\"orange\">GHOST</td>\
                     <td>{dur}</td>\
                     <td>{}</td>\
                     </tr>\n",
                    i + 1,
                    dir_label(t.direction),
                    t.entry_price,
                    t.exit_price.unwrap_or(0.0),
                    t.signal_label,
                ));
            } else {
                let pnl = t.pnl_pct();
                let clr = pnl_class(pnl);
                closed_rows.push_str(&format!(
                    "<tr>\
                     <td>{}</td>\
                     <td>{}</td>\
                     <td>{et}</td>\
                     <td>{ext}</td>\
                     <td>{:.4}</td>\
                     <td>{:.4}</td>\
                     <td class=\"{clr}\">{pnl:+.2}%</td>\
                     <td>{dur}</td>\
                     <td>{}</td>\
                     </tr>\n",
                    i + 1,
                    dir_label(t.direction),
                    t.entry_price,
                    t.exit_price.unwrap_or(0.0),
                    t.signal_label,
                ));
            }
        }
        if closed_rows.is_empty() {
            closed_rows = "<tr><td colspan=\"9\" style=\"color:#8b949e;text-align:center\">No closed trades</td></tr>".to_string();
        }

        Some(format!(
            r#"<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{symbol} — Alligator Trades</title>
<style>
  body {{ font-family:Segoe UI,sans-serif; background:#0d1117; color:#c9d1d9; padding:20px }}
  h1,h2 {{ color:#f0f6fc }}
  table {{ border-collapse:collapse; margin:12px 0 }}
  th,td {{ padding:6px 12px; border:1px solid #30363d; text-align:left }}
  th {{ background:#21262d; color:#8b949e }}
  .green {{ color:#4caf50 }} .red {{ color:#ef5350 }} .orange {{ color:#ff9800;font-weight:600 }}
  .ghost-row {{ opacity:0.65 }}
  a {{ color:#58a6ff }}
</style></head><body>
<h1>{symbol} &mdash; 🐊 Alligator Bot</h1>
<p>Base: {base:.4} | Strategy: Williams Alligator | Alignment: {align_str}
 | State: <b class="{state_cls}">{state_bold}</b>
 | Signal: {signal_str}
 | Active Entry: {entry_str}
 | SL: {sl_str}
 | TP: {tp_str}
 | Total Closed PnL: <span class="{pnl_cls}">{total_pnl:+.2}%</span> (excl. {ghost_count} ghost)</p>

{chart}

<h2>Closed Trades</h2>
<table><thead><tr><th>#</th><th>Dir</th><th>Entry Time</th><th>Exit Time</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Duration</th><th>Signal</th></tr></thead><tbody>
{closed_rows}
</tbody></table>
<p><a href="/">Back to dashboard</a></p>
</body></html>"#,
            symbol = symbol,
            base = tr.base_price,
            align_str = align_str,
            state_cls = state_cls,
            state_bold = state_bold,
            signal_str = signal_str,
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
        let interval = *self.interval_minutes.read().unwrap();
        let mut lines = Vec::new();
        for (sym, tr) in &trackers {
            let active = tr
                .active_trade
                .as_ref()
                .map(|t| {
                    let ghost_tag = if t.is_ghost { " [GHOST]" } else { "" };
                    format!(
                        "{} EP={} SL={} TP={}{}",
                        t.direction.upper(),
                        t.entry_price,
                        t.sl,
                        t.tp,
                        ghost_tag,
                    )
                })
                .unwrap_or_else(|| "flat".to_string());
            let ghost_ct = tr.closed_trades.iter().filter(|t| t.is_ghost).count();
            lines.push(format!(
                "<b>{sym}</b> base={bp:.4} | interval={interval}m | alligator={al} | jaw={jv:.4} teeth={tv:.4} lips={lv:.4} | current={cp} | active: {active} | klines={kc} | closed_trades={ct}{ghost_str} | ghost_left={gl} | triggered={trig} | ws={ws}",
                bp = tr.base_price,
                al = tr.alignment.as_str(),
                jv = tr.jaw_val,
                tv = tr.teeth_val,
                lv = tr.lips_val,
                cp = tr.current_price.map(|p| format!("{p}")).unwrap_or_else(|| "None".to_string()),
                kc = tr.klines.len(),
                ct = tr.closed_trades.len(),
                ghost_str = if ghost_ct > 0 { format!(" ({} ghost)", ghost_ct) } else { String::new() },
                gl = tr.ghost_remaining,
                trig = tr.ghost_triggered,
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

    // ─── Alligator Scoring ───────────────────────────────────────────

    pub fn score_symbol(klines: &[KlineData]) -> AlligatorScore {
        Self::score_symbol_with_config(klines, 1.5, 0.5)
    }

    pub fn score_symbol_with_config(klines: &[KlineData], sl_pct: f64, tp_pct: f64) -> AlligatorScore {
        if klines.len() < 21 {
            return AlligatorScore {
                symbol: String::new(),
                base_price: 0.0,
                total_signals: 0,
                total_trades: 0,
                wins: 0,
                losses: 0,
                total_pnl_pct: 0.0,
                win_rate: 0.0,
                max_drawdown_pct: 0.0,
                suitability: 0.0,
            };
        }

        let base_price = klines[0].close;
        let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
        let highs: Vec<f64> = klines.iter().map(|k| k.high).collect();
        let lows: Vec<f64> = klines.iter().map(|k| k.low).collect();

        // Calculate SMMAs for all klines
        let mut close_deque: VecDeque<f64> = VecDeque::new();
        let mut _scores: Vec<AlligatorScore> = Vec::new();

        let mut total_signals: u32 = 0;
        let mut total_trades: u32 = 0;
        let mut wins: u32 = 0;
        let mut losses: u32 = 0;
        let mut total_pnl_pct: f64 = 0.0;
        let mut peak_pnl: f64 = 0.0;
        let mut max_drawdown: f64 = 0.0;

        let mut active: Option<(TradeDirection, f64, f64, f64)> = None;
        let mut prev_alignment = AlligatorAlignment::Mixed;
        let mut alignment = AlligatorAlignment::Mixed;

        for i in 0..klines.len() {
            close_deque.push_back(closes[i]);
            if close_deque.len() > MAX_CLOSE_POOL {
                close_deque.pop_front();
            }

            if close_deque.len() < JAW_PERIOD {
                continue;
            }

            let jaw_raw = calc_smma(&close_deque, JAW_PERIOD);
            let teeth_raw = calc_smma(&close_deque, TEETH_PERIOD);
            let lips_raw = calc_smma(&close_deque, LIPS_PERIOD);

            let has_displaced = jaw_raw.len() > JAW_DISPLACEMENT
                && teeth_raw.len() > TEETH_DISPLACEMENT
                && lips_raw.len() > LIPS_DISPLACEMENT;

            if !has_displaced {
                continue;
            }

            let jv = get_displaced(&jaw_raw, JAW_DISPLACEMENT).unwrap();
            let tv = get_displaced(&teeth_raw, TEETH_DISPLACEMENT).unwrap();
            let lv = get_displaced(&lips_raw, LIPS_DISPLACEMENT).unwrap();
            prev_alignment = alignment;
            alignment = determine_alignment(lv, tv, jv);

            let high = highs[i];
            let low = lows[i];

            // check exit
            if let Some((dir, entry, sl, tp)) = active {
                let hit_sl = match dir {
                    TradeDirection::Long => low <= sl,
                    TradeDirection::Short => high >= sl,
                };
                let hit_tp = match dir {
                    TradeDirection::Long => high >= tp,
                    TradeDirection::Short => low <= tp,
                };
                let alignment_exit = match dir {
                    TradeDirection::Long => alignment != AlligatorAlignment::Bullish,
                    TradeDirection::Short => alignment != AlligatorAlignment::Bearish,
                };
                if hit_sl || hit_tp || (prev_alignment != alignment && alignment_exit) {
                    let exit_price = if hit_sl {
                        match dir {
                            TradeDirection::Long => low,
                            TradeDirection::Short => high,
                        }
                    } else if hit_tp {
                        tp
                    } else {
                        closes[i]
                    };
                    let pnl = match dir {
                        TradeDirection::Long => (exit_price - entry) / entry * 100.0,
                        TradeDirection::Short => (entry - exit_price) / entry * 100.0,
                    };
                    if hit_tp {
                        wins += 1;
                    } else {
                        losses += 1;
                    }
                    total_pnl_pct += pnl;
                    if total_pnl_pct > peak_pnl {
                        peak_pnl = total_pnl_pct;
                    }
                    let dd = peak_pnl - total_pnl_pct;
                    if dd > max_drawdown {
                        max_drawdown = dd;
                    }
                    active = None;
                }
            }

            // check entry
            if active.is_none() && prev_alignment != alignment {
                let should_enter = match alignment {
                    AlligatorAlignment::Bullish => {
                        prev_alignment != AlligatorAlignment::Bullish
                    }
                    AlligatorAlignment::Bearish => {
                        prev_alignment != AlligatorAlignment::Bearish
                    }
                    AlligatorAlignment::Mixed => false,
                };
                if should_enter {
                    let direction = match alignment {
                        AlligatorAlignment::Bullish => TradeDirection::Long,
                        AlligatorAlignment::Bearish => TradeDirection::Short,
                        AlligatorAlignment::Mixed => unreachable!(),
                    };
                    let entry = closes[i];
                    let (sl, tp) = match direction {
                        TradeDirection::Long => {
                            (entry * (1.0 - sl_pct / 100.0), entry * (1.0 + tp_pct / 100.0))
                        }
                        TradeDirection::Short => {
                            (entry * (1.0 + sl_pct / 100.0), entry * (1.0 - tp_pct / 100.0))
                        }
                    };
                    active = Some((direction, entry, sl, tp));
                    total_signals += 1;
                    total_trades += 1;
                }
            }
        }

        let win_rate = if total_trades > 0 {
            wins as f64 / total_trades as f64 * 100.0
        } else {
            0.0
        };

        // Suitability: trade count 25%, win rate 30%, PnL 25%, drawdown penalty 20%
        let trades_norm = (total_trades as f64 / klines.len() as f64 * 100.0).min(100.0);
        let pnl_norm = (total_pnl_pct + 20.0).max(0.0).min(100.0);
        let dd_penalty = (1.0 - (max_drawdown / 30.0).min(1.0)) * 100.0;
        let suitability = trades_norm * 0.25 + win_rate * 0.30 + pnl_norm * 0.25 + dd_penalty * 0.20;

        AlligatorScore {
            symbol: String::new(),
            base_price,
            total_signals,
            total_trades,
            wins,
            losses,
            total_pnl_pct,
            win_rate,
            max_drawdown_pct: max_drawdown,
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

    // Compute PnL line for each kline index
    let mut pnl_line: Vec<f64> = vec![0.0; klines.len()];
    let mut _active_trade_range: Option<(usize, usize, GridTrade)> = None;

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
  .marker-entry-long {{ fill:#4caf50;stroke:#000;stroke-width:0.5 }}
  .marker-entry-short {{ fill:#ef5350;stroke:#000;stroke-width:0.5 }}
  .marker-ghost {{ fill:#ff9800;stroke:#000;stroke-width:0.5 }}
  .marker-exit {{ fill:#f0883e;stroke:#000;stroke-width:0.5 }}
  .pnl-line {{ fill:none;stroke:#c9d1d9;stroke-width:1 }}
  .pnl-fill-p {{ fill:rgba(76,175,80,0.3) }}
  .pnl-fill-n {{ fill:rgba(239,83,80,0.3) }}
  .axis-text {{ fill:#8b949e;font-size:10px;font-family:monospace }}
  .smma-jaw {{ fill:none;stroke:#2196f3;stroke-width:1.5 }}
  .smma-teeth {{ fill:none;stroke:#f44336;stroke-width:1.5 }}
  .smma-lips {{ fill:none;stroke:#4caf50;stroke-width:2 }}
</style>
"###,
    ));

    // time grid lines
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

    // ── SMMA overlay lines ──────────────────────────────────────────
    // Build SMMA value arrays aligned to kline timeline
    let mut jaw_pts: Vec<(f64, f64)> = Vec::new();
    let mut teeth_pts: Vec<(f64, f64)> = Vec::new();
    let mut lips_pts: Vec<(f64, f64)> = Vec::new();

    // Use close_prices stored in tracker to recompute SMMAs aligned
    let jaw_full = calc_smma(&tr.close_prices, JAW_PERIOD);
    let teeth_full = calc_smma(&tr.close_prices, TEETH_PERIOD);
    let lips_full = calc_smma(&tr.close_prices, LIPS_PERIOD);

    // Map SMMA values to kline indices
    // The SMMA values correspond to the last N close_prices, where N = close_prices.len()
    // The klines contain the same data starting from some offset
    // We need to align: close_prices.len() may be less than klines.len()
    let close_offset = klines.len() - tr.close_prices.len();

    for i in 0..jaw_full.len() {
        // displaced value at kline index
        let displaced_idx = i + JAW_DISPLACEMENT;
        if displaced_idx >= jaw_full.len() {
            break;
        }
        let kline_i = close_offset + i;
        if kline_i >= klines.len() {
            break;
        }
        let x = to_x(kline_i);
        let jv = jaw_full[displaced_idx];
        let y = to_y(jv);
        if y >= margin_t && y <= margin_t + plot_h {
            jaw_pts.push((x, y));
        }
    }

    for i in 0..teeth_full.len() {
        let displaced_idx = i + TEETH_DISPLACEMENT;
        if displaced_idx >= teeth_full.len() {
            break;
        }
        let kline_i = close_offset + i;
        if kline_i >= klines.len() {
            break;
        }
        let x = to_x(kline_i);
        let tv = teeth_full[displaced_idx];
        let y = to_y(tv);
        if y >= margin_t && y <= margin_t + plot_h {
            teeth_pts.push((x, y));
        }
    }

    for i in 0..lips_full.len() {
        let displaced_idx = i + LIPS_DISPLACEMENT;
        if displaced_idx >= lips_full.len() {
            break;
        }
        let kline_i = close_offset + i;
        if kline_i >= klines.len() {
            break;
        }
        let x = to_x(kline_i);
        let lv = lips_full[displaced_idx];
        let y = to_y(lv);
        if y >= margin_t && y <= margin_t + plot_h {
            lips_pts.push((x, y));
        }
    }

    // Draw SMMA polylines
    let jaw_str: Vec<String> = jaw_pts.iter().map(|(x, y)| format!("{x},{y}")).collect();
    let teeth_str: Vec<String> = teeth_pts.iter().map(|(x, y)| format!("{x},{y}")).collect();
    let lips_str: Vec<String> = lips_pts.iter().map(|(x, y)| format!("{x},{y}")).collect();

    if !jaw_str.is_empty() {
        s.push_str(&format!(r###"<polyline points="{}" class="smma-jaw"/>"###, jaw_str.join(" ")));
    }
    if !teeth_str.is_empty() {
        s.push_str(&format!(r###"<polyline points="{}" class="smma-teeth"/>"###, teeth_str.join(" ")));
    }
    if !lips_str.is_empty() {
        s.push_str(&format!(r###"<polyline points="{}" class="smma-lips"/>"###, lips_str.join(" ")));
    }

    // ── Trade markers ───────────────────────────────────────────────
    for t in &tr.closed_trades {
        let entry_i = klines
            .iter()
            .position(|k| k.datetime >= t.entry_time);
        if let Some(ei) = entry_i {
            let ex = to_x(ei);
            let ey = to_y(t.entry_price);
            let marker_cls = if t.is_ghost {
                "marker-ghost"
            } else {
                match t.direction {
                    TradeDirection::Long => "marker-entry-long",
                    TradeDirection::Short => "marker-entry-short",
                }
            };
            match t.direction {
                TradeDirection::Long => {
                    let y1 = ey - 7.0;
                    let y2 = ey + 2.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}"/>"###,
                        xl = ex - 5.0,
                        xr = ex + 5.0,
                    ));
                }
                TradeDirection::Short => {
                    let y1 = ey + 7.0;
                    let y2 = ey - 2.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}"/>"###,
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
            let marker_cls = if t.is_ghost {
                "marker-ghost"
            } else {
                match t.direction {
                    TradeDirection::Long => "marker-entry-long",
                    TradeDirection::Short => "marker-entry-short",
                }
            };
            match t.direction {
                TradeDirection::Long => {
                    let y1 = ey - 9.0;
                    let y2 = ey + 3.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}" stroke-width="1.5" stroke="#fff"/>"###,
                        xl = ex - 6.0,
                        xr = ex + 6.0,
                    ));
                }
                TradeDirection::Short => {
                    let y1 = ey + 9.0;
                    let y2 = ey - 3.0;
                    s.push_str(&format!(
                        r###"<polygon points="{ex},{y1} {xl},{y2} {xr},{y2}" class="{marker_cls}" stroke-width="1.5" stroke="#fff"/>"###,
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

    // SMMA legend
    s.push_str(&format!(
        r###"<g transform="translate({lx},{ly})">
  <rect x="0" y="0" width="140" height="58" rx="4" fill="#161b22" fill-opacity="0.9" stroke="#30363d" stroke-width="0.5"/>
  <text x="10" y="16" fill="#8b949e" font-size="9" font-family="monospace">Jaw (13)</text>
  <line x1="80" y1="12" x2="130" y2="12" stroke="#2196f3" stroke-width="2"/>
  <text x="10" y="32" fill="#8b949e" font-size="9" font-family="monospace">Teeth (8)</text>
  <line x1="80" y1="28" x2="130" y2="28" stroke="#f44336" stroke-width="2"/>
  <text x="10" y="48" fill="#8b949e" font-size="9" font-family="monospace">Lips (5)</text>
  <line x1="80" y1="44" x2="130" y2="44" stroke="#4caf50" stroke-width="2"/>
</g>"###,
        lx = margin_l + 10.0,
        ly = margin_t + 10.0,
    ));

    s.push_str("</svg>");
    s
}
