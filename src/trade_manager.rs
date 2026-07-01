use crate::config::get_account;
use crate::engine::KlineData;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

type HmacSha256 = Hmac<Sha256>;

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

fn http_client() -> &'static Client {
    HTTP_CLIENT.get_or_init(|| Client::new())
}

fn sign(payload: &str, secret: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[derive(Clone)]
struct CachedExchange {
    api_key: String,
    secret: String,
    base_url: String,
}

fn exchange_cache() -> &'static Mutex<HashMap<i32, CachedExchange>> {
    static CACHE: OnceLock<Mutex<HashMap<i32, CachedExchange>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_exchange(sub_id: i32) -> CachedExchange {
    let mut cache = exchange_cache().lock().unwrap();
    if let Some(ex) = cache.get(&sub_id) {
        return ex.clone();
    }
    let acc = get_account(sub_id);
    let ex = CachedExchange {
        api_key: acc.apikey.clone(),
        secret: acc.secret.clone(),
        base_url: acc.base_url().to_string(),
    };
    cache.insert(sub_id, ex.clone());
    ex
}

pub async fn fetch_ticker(symbol: &str) -> Result<f64, String> {
    let url = format!("https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}");
    let resp = http_client()
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let val: Value = resp.json().await.map_err(|e| e.to_string())?;
    if val["retCode"].as_i64() != Some(0) {
        return Err(format!("ticker error: {val}"));
    }
    let price = val["result"]["list"][0]["lastPrice"]
        .as_str()
        .ok_or("no lastPrice")?
        .parse::<f64>()
        .map_err(|e| e.to_string())?;
    Ok(price)
}

pub async fn fetch_historical_klines(
    symbol: &str,
    interval: u32,
    limit: u32,
) -> Result<Vec<KlineData>, String> {
    let url = format!(
        "https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    );
    let resp = http_client()
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let val: Value = resp.json().await.map_err(|e| e.to_string())?;
    if val["retCode"].as_i64() != Some(0) {
        return Err(format!("kline error: {val}"));
    }
    let list = val["result"]["list"]
        .as_array()
        .ok_or("no kline list")?;
    let mut klines = Vec::with_capacity(list.len());
    for candle in list.iter().rev() {
        let arr = candle.as_array().ok_or("kline not array")?;
        let ts = arr
            .first()
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let open = arr
            .get(1)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let high = arr
            .get(2)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let low = arr
            .get(3)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let close = arr
            .get(4)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let volume = arr
            .get(5)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let secs = ts / 1000;
        let nsecs = ((ts % 1000) * 1_000_000) as u32;
        let datetime = DateTime::from_timestamp(secs, nsecs)
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        klines.push(KlineData {
            ts,
            datetime,
            open,
            high,
            low,
            close,
            volume,
        });
    }
    Ok(klines)
}

pub async fn fetch_active_perps() -> Result<Vec<String>, String> {
    let url = "https://api.bybit.com/v5/market/tickers?category=linear&limit=1000";
    let resp = http_client()
        .get(url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let val: Value = resp.json().await.map_err(|e| e.to_string())?;
    if val["retCode"].as_i64() != Some(0) {
        return Err(format!("tickers error: {val}"));
    }
    let list = val["result"]["list"].as_array().ok_or("no list")?;
    let mut symbols = Vec::new();
    for item in list {
        if let Some(sym) = item["symbol"].as_str() {
            if sym.ends_with("USDT") || sym.ends_with("USDC") {
                symbols.push(sym.to_string());
            }
        }
    }
    Ok(symbols)
}

async fn private_post(
    base_url: &str,
    endpoint: &str,
    body: Value,
    api_key: &str,
    secret: &str,
) -> Result<Value, String> {
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000";
    let body_str = body.to_string();
    let sign_payload = format!("{timestamp}{api_key}{recv_window}{body_str}");
    let signature = sign(&sign_payload, secret);

    let url = format!("{base_url}{endpoint}");
    let resp = http_client()
        .post(&url)
        .header("X-BAPI-API-KEY", api_key)
        .header("X-BAPI-TIMESTAMP", &timestamp)
        .header("X-BAPI-SIGN", &signature)
        .header("X-BAPI-RECV-WINDOW", recv_window)
        .header("X-BAPI-SIGN-TYPE", "2")
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let text = resp.text().await.map_err(|e| e.to_string())?;
    let val: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if val["retCode"].as_i64() != Some(0) {
        return Err(format!("API error: {text}"));
    }
    Ok(val)
}

async fn private_get(
    base_url: &str,
    endpoint: &str,
    params: &[(&str, &str)],
    api_key: &str,
    secret: &str,
) -> Result<Value, String> {
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000";

    let query_parts: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let query_string = query_parts.join("&");
    let sign_payload = format!("{timestamp}{api_key}{recv_window}{query_string}");
    let signature = sign(&sign_payload, secret);

    let url = if query_string.is_empty() {
        format!("{base_url}{endpoint}")
    } else {
        format!("{base_url}{endpoint}?{query_string}")
    };

    let resp = http_client()
        .get(&url)
        .header("X-BAPI-API-KEY", api_key)
        .header("X-BAPI-TIMESTAMP", &timestamp)
        .header("X-BAPI-SIGN", &signature)
        .header("X-BAPI-RECV-WINDOW", recv_window)
        .header("X-BAPI-SIGN-TYPE", "2")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let text = resp.text().await.map_err(|e| e.to_string())?;
    let val: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if val["retCode"].as_i64() != Some(0) {
        return Err(format!("API error: {text}"));
    }
    Ok(val)
}

fn qty_step_cache() -> &'static Mutex<HashMap<String, f64>> {
    static CACHE: OnceLock<Mutex<HashMap<String, f64>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn get_qty_step(symbol: &str) -> f64 {
    {
        let cache = qty_step_cache().lock().unwrap();
        if let Some(step) = cache.get(symbol) {
            return *step;
        }
    }
    let url = format!("https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={symbol}");
    let step = match http_client().get(&url).send().await {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(val) if val["retCode"].as_i64() == Some(0) => {
                val["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.001)
            }
            _ => 0.001,
        },
        Err(_) => 0.001,
    };
    qty_step_cache().lock().unwrap().insert(symbol.to_string(), step);
    step
}

fn step_decimals(step: f64) -> usize {
    if step >= 1.0 {
        return 0;
    }
    let s = format!("{:.10}", step);
    let trimmed = s.trim_end_matches('0');
    if let Some(dot) = trimmed.find('.') {
        trimmed[dot + 1..].len()
    } else {
        0
    }
}

fn round_qty(qty: f64, step: f64) -> String {
    if step <= 0.0 {
        return format!("{}", qty as i64);
    }
    let rounded = (qty / step).round() * step;
    let decimals = step_decimals(step);
    format!("{:.prec$}", rounded, prec = decimals)
}

pub fn open_long(symbol: String, qty: f64, sub_id: i32, limit_price: f64, sl: f64, tp: f64) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let step = get_qty_step(&symbol).await;
            let ex = get_exchange(sub_id);
            let qty_str = round_qty(qty, step);
            let sym = symbol.clone();
            let body = json!({
                "category": "linear",
                "symbol": sym,
                "side": "Buy",
                "orderType": "Limit",
                "qty": qty_str.clone(),
                "price": format!("{}", limit_price),
                "takeProfit": format!("{}", tp),
                "stopLoss": format!("{}", sl),
                "tpslMode": "Full",
                "positionIdx": 1,
                "timeInForce": "GTC"
            });
            match private_post(&ex.base_url, "/v5/order/create", body, &ex.api_key, &ex.secret).await {
                Ok(v) => println!(
                    "[TradeManager] LONG {symbol} qty={qty_str} limit={limit_price} SL={sl} TP={tp} subID={sub_id} order={}",
                    v["result"]["orderId"]
                ),
                Err(e) => println!(
                    "[TradeManager] openLong({symbol}, limit={limit_price}) failed: {e}"
                ),
            }
        });
    });
}

pub fn open_short(symbol: String, qty: f64, sub_id: i32, limit_price: f64, sl: f64, tp: f64) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let step = get_qty_step(&symbol).await;
            let ex = get_exchange(sub_id);
            let qty_str = round_qty(qty, step);
            let sym = symbol.clone();
            let body = json!({
                "category": "linear",
                "symbol": sym,
                "side": "Sell",
                "orderType": "Limit",
                "qty": qty_str.clone(),
                "price": format!("{}", limit_price),
                "takeProfit": format!("{}", tp),
                "stopLoss": format!("{}", sl),
                "tpslMode": "Full",
                "positionIdx": 2,
                "timeInForce": "GTC"
            });
            match private_post(&ex.base_url, "/v5/order/create", body, &ex.api_key, &ex.secret).await {
                Ok(v) => println!(
                    "[TradeManager] SHORT {symbol} qty={qty_str} limit={limit_price} SL={sl} TP={tp} subID={sub_id} order={}",
                    v["result"]["orderId"]
                ),
                Err(e) => println!(
                    "[TradeManager] openShort({symbol}, limit={limit_price}) failed: {e}"
                ),
            }
        });
    });
}

pub fn cancel_old_orders(symbol: String, sub_id: i32) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let ex = get_exchange(sub_id);
            let sym = &symbol;

            match private_get(
                &ex.base_url,
                "/v5/order/realtime",
                &[("category", "linear"), ("symbol", sym), ("limit", "50")],
                &ex.api_key,
                &ex.secret,
            )
            .await
            {
                Ok(resp) => {
                    let now = Utc::now();
                    let cutoff = now - chrono::Duration::minutes(3);
                    if let Some(list) = resp["result"]["list"].as_array() {
                        for order in list {
                            let stop_type = order["stopOrderType"].as_str().unwrap_or("");
                            if !stop_type.is_empty() {
                                continue; // skip TP/SL orders
                            }
                            let status = order["orderStatus"].as_str().unwrap_or("");
                            if status != "New" && status != "PartiallyFilled" && status != "Untriggered" {
                                continue;
                            }
                            let created_str = order["createdTime"]
                                .as_str()
                                .unwrap_or("0");
                            let created_ms: i64 = created_str.parse().unwrap_or(0);
                            let created = DateTime::from_timestamp_millis(created_ms)
                                .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
                            if created > cutoff {
                                continue;
                            }
                            let oid = order["orderId"].as_str().unwrap_or("");
                            if oid.is_empty() {
                                continue;
                            }
                            match private_post(
                                &ex.base_url,
                                "/v5/order/cancel",
                                json!({"category": "linear", "symbol": sym, "orderId": oid}),
                                &ex.api_key,
                                &ex.secret,
                            )
                            .await
                            {
                                Ok(_) => println!(
                                    "[TradeManager] Cancelled stale order {oid} {sym} subID={sub_id}"
                                ),
                                Err(e) => println!(
                                    "[TradeManager] Cancel {oid} failed: {e}"
                                ),
                            }
                        }
                    }
                }
                Err(e) => println!(
                    "[TradeManager] cancelOldOrders({sub_id}) failed: {e}"
                ),
            }
        });
    });
}
