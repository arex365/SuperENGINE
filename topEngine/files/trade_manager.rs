use crate::config::get_account;
use chrono::Utc;
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

pub async fn fetch_ticker(symbol: &str) -> Result<(f64, f64), String> {
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
    let list = &val["result"]["list"][0];
    let bid = list["bid1Price"]
        .as_str()
        .ok_or("no bid1Price")?
        .parse::<f64>()
        .map_err(|e| e.to_string())?;
    let ask = list["ask1Price"]
        .as_str()
        .ok_or("no ask1Price")?
        .parse::<f64>()
        .map_err(|e| e.to_string())?;
    Ok((bid, ask))
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

pub async fn open_long(symbol: String, position_size: f64, sub_id: i32) {
    tokio::spawn(async move {
        let step = get_qty_step(&symbol).await;
        let ex = get_exchange(sub_id);
        let qty = round_qty(position_size, step);
        let sym = symbol.clone();
        let body = json!({
            "category": "linear",
            "symbol": sym,
            "side": "Buy",
            "orderType": "Market",
            "qty": qty.clone(),
            "positionIdx": 1,
            "timeInForce": "IOC"
        });
        match private_post(&ex.base_url, "/v5/order/create", body, &ex.api_key, &ex.secret).await {
            Ok(v) => println!(
                "[TradeManager] LONG {symbol} {qty} subID={sub_id} order={}",
                v["result"]["orderId"]
            ),
            Err(e) => println!(
                "[TradeManager] openLong({symbol}, {position_size}) failed: {e}"
            ),
        }
    });
}

pub async fn open_short(symbol: String, position_size: f64, sub_id: i32) {
    tokio::spawn(async move {
        let step = get_qty_step(&symbol).await;
        let ex = get_exchange(sub_id);
        let qty = round_qty(position_size, step);
        let sym = symbol.clone();
        let body = json!({
            "category": "linear",
            "symbol": sym,
            "side": "Sell",
            "orderType": "Market",
            "qty": qty.clone(),
            "positionIdx": 2,
            "timeInForce": "IOC"
        });
        match private_post(&ex.base_url, "/v5/order/create", body, &ex.api_key, &ex.secret).await {
            Ok(v) => println!(
                "[TradeManager] SHORT {symbol} {qty} subID={sub_id} order={}",
                v["result"]["orderId"]
            ),
            Err(e) => println!(
                "[TradeManager] openShort({symbol}, {position_size}) failed: {e}"
            ),
        }
    });
}

pub async fn close_all_positions(symbol: String, sub_id: i32) {
    tokio::spawn(async move {
        let step = get_qty_step(&symbol).await;
        let ex = get_exchange(sub_id);
        let sym = &symbol;

        let _ = private_post(
            &ex.base_url,
            "/v5/order/cancel-all",
            json!({"category": "linear", "symbol": sym}),
            &ex.api_key,
            &ex.secret,
        )
        .await;

        match private_get(
            &ex.base_url,
            "/v5/position/list",
            &[("category", "linear"), ("symbol", sym)],
            &ex.api_key,
            &ex.secret,
        )
        .await
        {
            Ok(pos_resp) => {
                if let Some(list) = pos_resp["result"]["list"].as_array() {
                    for pos in list {
                        let size = pos["size"]
                            .as_str()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        if size <= 0.0 {
                            continue;
                        }
                        let side = pos["side"].as_str().unwrap_or("");
                        let (close_side, position_idx) = match side {
                            "Buy" => ("Sell", 1),
                            "Sell" => ("Buy", 2),
                            _ => continue,
                        };
                        let sym_name = pos["symbol"].as_str().unwrap_or("").to_string();
                        let body = json!({
                            "category": "linear",
                            "symbol": &sym_name,
                            "side": close_side,
                            "orderType": "Market",
                            "qty": round_qty(size, step),
                            "positionIdx": position_idx,
                            "timeInForce": "IOC",
                            "reduceOnly": true,
                        });
                        let _ = private_post(
                            &ex.base_url,
                            "/v5/order/create",
                            body,
                            &ex.api_key,
                            &ex.secret,
                        )
                        .await;
                        println!("[TradeManager] Close {side} {sym_name} {size} subID={sub_id}");
                    }
                }
            }
            Err(e) => println!(
                "[TradeManager] closeAllPositions({sub_id}) failed: {e}"
            ),
        }
    });
}
