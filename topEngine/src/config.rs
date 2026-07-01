use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct AccountConfig {
    pub id: i32,
    #[serde(rename = "APIKEY")]
    pub apikey: String,
    #[serde(rename = "SECRET")]
    pub secret: String,
    #[serde(rename = "DEMO")]
    pub demo: bool,
    #[serde(rename = "TESTNET")]
    pub testnet: bool,
    #[serde(rename = "BASE_URL")]
    pub base_url: String,
    #[serde(rename = "BASE_URL_DEMO")]
    pub base_url_demo: String,
    #[serde(rename = "BASE_URL_PROD")]
    pub base_url_prod: String,
    #[serde(rename = "QUOTE_ASSET")]
    pub quote_asset: String,
    #[serde(rename = "PAPI")]
    pub papi: bool,
}

pub fn load_config() -> Vec<AccountConfig> {
    let data = fs::read_to_string("config.json").expect("Failed to read config.json");
    serde_json::from_str(&data).expect("Failed to parse config.json")
}

pub fn get_account(sub_id: i32) -> AccountConfig {
    let configs = load_config();
    configs
        .into_iter()
        .find(|c| c.id == sub_id)
        .unwrap_or_else(|| panic!("Subscriber ID {sub_id} not found in config"))
}

impl AccountConfig {
    pub fn base_url(&self) -> &str {
        if self.demo || self.testnet {
            &self.base_url_demo
        } else {
            &self.base_url_prod
        }
    }
}
