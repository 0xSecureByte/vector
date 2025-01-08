use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AzureEventHubConfig {
    pub connection_string: String,
    pub blob_storage_account: String,
    pub blob_storage_key: String,
    pub blob_storage_url: String,
}

impl AzureEventHubConfig {
    pub fn from_connection_string(conn_str: &str) -> Option<Self> {
        Some(Self {
            connection_string: conn_str.to_string(),
            blob_storage_account: "account".to_string(),
            blob_storage_key: "key".to_string(),
            blob_storage_url: "url".to_string(),
        })
    }
}
