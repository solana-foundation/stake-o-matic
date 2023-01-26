use crate::Cluster;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use {
    chrono::{DateTime, Utc},
    log::*,
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        error,
        time::{Duration, Instant},
    },
};

#[allow(dead_code)]
#[derive(Debug)]
pub enum ClusterJson {
    MainnetBeta,
    Testnet,
}

impl ClusterJson {
    pub fn from_cluster(cluster: Cluster) -> ClusterJson {
        match cluster {
            Cluster::MainnetBeta => ClusterJson::MainnetBeta,
            Cluster::Testnet => ClusterJson::Testnet,
        }
    }
}

impl Default for ClusterJson {
    fn default() -> Self {
        Self::MainnetBeta
    }
}

impl AsRef<str> for ClusterJson {
    fn as_ref(&self) -> &str {
        match self {
            Self::MainnetBeta => "mainnet.json",
            Self::Testnet => "testnet.json",
        }
    }
}

const DEFAULT_BASE_URL: &str = "https://www.validators.app/api/v1/";
const TOKEN_HTTP_HEADER_NAME: &str = "Token";

#[derive(Debug)]
pub struct ClientConfig {
    pub base_url: String,
    pub cluster: ClusterJson,
    pub api_token: String,
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            cluster: ClusterJson::default(),
            api_token: String::default(),
            timeout: Duration::from_secs(90),
        }
    }
}

#[derive(Debug)]
enum Endpoint {
    Ping,
    Validators,
    CommissionChangeIndex,
}

impl Endpoint {
    fn with_cluster(path: &str, cluster: &ClusterJson) -> String {
        format!("{}/{}", path, cluster.as_ref())
    }
    pub fn path(&self, cluster: &ClusterJson) -> String {
        match self {
            Self::Ping => "ping.json".to_string(),
            Self::Validators => Self::with_cluster("validators", cluster),
            Self::CommissionChangeIndex => Self::with_cluster("commission-changes", cluster),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct PingResponse {
    answer: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ValidatorsResponseEntry {
    pub account: Option<String>,
    pub active_stake: Option<u64>,
    pub commission: Option<u8>,
    pub created_at: Option<String>,
    pub data_center_concentration_score: Option<i64>,
    pub data_center_host: Option<String>,
    pub data_center_key: Option<String>,
    pub delinquent: Option<bool>,
    pub details: Option<String>,
    pub keybase_id: Option<String>,
    pub name: Option<String>,
    pub network: Option<String>,
    pub ping_time: Option<f64>,
    pub published_information_score: Option<i64>,
    pub root_distance_score: Option<i64>,
    pub security_report_score: Option<i64>,
    pub skipped_slot_percent: Option<String>,
    pub skipped_slot_score: Option<i64>,
    pub skipped_slots: Option<u64>,
    pub software_version: Option<String>,
    pub software_version_score: Option<i64>,
    pub stake_concentration_score: Option<i64>,
    pub total_score: Option<i64>,
    pub updated_at: Option<String>,
    pub url: Option<String>,
    pub vote_account: Option<String>,
    pub vote_distance_score: Option<i64>,
    pub www_url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ValidatorsResponse(Vec<ValidatorsResponseEntry>);

impl AsRef<Vec<ValidatorsResponseEntry>> for ValidatorsResponse {
    fn as_ref(&self) -> &Vec<ValidatorsResponseEntry> {
        &self.0
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CommissionChangeIndexHistoryEntry {
    pub created_at: String,
    // commission_before can be null; presumably for new validators that have set their commission for the first time
    pub commission_before: Option<f32>,
    // This has shown up as null in at least once case. Not sure what it indicates.
    pub commission_after: Option<f32>,
    pub epoch: u64,
    pub network: String,
    pub id: i32,
    pub epoch_completion: f32,
    pub batch_uuid: String,
    pub account: String,
    // name can be null
    pub name: Option<String>,
}

impl Default for CommissionChangeIndexHistoryEntry {
    fn default() -> CommissionChangeIndexHistoryEntry {
        CommissionChangeIndexHistoryEntry {
            created_at: "".to_string(),
            commission_before: None,
            commission_after: None,
            epoch: 0,
            network: "".to_string(),
            id: 0,
            epoch_completion: 0.0,
            batch_uuid: "".to_string(),
            account: "".to_string(),
            name: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommissionChangeIndexResponse {
    pub commission_histories: Vec<CommissionChangeIndexHistoryEntry>,
    pub total_count: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum SortKind {
    Score,
    Name,
    Stake,
}

impl std::fmt::Display for SortKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Score => write!(f, "score"),
            Self::Name => write!(f, "name"),
            Self::Stake => write!(f, "stake"),
        }
    }
}

pub type Limit = u32;

pub struct Client {
    base_url: reqwest::Url,
    cluster: ClusterJson,
    api_token: String,
    client: reqwest::blocking::Client,
}

pub fn get_validators_app_token_from_env() -> Result<String, String> {
    std::env::var("VALIDATORS_APP_TOKEN").map_err(|err| format!("VALIDATORS_APP_TOKEN: {}", err))
}

impl Client {
    pub fn new<T: AsRef<str>>(api_token: T, cluster: ClusterJson) -> Self {
        let config = ClientConfig {
            api_token: api_token.as_ref().to_string(),
            cluster,
            ..ClientConfig::default()
        };
        Self::new_with_config(config)
    }

    pub fn new_with_cluster(cluster: Cluster) -> Result<Self, Box<dyn error::Error>> {
        let token = get_validators_app_token_from_env()?;
        let client = Self::new(token, ClusterJson::from_cluster(cluster));

        Ok(client)
    }

    pub fn new_with_config(config: ClientConfig) -> Self {
        let ClientConfig {
            base_url,
            cluster,
            api_token,
            timeout,
        } = config;
        Self {
            base_url: reqwest::Url::parse(&base_url).unwrap(),
            cluster,
            api_token,
            client: reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap(),
        }
    }

    fn request(
        &self,
        endpoint: Endpoint,
        query: &HashMap<String, String>,
    ) -> reqwest::Result<reqwest::blocking::Response> {
        let url = self.base_url.join(&endpoint.path(&self.cluster)).unwrap();
        info!("Requesting {}", url);
        let start = Instant::now();
        let request = self
            .client
            .get(url)
            .header(TOKEN_HTTP_HEADER_NAME, &self.api_token)
            .query(&query)
            .build()?;
        let result = self.client.execute(request);
        info!("Response took {:?}", Instant::now().duration_since(start));
        result
    }

    #[allow(dead_code)]
    pub fn ping(&self) -> reqwest::Result<()> {
        let response = self.request(Endpoint::Ping, &HashMap::new())?;
        response.json::<PingResponse>().map(|_| ())
    }

    pub fn validators(
        &self,
        sort: Option<SortKind>,
        limit: Option<Limit>,
    ) -> reqwest::Result<ValidatorsResponse> {
        let mut query = HashMap::new();
        if let Some(sort) = sort {
            query.insert("sort".into(), sort.to_string());
        }
        if let Some(limit) = limit {
            query.insert("limit".into(), limit.to_string());
        }
        let response = self.request(Endpoint::Validators, &query)?;
        response.json::<ValidatorsResponse>()
    }

    // See https://www.validators.app/api-documentation#commission-change-index
    // Note that the endpoint returns a different format from what is currently (Jan 2022) documented at this URL, and the endpoint is currently  described as experimental. So this may change.
    pub fn commission_change_index(
        &self,
        date_from: Option<DateTime<Utc>>,
        records_per_page: Option<i32>,
        page: Option<i32>,
    ) -> reqwest::Result<CommissionChangeIndexResponse> {
        let mut query: HashMap<String, String> = HashMap::new();

        if let Some(date_from) = date_from {
            query.insert("date_from".into(), date_from.format("%FT%T").to_string());
        }

        if let Some(records_per_page) = records_per_page {
            query.insert("per".into(), records_per_page.to_string());
        }

        if let Some(page) = page {
            query.insert("page".into(), page.to_string());
        }

        let response = self.request(Endpoint::CommissionChangeIndex, &query)?;
        response.json::<CommissionChangeIndexResponse>()
    }

    // Returns map of identity -> CommissionChangeIndexHistoryEntries
    pub fn get_all_commision_changes_since(
        &self,
        date_from: DateTime<Utc>,
    ) -> Result<HashMap<Pubkey, Vec<CommissionChangeIndexHistoryEntry>>, Box<dyn error::Error>>
    {
        let mut return_map: HashMap<Pubkey, Vec<CommissionChangeIndexHistoryEntry>> =
            HashMap::new();

        let mut page = 1;
        let records_per_page = 50;

        loop {
            let results =
                self.commission_change_index(Some(date_from), Some(records_per_page), Some(page))?;
            for record in results.commission_histories {
                let pubkey = Pubkey::from_str(record.account.as_str())?;

                // Ignore if there is no "after" value. Not sure if this is the right thing to do.
                if record.commission_after.is_some() {
                    let validator_records = return_map.entry(pubkey).or_insert_with(Vec::new);
                    validator_records.push(record);
                }
            }

            if page * records_per_page >= results.total_count {
                break;
            } else {
                page += 1;
            }
        }

        Ok(return_map)
    }
}
