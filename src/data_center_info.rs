use {
    crate::validators_app,
    log::*,
    serde::{Deserialize, Serialize},
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashMap, error, str::FromStr},
};

const DATA_CENTER_ID_UNKNOWN: &str = "0-Unknown";

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct DataCenterId {
    pub asn: u64,
    pub location: String,
}

impl Default for DataCenterId {
    fn default() -> Self {
        Self::from_str(DATA_CENTER_ID_UNKNOWN).unwrap()
    }
}

impl std::str::FromStr for DataCenterId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, '-');
        let asn = parts.next();
        let location = parts.next();
        if let (Some(asn), Some(location)) = (asn, location) {
            let asn = asn.parse().map_err(|e| format!("{:?}", e))?;
            let location = location.to_string();
            Ok(Self { asn, location })
        } else {
            Err(format!("cannot construct DataCenterId from input: {}", s))
        }
    }
}

impl std::fmt::Display for DataCenterId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.asn, self.location)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataCenterInfo {
    pub id: DataCenterId,
    pub stake: u64,
    pub stake_percent: f64,
    pub validators: Vec<Pubkey>,
}

impl DataCenterInfo {
    pub fn new(id: DataCenterId) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }
}

impl std::fmt::Display for DataCenterInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:<30}  {:>20}  {:>5.2}  {}",
            self.id.to_string(),
            self.stake,
            self.stake_percent,
            self.validators.len()
        )
    }
}

#[derive(Default)]
pub struct DataCenters {
    pub info: Vec<DataCenterInfo>,
    pub by_identity: HashMap<Pubkey, DataCenterId>,
}

pub fn get() -> Result<DataCenters, Box<dyn error::Error>> {
    let token = std::env::var("VALIDATORS_APP_TOKEN")?;
    let client = validators_app::Client::new(token);
    let validators = client.validators(None, None)?;
    let mut data_center_map = HashMap::new();
    let mut total_stake = 0;
    let mut unknown_data_center_stake: u64 = 0;

    let mut by_identity = HashMap::new();
    for v in validators.as_ref() {
        let identity = v
            .account
            .as_ref()
            .and_then(|pubkey| Pubkey::from_str(pubkey).ok());
        let identity = if let Some(identity) = identity {
            identity
        } else {
            warn!("No identity for: {:?}", v);
            continue;
        };

        let stake = v.active_stake.unwrap_or(0);

        let data_center = v
            .data_center_key
            .as_deref()
            .or_else(|| {
                unknown_data_center_stake = unknown_data_center_stake.saturating_add(stake);
                None
            })
            .unwrap_or(DATA_CENTER_ID_UNKNOWN);
        let data_center_id = DataCenterId::from_str(data_center)
            .map_err(|e| {
                unknown_data_center_stake = unknown_data_center_stake.saturating_add(stake);
                e
            })
            .unwrap_or_default();

        by_identity.insert(identity, data_center_id.clone());

        let mut data_center_info = data_center_map
            .entry(data_center_id.clone())
            .or_insert_with(|| DataCenterInfo::new(data_center_id));
        data_center_info.stake += stake;
        total_stake += stake;
        data_center_info.validators.push(identity);
    }

    let unknown_percent = 100f64 * (unknown_data_center_stake as f64) / total_stake as f64;
    if unknown_percent > 3f64 {
        warn!("unknown data center percentage: {:.0}%", unknown_percent);
    }

    let info = data_center_map
        .drain()
        .map(|(_, mut i)| {
            i.stake_percent = 100f64 * i.stake as f64 / total_stake as f64;
            i
        })
        .collect();
    Ok(DataCenters { info, by_identity })
}
