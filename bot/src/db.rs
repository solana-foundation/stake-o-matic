use {
    crate::{
        data_center_info::{DataCenterId, DataCenterInfo},
        generic_stake_pool::ValidatorStakeState,
    },
    log::*,
    serde::{Deserialize, Serialize},
    solana_sdk::{clock::Epoch, pubkey::Pubkey},
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{self, Write},
        path::{Path, PathBuf},
    },
};

#[derive(Default, Clone, Deserialize, Serialize)]
pub struct ValidatorClassification {
    pub identity: Pubkey, // Validator identity
    pub vote_address: Pubkey,

    pub stake_state: ValidatorStakeState,
    pub stake_state_reason: String,

    // History of stake states, newest first, including (`stake_state`, `stake_state_reason`) at index 0
    pub stake_states: Option<Vec<(ValidatorStakeState, String)>>,

    // Informational notes regarding this validator
    pub notes: Vec<String>,

    // Map of data center to number of times the validator has been observed there.
    pub data_center_residency: Option<HashMap<DataCenterId, usize>>,

    // The data center that the validator was observed at for this classification
    pub current_data_center: Option<DataCenterId>,

    // The identity of the staking program participant, used to establish a link between
    // testnet and mainnet validator classifications
    pub participant: Option<Pubkey>,
}

impl ValidatorClassification {
    pub fn stake_state_streak(&self) -> usize {
        let mut streak = 1;

        if let Some(ref stake_states) = self.stake_states {
            while streak < stake_states.len() && stake_states[0].0 == stake_states[streak].0 {
                streak += 1;
            }
        }
        streak
    }

    // Was the validator staked for at last `n` of the last `m` epochs?
    pub fn staked_for(&self, n: usize, m: usize) -> bool {
        self.stake_states
            .as_ref()
            .map(|stake_states| {
                stake_states
                    .iter()
                    .take(m)
                    .filter(|(stake_state, _)| *stake_state != ValidatorStakeState::None)
                    .count()
                    >= n
            })
            .unwrap_or_default()
    }
}

pub type ValidatorClassificationByIdentity =
    HashMap<solana_sdk::pubkey::Pubkey, ValidatorClassification>;

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct EpochClassificationV1 {
    // Data Center observations for this epoch
    pub data_center_info: Vec<DataCenterInfo>,

    // `None` indicates a pause due to unusual observations during classification
    pub validator_classifications: Option<ValidatorClassificationByIdentity>,

    // Informational notes regarding this epoch
    pub notes: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub enum EpochClassification {
    V1(EpochClassificationV1),
}

impl Default for EpochClassification {
    fn default() -> Self {
        Self::V1(EpochClassificationV1::default())
    }
}

impl EpochClassification {
    pub fn new(v1: EpochClassificationV1) -> Self {
        EpochClassification::V1(v1)
    }

    pub fn into_current(self) -> EpochClassificationV1 {
        match self {
            EpochClassification::V1(v1) => v1,
        }
    }

    fn file_name<P>(epoch: Epoch, path: P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        path.as_ref().join(format!("epoch-{}.yml", epoch))
    }

    pub fn exists<P>(epoch: Epoch, path: P) -> bool
    where
        P: AsRef<Path>,
    {
        Self::file_name(epoch, path).exists()
    }

    pub fn load<P>(epoch: Epoch, path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(Self::file_name(epoch, path))?;
        serde_yaml::from_reader(file)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))
    }

    // Loads the first epoch older than `epoch` that contains `Some(validator_classifications)`.
    // Returns `Ok(None)` if no previous epochs are available
    pub fn load_previous<P>(epoch: Epoch, path: P) -> Result<Option<(Epoch, Self)>, io::Error>
    where
        P: AsRef<Path>,
    {
        let mut previous_epoch = epoch;
        loop {
            if previous_epoch == 0 {
                info!("No previous EpochClassification found");
                return Ok(None);
            }
            previous_epoch -= 1;

            if Self::exists(previous_epoch, &path) {
                let previous_epoch_classification =
                    Self::load(previous_epoch, &path)?.into_current();

                if previous_epoch_classification
                    .validator_classifications
                    .is_some()
                {
                    info!(
                        "Previous EpochClassification found for epoch {}",
                        previous_epoch
                    );
                    return Ok(Some((
                        previous_epoch,
                        Self::V1(previous_epoch_classification),
                    )));
                } else {
                    info!(
                        "Skipping previous EpochClassification for epoch {}",
                        previous_epoch
                    );
                }
            }
        }
    }

    // Loads the latest epoch that contains `Some(validator_classifications)`
    // Returns `Ok(None)` if no epoch is available
    pub fn load_latest<P>(path: P) -> Result<Option<(Epoch, Self)>, io::Error>
    where
        P: AsRef<Path>,
    {
        let epoch_filename_regex = regex::Regex::new(r"^epoch-(\d+).yml$").unwrap();

        let mut epochs = vec![];
        if let Ok(entries) = fs::read_dir(&path) {
            for entry in entries.filter_map(|entry| entry.ok()) {
                if entry.path().is_file() {
                    let filename = entry
                        .file_name()
                        .into_string()
                        .unwrap_or_else(|_| String::new());

                    if let Some(captures) = epoch_filename_regex.captures(&filename) {
                        epochs.push(captures.get(1).unwrap().as_str().parse::<u64>().unwrap());
                    }
                }
            }
        }
        epochs.sort_unstable();

        if let Some(latest_epoch) = epochs.last() {
            Self::load_previous(*latest_epoch + 1, path)
        } else {
            Ok(None)
        }
    }

    pub fn save<P>(&self, epoch: Epoch, path: P) -> Result<(), io::Error>
    where
        P: AsRef<Path>,
    {
        let serialized = serde_yaml::to_string(self)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))?;

        fs::create_dir_all(&path)?;
        let mut file = File::create(Self::file_name(epoch, path))?;
        file.write_all(&serialized.into_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_staked_for() {
        let mut vc = ValidatorClassification::default();

        assert_eq!(vc.staked_for(0, 0), false);
        assert_eq!(vc.staked_for(1, 0), false);
        assert_eq!(vc.staked_for(0, 1), false);

        vc.stake_states = Some(vec![
            (ValidatorStakeState::None, String::new()),
            (ValidatorStakeState::Baseline, String::new()),
            (ValidatorStakeState::Bonus, String::new()),
        ]);
        assert_eq!(vc.staked_for(3, 3), false);
        assert_eq!(vc.staked_for(2, 3), true);
    }
}
