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

#[derive(Clone, Deserialize, Serialize)]
pub struct ValidatorClassification {
    pub identity: Pubkey,

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
}

impl ValidatorClassification {
    // data center that the validator has been observed at the most
    pub fn primary_data_center(&self) -> DataCenterId {
        let mut primary_data_center = &DataCenterId::default();
        if let Some(ref data_center_residency) = self.data_center_residency {
            let mut primary_count = 0;

            for (data_center, count) in data_center_residency {
                if *count > primary_count {
                    primary_count = *count;
                    primary_data_center = data_center;
                }
            }
        }
        primary_data_center.clone()
    }

    pub fn stake_state_streak(&self) -> usize {
        let mut streak = 1;

        if let Some(ref stake_states) = self.stake_states {
            while streak < stake_states.len() && stake_states[0].0 == stake_states[streak].0 {
                streak += 1;
            }
        }
        streak
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
