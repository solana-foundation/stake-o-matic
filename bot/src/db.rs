use {
    crate::{
        data_center_info::{DataCenterId, DataCenterInfo},
        generic_stake_pool::ValidatorStakeState,
        InfrastructureConcentrationAffects,
    },
    log::*,
    semver::Version,
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
    pub identity: Pubkey,
    // Validator identity
    pub vote_address: Pubkey,

    pub stake_state: ValidatorStakeState,
    pub stake_state_reason: String,

    // Summary of the action was taken this epoch to advance the validator's stake
    pub stake_action: Option<String>,

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

    // The validator was not funded this epoch and should be prioritized next epoch
    pub prioritize_funding_in_next_epoch: Option<bool>,

    pub blocks: Option<usize>,
    pub slots: Option<usize>,

    pub vote_credits: Option<u64>,
    pub commission: Option<u8>,

    pub self_stake: Option<u64>,

    // Whether this is the first epoch the validator is a resident of current_data_center
    pub new_data_center_residency: Option<bool>,

    pub release_version: Option<Version>,

    // The number of times the validator has exceeded the max commission
    // Note we only started counting this around Jan 2022; epochs prior to Jan 2022 are not counted
    pub num_epochs_max_commission_exceeded: Option<u8>,

    // The number of times the validator was below max_commission at the end of one epoch, then above max_commission at
    // the end of a subsequent epoch
    // Note that we only started counting this around April/May 2022
    pub num_epochs_commission_increased_above_max: Option<u8>,

    // Whether the validator reported stats during the epoch
    // If false, String gives the reason for passing or failing
    pub self_reported_metrics: Option<(bool, String)>,

    /// Whether the validator meets the requirements for self-reporting metrics, and the reason why
    /// Note that this will be set whether self-reported metrics are required or not
    pub self_reported_metrics_summary: Option<(bool, String)>,
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

    // Config values from Config struct
    pub config: Option<EpochConfig>,

    // General info about the Epoch
    pub stats: Option<EpochStats>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct EpochStats {
    pub bonus_stake_amount: u64,
    pub min_epoch_credits: u64,
    pub avg_epoch_credits: u64,
    pub max_skip_rate: usize,
    pub cluster_average_skip_rate: usize,
    pub total_active_stake: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct EpochConfig {
    pub require_classification: Option<bool>,
    pub quality_block_producer_percentage: Option<usize>,
    pub max_poor_block_producer_percentage: Option<usize>,
    pub max_commission: Option<u8>,
    pub min_release_version: Option<semver::Version>,
    pub max_old_release_version_percentage: Option<usize>,
    pub max_poor_voter_percentage: Option<usize>,
    pub max_infrastructure_concentration: Option<f64>,
    pub infrastructure_concentration_affects: Option<InfrastructureConcentrationAffects>,
    pub bad_cluster_average_skip_rate: Option<usize>,
    pub min_epoch_credit_percentage_of_average: Option<usize>,
    pub min_self_stake_lamports: Option<u64>,
    pub max_active_stake_lamports: Option<u64>,
    pub enforce_min_self_stake: Option<bool>,
    pub enforce_testnet_participation: Option<bool>,
    pub min_testnet_participation: Option<(/*n:*/ usize, /*m:*/ usize)>,
    pub baseline_stake_amount_lamports: Option<u64>,
    pub require_performance_metrics_reporting: Option<bool>,
    pub performance_waiver_release_version: Option<semver::Version>,
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
    // Note that the epoch returned is the epoch _after_ the epoch being evaluated in the file
    pub fn load_previous<P>(epoch: Epoch, path: P) -> Result<Option<(Epoch, Self)>, io::Error>
    where
        P: AsRef<Path>,
    {
        let mut previous_epoch = epoch;
        loop {
            if previous_epoch == 0 {
                info!(
                    "No previous EpochClassification found at {}",
                    path.as_ref().display()
                );
                return Ok(None);
            }
            previous_epoch -= 1;

            if Self::exists(previous_epoch, &path) {
                let previous_epoch_classification =
                    Self::load_if_validators_classified(previous_epoch, &path)?;

                if let Some(epoch_classification) = previous_epoch_classification {
                    info!(
                        "Previous EpochClassification found for epoch {} at {}",
                        previous_epoch,
                        path.as_ref().display()
                    );
                    return Ok(Some((
                        previous_epoch,
                        Self::V1(epoch_classification.into_current()),
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

    // Returns the EpochClassification for `epoch` at `path` if it exists and if it contains validator_classifications
    // (that is, if stake was adjusted for validators for the epoch)
    pub fn load_if_validators_classified<P>(
        epoch: Epoch,
        path: P,
    ) -> Result<Option<Self>, io::Error>
    where
        P: AsRef<Path> + Copy,
    {
        if Self::exists(epoch, path) {
            let epoch_classification = Self::load(epoch, &path)?.into_current();

            if epoch_classification.validator_classifications.is_some() {
                return Ok(Some(Self::V1(epoch_classification)));
            }
        }
        Ok(None)
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DryRunStats {
    pub none_count: u64,
    pub baseline_count: u64,
    pub bonus_count: u64,
}

/**
 Holds information about dry runs.
*/
impl DryRunStats {
    fn file_name<P>(epoch: Epoch, path: P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        path.as_ref().join(format!("epoch-{}-dryrun.yml", epoch))
    }

    pub fn exists<P>(epoch: Epoch, path: P) -> bool
    where
        P: AsRef<Path>,
    {
        Path::new(&Self::file_name(epoch, path)).exists()
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
