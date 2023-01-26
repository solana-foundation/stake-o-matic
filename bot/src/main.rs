use crate::data_center_info::{DataCenterInfo, DataCenters};
use crate::performance_db_utils::{
    get_reported_performance_metrics, NUM_SAMPLED_REPORTING_EPOCHS, SUCCESS_MIN_PERCENT,
};
use crate::slack_utils::send_slack_channel_message;
use crate::stake_pool_v0::MIN_STAKE_ACCOUNT_BALANCE;
use crate::validators_app::CommissionChangeIndexHistoryEntry;
use crate::Cluster::{MainnetBeta, Testnet};
use std::env;
use {
    crate::{db::*, generic_stake_pool::*, rpc_client_utils::*},
    chrono::{Duration as ChronoDuration, Utc},
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, values_t, App, AppSettings, Arg,
        ArgMatches, SubCommand,
    },
    itertools::Itertools,
    log::*,
    openssl::rsa::{Padding, Rsa},
    serde::{Deserialize, Serialize},
    solana_clap_utils::{
        input_parsers::{keypair_of, lamports_of_sol, pubkey_of},
        input_validators::{
            is_amount, is_keypair, is_parsable, is_pubkey_or_keypair, is_url, is_valid_percentage,
        },
    },
    solana_client::rpc_client::RpcClient,
    solana_foundation_delegation_program_cli::get_participants_with_state,
    solana_foundation_delegation_program_registry::state::{Participant, ParticipantState},
    solana_notifier::Notifier,
    solana_sdk::{
        account::from_account,
        account_utils::StateMut,
        clock::{Epoch, Slot},
        commitment_config::CommitmentConfig,
        native_token::*,
        pubkey::Pubkey,
        slot_history::{self, SlotHistory},
        stake::{self, state::StakeState},
        stake_history::StakeHistory,
        sysvar,
    },
    solana_vote_program::vote_state::VoteState,
    std::{
        collections::{HashMap, HashSet},
        error, fs,
        fs::File,
        io::{Read, Write},
        path::PathBuf,
        process,
        str::FromStr,
        sync::Arc,
        time::Duration,
    },
    thiserror::Error,
};

mod data_center_info;
mod db;
mod generic_stake_pool;
mod noop_stake_pool;
mod performance_db_utils;
mod rpc_client_utils;
mod slack_utils;
mod stake_pool;
mod stake_pool_v0;
mod validator_list;
mod validators_app;

type BoxResult<T> = Result<T, Box<dyn error::Error>>;
type ValidatorList = HashSet<Pubkey>;
type IdentityToParticipant = HashMap<Pubkey, Pubkey>;

pub enum InfrastructureConcentrationAffectKind {
    Destake(String),
    Warn(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum InfrastructureConcentrationAffects {
    WarnAll,
    DestakeListed(ValidatorList),
    DestakeAll,
    DestakeNew,
    DestakeOverflow,
}

#[derive(Debug, Error)]
#[error("cannot convert to InfrastructureConcentrationAffects: {0}")]
pub struct InfrastructureConcentrationAffectsFromStrError(String);

impl FromStr for InfrastructureConcentrationAffects {
    type Err = InfrastructureConcentrationAffectsFromStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "warn" => Ok(Self::WarnAll),
            "destake-all" => Ok(Self::DestakeAll),
            "destake-new" => Ok(Self::DestakeNew),
            "destake-overflow" => Ok(Self::DestakeOverflow),
            _ => {
                let file = File::open(s)
                    .map_err(|_| InfrastructureConcentrationAffectsFromStrError(s.to_string()))?;
                let mut list: Vec<String> = serde_yaml::from_reader(file)
                    .map_err(|_| InfrastructureConcentrationAffectsFromStrError(s.to_string()))?;
                let list = list
                    .drain(..)
                    .filter_map(|ref s| Pubkey::from_str(s).ok())
                    .collect();
                Ok(Self::DestakeListed(list))
            }
        }
    }
}

fn is_release_version(string: String) -> Result<(), String> {
    if string.starts_with('v') && semver::Version::parse(string.split_at(1).1).is_ok() {
        return Ok(());
    }
    semver::Version::parse(&string)
        .map(|_| ())
        .map_err(|err| format!("{:?}", err))
}

fn release_version_of(matches: &ArgMatches<'_>, name: &str) -> Option<semver::Version> {
    matches
        .value_of(name)
        .map(ToString::to_string)
        .map(|string| {
            if string.starts_with('v') {
                semver::Version::parse(string.split_at(1).1)
            } else {
                semver::Version::parse(&string)
            }
            .expect("semver::Version")
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Cluster {
    Testnet,
    MainnetBeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum OutputMode {
    Yes,
    First,
    No,
}

impl std::fmt::Display for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Testnet => "testnet",
                Self::MainnetBeta => "mainnet-beta",
            }
        )
    }
}

#[derive(Debug, Serialize)]
pub struct Config {
    json_rpc_url: String,
    websocket_url: String,
    participant_json_rpc_url: String,
    cluster: Cluster,
    db_path: PathBuf,
    db_suffix: String,
    require_classification: bool,
    csv_output_mode: OutputMode,
    epoch_classification: OutputMode,

    /// Perform all stake processing, without sending transactions to the network
    dry_run: bool,

    /// Quality validators produce within this percentage of the cluster average skip rate over
    /// the previous epoch
    quality_block_producer_percentage: usize,

    /// Don't ever unstake more than this percentage of the cluster at one time for poor block
    /// production
    max_poor_block_producer_percentage: usize,

    /// Vote accounts with a larger commission than this amount will not be staked.
    max_commission: u8,

    /// If Some(), destake validators with a version less than this version subject to the
    /// `max_old_release_version_percentage` limit
    min_release_version: Option<semver::Version>,

    /// Do not unstake more than this percentage of the cluster at one time for running an
    /// older software version
    max_old_release_version_percentage: usize,

    /// Do not unstake more than this percentage of the cluster at one time for being poor
    /// voters
    max_poor_voter_percentage: usize,

    /// Base path of confirmed block cache
    confirmed_block_cache_path: PathBuf,

    /// Vote accounts sharing infrastructure with larger than this amount will not be staked
    /// None: skip infrastructure concentration check
    max_infrastructure_concentration: Option<f64>,

    /// How validators with infrastruction concentration above `max_infrastructure_concentration`
    /// will be affected. Accepted values are:
    /// 1) "warn"       - Stake unaffected. A warning message is notified
    /// 2) "destake"    - Removes all validator stake
    /// 3) PATH_TO_YAML - Reads a list of validator identity pubkeys from the specified YAML file
    ///                   destaking those in the list and warning any others
    /// 4) "destake-new" - When infrastructure concentration is too high, only destake validators
    ///                    who are new to the data center
    /// 5) "destake-overflow" = Destake "junior" validators who are causing the infrastructure to be
    ///                         over max_infrastructure_concentration
    infrastructure_concentration_affects: InfrastructureConcentrationAffects,

    bad_cluster_average_skip_rate: usize,

    /// Destake if the validator's vote credits for the latest full epoch are less than this percentage
    /// of the cluster average
    min_epoch_credit_percentage_of_average: usize,

    /// Minimum amount of lamports a validator must stake on itself to be eligible for a delegation
    min_self_stake_lamports: u64,

    /// identities of validators who don't have to meet the min_self_stake requirement
    min_self_stake_exceptions: Vec<Pubkey>,

    /// Validators with more than this amount of active stake are not eligible for a delegation
    max_active_stake_lamports: u64,

    /// If true, enforce the `min_self_stake_lamports` limit. If false, only warn on insufficient stake
    enforce_min_self_stake: bool,

    /// If true, enforce `min_testnet_staked_epochs`. If false, only warn if
    /// `min_testnet_staked_epochs` is Some.
    ///
    /// This setting is ignored if `cluster` is not `"mainnet-beta"` or `min_testnet_participation
    /// is `None`.
    enforce_testnet_participation: bool,

    /// If Some, require that the participant's mainnet-beta validator be staked for `n` out of the
    /// last `m` epochs to be delegable for mainnet-beta stake
    ///
    /// This setting is ignored if `cluster` is not `"mainnet-beta"`
    min_testnet_participation: Option<(/*n:*/ usize, /*m:*/ usize)>,

    /// Stake amount earned for baseline
    baseline_stake_amount_lamports: Option<u64>,

    /// Whether to require that validators report their performance metrics
    require_performance_metrics_reporting: bool,

    /// URL and token for the performance metrics  influxdb
    performance_db_url: Option<String>,
    performance_db_token: Option<String>,
    blocklist_datacenter_asns: Option<Vec<u64>>,
    require_dry_run_to_distribute_stake: bool,
}

const DEFAULT_MAINNET_BETA_JSON_RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const DEFAULT_TESTNET_JSON_RPC_URL: &str = "https://api.testnet.solana.com";

impl Config {
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self {
            json_rpc_url: DEFAULT_MAINNET_BETA_JSON_RPC_URL.to_string(),
            websocket_url: solana_cli_config::Config::compute_websocket_url(
                DEFAULT_MAINNET_BETA_JSON_RPC_URL,
            ),
            participant_json_rpc_url: DEFAULT_MAINNET_BETA_JSON_RPC_URL.to_string(),
            cluster: Cluster::MainnetBeta,
            db_path: PathBuf::default(),
            db_suffix: "".to_string(),
            csv_output_mode: OutputMode::No,
            epoch_classification: OutputMode::No,
            require_classification: false,
            dry_run: true,
            quality_block_producer_percentage: 15,
            max_poor_block_producer_percentage: 20,
            max_commission: 100,
            min_release_version: None,
            max_old_release_version_percentage: 10,
            max_poor_voter_percentage: 20,
            confirmed_block_cache_path: default_confirmed_block_cache_path(),
            max_infrastructure_concentration: Some(100.0),
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::WarnAll,
            bad_cluster_average_skip_rate: 50,
            min_epoch_credit_percentage_of_average: 50,
            min_self_stake_lamports: 0,
            // TODO: this should be empty
            min_self_stake_exceptions: vec![],
            max_active_stake_lamports: u64::MAX,
            enforce_min_self_stake: false,
            enforce_testnet_participation: false,
            min_testnet_participation: None,
            baseline_stake_amount_lamports: None,
            performance_db_url: None,
            performance_db_token: None,
            require_performance_metrics_reporting: false,
            blocklist_datacenter_asns: None,
            require_dry_run_to_distribute_stake: false,
        }
    }

    fn cluster_db_path_for(&self, cluster: Cluster) -> PathBuf {
        if self.db_suffix.is_empty() {
            self.db_path.join(format!("data-{}", cluster))
        } else {
            self.db_path
                .join(format!("data-{}-{}", cluster, self.db_suffix))
        }
    }

    fn cluster_db_path(&self) -> PathBuf {
        self.cluster_db_path_for(self.cluster)
    }
}

fn default_confirmed_block_cache_path() -> PathBuf {
    let home_dir = std::env::var("HOME").unwrap();
    PathBuf::from(home_dir).join(".cache/solana/som/confirmed-block-cache/")
}

fn app_version() -> String {
    // Determine version based on the environment variables set by Github Actions
    let tag = option_env!("GITHUB_REF")
        .and_then(|github_ref| github_ref.strip_prefix("refs/tags/").map(|s| s.to_string()));

    tag.unwrap_or_else(|| match option_env!("GITHUB_SHA") {
        None => "devbuild".to_string(),
        Some(commit) => commit[..8].to_string(),
    })
}

fn get_config() -> BoxResult<(Config, Arc<RpcClient>, Box<dyn GenericStakePool>)> {
    let default_confirmed_block_cache_path = default_confirmed_block_cache_path()
        .to_str()
        .unwrap()
        .to_string();
    let app_version = &*app_version();
    let min_stake_account_balance = &*lamports_to_sol(MIN_STAKE_ACCOUNT_BALANCE).to_string();
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(app_version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::InferSubcommands)
        .arg(
            Arg::with_name("json_rpc_url")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .multiple(true)
                .validator(is_url)
                .help("JSON RPC URLs for the cluster. Bot will use first URL that works")
        )
        .arg(
            Arg::with_name("participant_json_rpc_url")
                .long("participant-url")
                .value_name("URL")
                .takes_value(true)
                .validator(is_url)
                .default_value(DEFAULT_MAINNET_BETA_JSON_RPC_URL)
                .help("JSON RPC URL for the participant cluster, typically a mainnet URL")
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .value_name("NAME")
                .possible_values(&["mainnet-beta", "testnet"])
                .takes_value(true)
                .default_value("testnet")
                .required(true)
                .help("Name of the cluster to operate on")
        )
        .arg(
            Arg::with_name("confirm")
                .long("confirm")
                .takes_value(false)
                .help("Confirm that the stake adjustments should actually be made")
        )
        .arg(
            Arg::with_name("csv-output-mode")
                .long("csv-output-mode")
                .value_name("no|yes|first")
                .takes_value(true)
                .default_value("no")
                .possible_values(&["no", "yes", "first"])
                .help("Output summary CSV.  If \"first\", CSV will only be generated on the first run.  If \"yes\", CSV will always be generated. If \"no\", no CSV is ever generated.")
        )
        .arg(
            Arg::with_name("epoch_classification")
                .long("epoch-classification")
                .value_name("no|yes|first")
                .takes_value(true)
                .default_value("no")
                .possible_values(&["no", "yes", "first"])
                .help("Output epoch classification.  If \"first\", classification will only be output on the first run.  If \"yes\", classification will always be dumped. If \"no\", no classification is ever dumped.")
        )
        .arg(
            Arg::with_name("db_path")
                .long("db-path")
                .value_name("PATH")
                .takes_value(true)
                .default_value("db")
                .help("Location for storing staking history")
        )
        .arg(
            Arg::with_name("db_suffix")
                .long("db-suffix")
                .value_name("SUFFIX")
                .takes_value(true)
                .default_value("")
                .help("Suffix for filename storing staking history")
        )
        .arg(
            Arg::with_name("require_classification")
                .long("require-classification")
                .takes_value(false)
                .help("Fail if the classification for the previous epoch does not exist")
        )
        .arg(
            Arg::with_name("quality_block_producer_percentage")
                .long("quality-block-producer-percentage")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("15")
                .validator(is_valid_percentage)
                .help("Quality validators have a skip rate within this percentage of \
                       the cluster average in the previous epoch.")
        )
        .arg(
            Arg::with_name("bad_cluster_average_skip_rate")
                .long("bad-cluster-average-skip-rate")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("50")
                .validator(is_valid_percentage)
                .help("Threshold to notify for a poor average cluster skip rate.")
        )
        .arg(
            Arg::with_name("max_poor_block_producer_percentage")
                .long("max-poor-block-producer-percentage")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("20")
                .validator(is_valid_percentage)
                .help("Do not add or remove bonus stake if at least this \
                       percentage of all validators are poor block producers")
        )
        .arg(
            Arg::with_name("min_epoch_credit_percentage_of_average")
                .long("min-epoch-credit-percentage-of-average")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("50")
                .validator(is_valid_percentage)
                .help("Validator vote credits for the latest full epoch must \
                       be at least this percentage of the cluster average vote credits")
        )
        .arg(
            Arg::with_name("max_commission")
                .long("max-commission")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("100")
                .validator(is_valid_percentage)
                .help("Vote accounts with a larger commission than this amount will not be staked")
        )
        .arg(
            Arg::with_name("min_release_version")
                .long("min-release-version")
                .value_name("SEMVER")
                .takes_value(true)
                .validator(is_release_version)
                .help("Remove the base and bonus stake from validators with \
                       a release version older than this one")
        )
        .arg(
            Arg::with_name("max_poor_voter_percentage")
                .long("max-poor-voter-percentage")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("20")
                .validator(is_valid_percentage)
                .help("Do not remove stake from validators poor voting history \
                       if more than this percentage of all validators have a \
                       poor voting history")
        )
        .arg(
            Arg::with_name("max_old_release_version_percentage")
                .long("max-old-release-version-percentage")
                .value_name("PERCENTAGE")
                .takes_value(true)
                .default_value("10")
                .validator(is_valid_percentage)
                .help("Do not remove stake from validators running older \
                       software versions if more than this percentage of \
                       all validators are running an older software version")
        )
        .arg(
            Arg::with_name("confirmed_block_cache_path")
                .long("confirmed-block-cache-path")
                .takes_value(true)
                .value_name("PATH")
                .default_value(&default_confirmed_block_cache_path)
                .help("Base path of confirmed block cache")
        )
        .arg(
            Arg::with_name("max_infrastructure_concentration")
                .long("max-infrastructure-concentration")
                .takes_value(true)
                .value_name("PERCENTAGE")
                .validator(is_valid_percentage)
                .help("Vote accounts sharing infrastructure with larger than this amount will not be staked")
        )
        .arg(
            Arg::with_name("infrastructure_concentration_affects")
                .long("infrastructure-concentration-affects")
                .takes_value(true)
                .value_name("AFFECTS")
                .default_value("warn")
                .validator(|ref s| {
                    InfrastructureConcentrationAffects::from_str(s)
                        .map(|_| ())
                        .map_err(|e| format!("{}", e))
                })
                .help("How validators with infrastruction concentration above \
                       `max_infrastructure_concentration` will be affected. \
                       Accepted values are:\n\
                       1) warn         - Stake unaffected. A warning message is notified\n\
                       2) destake-new  - Will not stake new validators, existing validator retain their stake\n\
                       3) destake-all  - Removes all validator stake \n\
                       4) PATH_TO_YAML - Reads a list of validator identity \
                                         pubkeys from the specified YAML file \
                                         destaking those in the list and warning \
                                         any others")
        )
        .arg(
            Arg::with_name("min_self_stake")
                .long("min-self-stake")
                .value_name("AMOUNT")
                .takes_value(true)
                .validator(is_amount)
                .default_value("0")
                .required(true)
                .help("Minimum amount of SOL a validator must stake on itself to be eligible for a delegation"),
        )
        .arg(
            Arg::with_name("max_active_stake")
                .long("max-active-stake")
                .value_name("AMOUNT")
                .takes_value(true)
                .validator(is_amount)
                .default_value("3500000")
                .required(true)
                .help("Maximum amount of stake a validator may have to be eligible for a delegation"),
        )
        .arg(
            Arg::with_name("enforce_min_self_stake")
                .long("enforce-min-self-stake")
                .takes_value(false)
                .help("Enforce the minimum self-stake requirement")
        )
        .arg(
            Arg::with_name("min_self_stake_exceptions_file")
                .long("min-self-stake-exceptions-file")
                .takes_value(true)
                .value_name("YAML_PATH")
        )
        .arg(
            Arg::with_name("min_self_stake_exceptions_key")
                .long("min-self-stake-exceptions-key")
                .takes_value(true)
                .value_name("KEY")
                .help("Private key")
        )
        .arg(
            Arg::with_name("min_testnet_participation")
                .long("min-testnet-participation")
                .value_name("N M")
                .multiple(true)
                .min_values(2)
                .max_values(2)
                .validator(is_parsable::<usize>)
                .help("Require that the participant's mainnet-beta validator be staked for N out of the \
                       last M epochs to be delegable for mainnet-beta stake.\n\
                       This setting is ignored if the --cluster is not `mainnet-beta`")
        )
        .arg(
            Arg::with_name("enforce_testnet_participation")
                .long("enforce-testnet-participation")
                .takes_value(false)
                .help("Enforce the minimum testnet participation requirement.\n
                       This setting is ignored if the --cluster is not `mainnet-beta`")
        )
        .arg(
            Arg::with_name("require_performance_metrics_reporting")
                .long("require-performance-metrics-reporting")
                .takes_value(false)
                .help("Require that validators report their performance metrics`")
        )
        .arg(
            Arg::with_name("performance_db_url")
                .long("performance-db-url")
                .takes_value(true)
                .value_name("URL")
                .help("URL of InfluxDB used to collect self-reported performance data")
        )
        .arg(
            Arg::with_name("performance_db_token")
                .long("performance-db-token")
                .takes_value(true)
                .value_name("TOKEN")
                .help("Token used to authenticate for InfluxDB connection")
        )
        .arg(
            Arg::with_name("blocklist_datacenter_asns")
                .multiple(true)
                .long("blocklist-datacenter-asns")
                .takes_value(true)
                .value_name("ASNS")
                .help("List of data center ASNS. Validators in these data centers will be destaked")
        )
        .arg(
            Arg::with_name("require_dry_run_to_distribute_stake")
                .long("require-dry-run-to-distribute-stake")
                .takes_value(false)
                .help("If set, only distribute stake if there is a dry run summary in the wiki repo")
        )
        .subcommand(
            SubCommand::with_name("stake-pool-v0").about("Use the stake-pool v0 solution")
                .arg(
                    Arg::with_name("reserve_stake_address")
                        .index(1)
                        .value_name("RESERVE_STAKE_ADDRESS")
                        .takes_value(true)
                        .required(true)
                        .validator(is_pubkey_or_keypair)
                        .help("The reserve stake account used to fund the stake pool")
                )
                .arg(
                    Arg::with_name("authorized_staker")
                        .index(2)
                        .value_name("KEYPAIR")
                        .validator(is_keypair)
                        .required(true)
                        .takes_value(true)
                        .help("Keypair of the authorized staker")
                )
                .arg(
                    Arg::with_name("min_reserve_stake_balance")
                        .long("min-reserve-stake-balance")
                        .value_name("SOL")
                        .takes_value(true)
                        .default_value(min_stake_account_balance)
                        .validator(is_amount)
                        .help("The minimum balance to keep in the reserve stake account")
                )
                .arg(
                    Arg::with_name("baseline_stake_amount")
                        .index(3)
                        .value_name("SOL")
                        .validator(is_amount)
                        .required(true)
                        .takes_value(true)
                        .help("The baseline SOL amount to stake to validators with adequate performance")
                )
        )
        .subcommand(
            SubCommand::with_name("stake-pool").about("Use a stake pool")
                .arg(
                    Arg::with_name("pool_address")
                        .index(1)
                        .value_name("POOL_ADDRESS")
                        .takes_value(true)
                        .required(true)
                        .validator(is_pubkey_or_keypair)
                        .help("The stake pool address")
                )
                .arg(
                    Arg::with_name("authorized_staker")
                        .index(2)
                        .value_name("KEYPAIR")
                        .validator(is_keypair)
                        .required(true)
                        .takes_value(true)
                        .help("Keypair of the authorized staker")
                )
                .arg(
                    Arg::with_name("min_reserve_stake_balance")
                        .long("min-reserve-stake-balance")
                        .value_name("SOL")
                        .takes_value(true)
                        .default_value(min_stake_account_balance)
                        .validator(is_amount)
                        .help("The minimum balance to keep in the reserve stake account")
                )
                .arg(
                    Arg::with_name("baseline_stake_amount")
                        .index(3)
                        .value_name("SOL")
                        .validator(is_amount)
                        .required(true)
                        .takes_value(true)
                        .help("The baseline SOL amount to stake to validators with adequate performance")
                )
        )
        .subcommand(
            SubCommand::with_name("noop-stake-pool").about("Use a no-op stake pool.  Useful for testing classification and generating output from an existing db.")
        )
        .get_matches();

    let dry_run = !matches.is_present("confirm");
    if dry_run {
        info!("Doing a dry run; stake will not be distributed");
    }

    let cluster = match value_t_or_exit!(matches, "cluster", String).as_str() {
        "mainnet-beta" => Cluster::MainnetBeta,
        "testnet" => Cluster::Testnet,
        _ => unreachable!(),
    };
    let quality_block_producer_percentage =
        value_t_or_exit!(matches, "quality_block_producer_percentage", usize);
    let min_epoch_credit_percentage_of_average =
        value_t_or_exit!(matches, "min_epoch_credit_percentage_of_average", usize);
    let max_commission = value_t_or_exit!(matches, "max_commission", u8);
    let max_poor_voter_percentage = value_t_or_exit!(matches, "max_poor_voter_percentage", usize);
    let max_poor_block_producer_percentage =
        value_t_or_exit!(matches, "max_poor_block_producer_percentage", usize);
    let max_old_release_version_percentage =
        value_t_or_exit!(matches, "max_old_release_version_percentage", usize);
    let min_release_version = release_version_of(&matches, "min_release_version");

    let enforce_min_self_stake = matches.is_present("enforce_min_self_stake");
    let min_self_stake_lamports = lamports_of_sol(&matches, "min_self_stake").unwrap();

    let min_self_stake_exceptions = match matches.value_of("min_self_stake_exceptions_file") {
        Some(filename) => {
            let mut file = File::open(filename)?;

            let mut list: Vec<String> = match matches.value_of("min_self_stake_exceptions_key") {
                Some(key_str) => {
                    info!("Attempting to decrypt {:?}", filename);

                    let metadata = fs::metadata(&filename).expect("unable to read metadata");
                    let mut file_buffer = vec![0; metadata.len() as usize];
                    file.read_exact(&mut file_buffer)?;

                    let key = base64::decode(key_str)?;
                    let rsa = Rsa::private_key_from_der(&*key)?;
                    let mut out_buffer: Vec<u8> = vec![0; rsa.size() as usize];
                    let _ = rsa
                        .private_decrypt(&*file_buffer, &mut out_buffer, Padding::PKCS1)
                        .unwrap();
                    let text = String::from_utf8(out_buffer)?;
                    info!("File decrypted");

                    serde_yaml::from_str(&text)?
                }
                _ => serde_yaml::from_reader(file)?,
            };

            list.drain(..)
                .filter_map(|ref s| Pubkey::from_str(s).ok())
                .collect()
        }
        _ => vec![],
    };

    debug!("min_self_stake_exceptions: {:?}", min_self_stake_exceptions);

    let max_active_stake_lamports = lamports_of_sol(&matches, "max_active_stake").unwrap();

    let enforce_testnet_participation = matches.is_present("enforce_testnet_participation");
    let min_testnet_participation = values_t!(matches, "min_testnet_participation", usize)
        .ok()
        .map(|v| (v[0], v[1]));
    if min_testnet_participation.is_some() && cluster != Cluster::MainnetBeta {
        error!("--min-testnet-participation only available for `--cluster mainnet-beta`");
        process::exit(1);
    }

    let db_path = value_t_or_exit!(matches, "db_path", PathBuf);
    let db_suffix = matches.value_of("db_suffix").unwrap().to_string();
    let csv_output_mode = match value_t_or_exit!(matches, "csv-output-mode", String).as_str() {
        "first" => OutputMode::First,
        "yes" => OutputMode::Yes,
        "no" => OutputMode::No,
        _ => unreachable!(),
    };
    let epoch_classification =
        match value_t_or_exit!(matches, "epoch_classification", String).as_str() {
            "first" => OutputMode::First,
            "yes" => OutputMode::Yes,
            "no" => OutputMode::No,
            _ => unreachable!(),
        };
    let require_classification = matches.is_present("require_classification");

    let confirmed_block_cache_path = matches
        .value_of("confirmed_block_cache_path")
        .map(PathBuf::from)
        .unwrap();

    let bad_cluster_average_skip_rate =
        value_t!(matches, "bad_cluster_average_skip_rate", usize).unwrap_or(50);
    let max_infrastructure_concentration =
        value_t!(matches, "max_infrastructure_concentration", f64).ok();
    let infrastructure_concentration_affects = value_t!(
        matches,
        "infrastructure_concentration_affects",
        InfrastructureConcentrationAffects
    )
    .unwrap();

    let require_performance_metrics_reporting =
        matches.is_present("require_performance_metrics_reporting");

    let performance_db_url = matches.value_of("performance_db_url").map(str::to_string);
    let performance_db_token = matches.value_of("performance_db_token").map(str::to_string);

    let blocklist_datacenter_asns = values_t!(matches, "blocklist_datacenter_asns", u64).ok();

    let require_dry_run_to_distribute_stake =
        matches.is_present("require_dry_run_to_distribute_stake");

    let default_json_rpc_url = match cluster {
        Cluster::MainnetBeta => DEFAULT_MAINNET_BETA_JSON_RPC_URL,
        Cluster::Testnet => DEFAULT_TESTNET_JSON_RPC_URL,
    }
    .to_string();

    // Create a list of RPC URLs to try. The first URL that returns a successful "getHealth" response
    // will be used for all requests
    let json_rpc_urls_to_try: Vec<String> = match values_t!(matches, "json_rpc_url", String) {
        Ok(argument_urls) => {
            let mut urls = argument_urls;
            urls.push(default_json_rpc_url);

            urls
        }
        _ => {
            vec![default_json_rpc_url]
        }
    };

    let (rpc_client, json_rpc_url) = json_rpc_urls_to_try
        .iter()
        .map(|url| {
            let rpc_client = Arc::new(RpcClient::new_with_timeout(
                url.clone(),
                Duration::from_secs(180),
            ));
            (rpc_client, url.clone())
        })
        .find(|(rpc_client, url)| {
            info!("Checking health of {}", url);
            matches!(check_rpc_health(rpc_client), Ok(_))
        })
        .unwrap_or_else(|| {
            error!("All RPC servers are unhealthy. Exiting.");
            process::exit(1);
        });

    info!("using RPC URL: {}", json_rpc_url);

    let websocket_url = solana_cli_config::Config::compute_websocket_url(&json_rpc_url);
    let participant_json_rpc_url = matches
        .value_of("participant_json_rpc_url")
        .unwrap()
        .to_string();

    let mut config = Config {
        json_rpc_url,
        websocket_url,
        participant_json_rpc_url,
        cluster,
        db_path,
        db_suffix,
        require_classification,
        csv_output_mode,
        epoch_classification,
        dry_run,
        quality_block_producer_percentage,
        max_poor_block_producer_percentage,
        max_commission,
        min_release_version,
        max_old_release_version_percentage,
        max_poor_voter_percentage,
        confirmed_block_cache_path,
        max_infrastructure_concentration,
        infrastructure_concentration_affects,
        bad_cluster_average_skip_rate,
        min_epoch_credit_percentage_of_average,
        min_self_stake_lamports,
        min_self_stake_exceptions,
        max_active_stake_lamports,
        enforce_min_self_stake,
        enforce_testnet_participation,
        min_testnet_participation,
        baseline_stake_amount_lamports: None,
        require_performance_metrics_reporting,
        performance_db_url,
        performance_db_token,
        blocklist_datacenter_asns,
        require_dry_run_to_distribute_stake,
    };

    let stake_pool: Box<dyn GenericStakePool> = match matches.subcommand() {
        ("stake-pool-v0", Some(matches)) => {
            let authorized_staker = keypair_of(matches, "authorized_staker").unwrap();
            let reserve_stake_address = pubkey_of(matches, "reserve_stake_address").unwrap();
            let min_reserve_stake_balance =
                sol_to_lamports(value_t_or_exit!(matches, "min_reserve_stake_balance", f64));
            let baseline_stake_amount = match value_t!(matches, "baseline_stake_amount", f64) {
                Ok(amt) => sol_to_lamports(amt),
                Err(_) => {
                    println!("Missing baseline_stake_amount");
                    process::exit(1)
                }
            };

            config.baseline_stake_amount_lamports = Some(baseline_stake_amount);

            Box::new(stake_pool_v0::new(
                &rpc_client,
                authorized_staker,
                baseline_stake_amount,
                reserve_stake_address,
                min_reserve_stake_balance,
            )?)
        }
        ("stake-pool", Some(matches)) => {
            let authorized_staker = keypair_of(matches, "authorized_staker").unwrap();
            let pool_address = pubkey_of(matches, "pool_address").unwrap();
            let min_reserve_stake_balance =
                sol_to_lamports(value_t_or_exit!(matches, "min_reserve_stake_balance", f64));
            let baseline_stake_amount = match value_t!(matches, "baseline_stake_amount", f64) {
                Ok(amt) => sol_to_lamports(amt),
                Err(_) => {
                    println!("Missing baseline_stake_amount");
                    process::exit(1)
                }
            };

            config.baseline_stake_amount_lamports = Some(baseline_stake_amount);

            Box::new(stake_pool::new(
                &rpc_client,
                authorized_staker,
                pool_address,
                baseline_stake_amount,
                min_reserve_stake_balance,
            )?)
        }
        ("noop-stake-pool", _) => Box::new(noop_stake_pool::new()),
        _ => unreachable!(),
    };

    Ok((config, rpc_client, stake_pool))
}

type ClassifyResult = (
    // quality
    ValidatorList,
    // poor
    ValidatorList,
    // classification reason
    HashMap<Pubkey, String>,
    // cluster_skip_rate
    usize,
    // too_many_poor_block_producers
    bool,
    // Pubkey => (blocks, slots)
    HashMap<Pubkey, (usize, usize)>,
);

fn classify_producers(
    first_slot_in_epoch: Slot,
    confirmed_blocks: HashSet<u64>,
    leader_schedule: HashMap<String, Vec<usize>>,
    config: &Config,
) -> BoxResult<ClassifyResult> {
    let mut poor_block_producers = HashSet::new();
    let mut quality_block_producers = HashSet::new();
    let mut blocks_and_slots = HashMap::new();
    let mut reason_msg = HashMap::new();

    let mut total_blocks = 0;
    let mut total_slots = 0;
    for (validator_identity, relative_slots) in leader_schedule {
        let mut validator_blocks = 0;
        let mut validator_slots = 0;
        for relative_slot in relative_slots {
            let slot = first_slot_in_epoch + relative_slot as Slot;
            total_slots += 1;
            validator_slots += 1;
            if confirmed_blocks.contains(&slot) {
                total_blocks += 1;
                validator_blocks += 1;
            }
        }
        if validator_slots > 0 {
            let validator_identity = Pubkey::from_str(&validator_identity)?;
            let e = blocks_and_slots.entry(validator_identity).or_insert((0, 0));
            e.0 += validator_blocks;
            e.1 += validator_slots;
        }
    }
    let cluster_average_skip_rate = 100 - total_blocks * 100 / total_slots;
    for (validator_identity, (blocks, slots)) in blocks_and_slots.clone() {
        let skip_rate: usize = 100 - (blocks * 100 / slots);

        let msg = format!(
            "{} blocks in {} slots, {:.2}% skip rate",
            blocks, slots, skip_rate
        );
        trace!("Validator {} produced {}", validator_identity, msg);
        reason_msg.insert(validator_identity, msg);

        if skip_rate.saturating_sub(config.quality_block_producer_percentage)
            > cluster_average_skip_rate
        {
            poor_block_producers.insert(validator_identity);
        } else {
            quality_block_producers.insert(validator_identity);
        }
    }

    let poor_block_producer_percentage = poor_block_producers.len() * 100
        / (quality_block_producers.len() + poor_block_producers.len());
    let too_many_poor_block_producers =
        poor_block_producer_percentage > config.max_poor_block_producer_percentage;

    info!("cluster_average_skip_rate: {}", cluster_average_skip_rate);
    info!("quality_block_producers: {}", quality_block_producers.len());
    trace!("quality_block_producers: {:?}", quality_block_producers);
    info!("poor_block_producers: {}", poor_block_producers.len());
    trace!("poor_block_producers: {:?}", poor_block_producers);
    info!(
        "poor_block_producer_percentage: {}% (too many poor producers={})",
        poor_block_producer_percentage, too_many_poor_block_producers,
    );

    Ok((
        quality_block_producers,
        poor_block_producers,
        reason_msg,
        cluster_average_skip_rate,
        too_many_poor_block_producers,
        blocks_and_slots,
    ))
}

fn classify_poor_voters(
    config: &Config,
    vote_account_info: &[VoteAccountInfo],
) -> (ValidatorList, usize, u64, u64, bool) {
    let avg_epoch_credits = vote_account_info
        .iter()
        .map(|vai| vai.epoch_credits)
        .sum::<u64>()
        / vote_account_info.len() as u64;

    let min_epoch_credits =
        avg_epoch_credits * (100 - config.min_epoch_credit_percentage_of_average as u64) / 100;

    let poor_voters = vote_account_info
        .iter()
        .filter_map(|vai| {
            if vai.epoch_credits < min_epoch_credits {
                Some(vai.identity)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let max_poor_voters = vote_account_info.len() * config.max_poor_voter_percentage / 100;
    let poor_voter_percentage = poor_voters.len() * 100 / vote_account_info.len();
    let too_many_poor_voters = poor_voters.len() > max_poor_voters;

    info!("Cluster average epoch credits: {}", avg_epoch_credits);
    info!("Minimum required epoch credits: {}", min_epoch_credits);
    info!("Poor voter: {}%", poor_voter_percentage);
    debug!(
        "poor_voters: {}, max poor_voters: {}",
        poor_voters.len(),
        max_poor_voters
    );
    trace!("poor_voters: {:?}", poor_voters);

    (
        poor_voters,
        poor_voter_percentage,
        min_epoch_credits,
        avg_epoch_credits,
        too_many_poor_voters,
    )
}

fn get_confirmed_blocks(
    rpc_client: &RpcClient,
    start_slot: Slot,
    end_slot: Slot,
) -> BoxResult<HashSet<Slot>> {
    info!(
        "loading slot history. slot range is [{},{}]",
        start_slot, end_slot
    );
    let slot_history_account = rpc_client
        .get_account_with_commitment(&sysvar::slot_history::id(), CommitmentConfig::finalized())?
        .value
        .unwrap();

    let slot_history: SlotHistory =
        from_account(&slot_history_account).ok_or("Failed to deserialize slot history")?;

    if start_slot >= slot_history.oldest() && end_slot <= slot_history.newest() {
        info!("slot range within the SlotHistory sysvar");
        Ok((start_slot..=end_slot)
            .filter(|slot| slot_history.check(*slot) == slot_history::Check::Found)
            .collect())
    } else {
        Err("slot range is not within the SlotHistory sysvar".into())
    }
}

/// Split validators into quality/poor lists based on their block production over the given `epoch`
fn classify_block_producers(
    rpc_client: &RpcClient,
    config: &Config,
    epoch: Epoch,
) -> BoxResult<ClassifyResult> {
    let epoch_schedule = rpc_client.get_epoch_schedule()?;
    let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch);
    let last_slot_in_epoch = epoch_schedule.get_last_slot_in_epoch(epoch);

    let confirmed_blocks =
        get_confirmed_blocks(rpc_client, first_slot_in_epoch, last_slot_in_epoch)?;

    let leader_schedule = rpc_client
        .get_leader_schedule_with_commitment(
            Some(first_slot_in_epoch),
            CommitmentConfig::finalized(),
        )?
        .unwrap();

    classify_producers(
        first_slot_in_epoch,
        confirmed_blocks,
        leader_schedule,
        config,
    )
}

// Look for self stake, where the stake withdraw authority matches the vote account withdraw
// authority
fn get_self_stake_by_vote_account(
    rpc_client: &RpcClient,
    epoch: Epoch,
    vote_account_info: &[VoteAccountInfo],
) -> BoxResult<HashMap<Pubkey, u64>> {
    let mut self_stake_by_vote_account = HashMap::new();

    info!("Building list of authorized voters...");

    let mut authorized_withdrawer = HashMap::new();
    for VoteAccountInfo { vote_address, .. } in vote_account_info {
        let vote_account = rpc_client.get_account(vote_address)?;

        if let Some(vote_state) = VoteState::from(&vote_account) {
            authorized_withdrawer.insert(vote_address, vote_state.authorized_withdrawer);
        }
    }

    info!("Fetching stake accounts...");
    let all_stake_accounts = rpc_client.get_program_accounts(&stake::program::id())?;

    let stake_history_account = rpc_client
        .get_account_with_commitment(&sysvar::stake_history::id(), CommitmentConfig::finalized())?
        .value
        .unwrap();

    let stake_history: StakeHistory =
        from_account(&stake_history_account).ok_or("Failed to deserialize stake history")?;

    for (_stake_pubkey, stake_account) in all_stake_accounts {
        if let Ok(StakeState::Stake(meta, stake)) = stake_account.state() {
            let vote_address = &stake.delegation.voter_pubkey;
            if let Some(vote_account_authorized_withdrawer) =
                authorized_withdrawer.get(vote_address)
            {
                if *vote_account_authorized_withdrawer == meta.authorized.withdrawer {
                    let effective_stake = stake
                        .delegation
                        .stake_activating_and_deactivating(epoch, Some(&stake_history))
                        .0;
                    if effective_stake > 0 {
                        *self_stake_by_vote_account.entry(*vote_address).or_default() +=
                            effective_stake;
                    }
                }
            }
        }
    }

    Ok(self_stake_by_vote_account)
}

// Returns HashMap<testnet_validator_identity, whether the validator met the min_testnet_participation criterion>
fn get_testnet_participation(
    config: &Config,
    testnet_epoch: &Epoch,
) -> BoxResult<Option<HashMap<Pubkey, bool>>> {
    if let Some((n, m)) = &config.min_testnet_participation {
        assert_eq!(config.cluster, Cluster::MainnetBeta);

        let db_testnet_path = &config.cluster_db_path_for(Cluster::Testnet);

        let mut validator_stake_count: HashMap<Pubkey, usize> = HashMap::new();
        let mut num_classified_epochs = 0;
        let mut epoch = *testnet_epoch;

        while num_classified_epochs < *m {
            if let Some(epoch_classification) =
                EpochClassification::load_if_validators_classified(epoch, db_testnet_path)?
            {
                if let Some(validator_classifications) = epoch_classification
                    .into_current()
                    .validator_classifications
                {
                    num_classified_epochs += 1;
                    for (_pubkey, validator_classification) in validator_classifications {
                        let identity = validator_classification.identity;
                        let count = *validator_stake_count.entry(identity).or_insert(0);
                        if validator_classification.stake_state != ValidatorStakeState::None {
                            validator_stake_count.insert(identity, count + 1);
                        }
                    }
                }
            }
            epoch -= 1;
        }

        let testnet_participation: HashMap<Pubkey, bool> = validator_stake_count
            .iter()
            .map(|(pubkey, c)| (*pubkey, c >= n))
            .collect();

        let num_poor_testnet_participants =
            testnet_participation.iter().filter(|(_, v)| !*v).count();

        let poor_testnet_particiant_percentage = if testnet_participation.is_empty() {
            100
        } else {
            num_poor_testnet_participants * 100 / testnet_participation.len()
        };

        info!(
            "Total testnet participation: {}",
            testnet_participation.len()
        );
        info!(
            "Poor testnet participants: {} ({}%)",
            num_poor_testnet_participants, poor_testnet_particiant_percentage
        );

        Ok(Some(testnet_participation))
    } else {
        Ok(None)
    }
}

fn classify(
    rpc_client: &RpcClient,
    config: &Config,
    epoch: Epoch,
    validator_list: &ValidatorList,
    identity_to_participant: &IdentityToParticipant,
    previous_epoch_validator_classifications: Option<&ValidatorClassificationByIdentity>,
    non_rejected_participants: HashMap<Pubkey, Participant>,
) -> BoxResult<EpochClassificationV1> {
    let last_epoch = epoch - 1;

    let testnet_rpc_client = RpcClient::new_with_timeout(
        DEFAULT_TESTNET_JSON_RPC_URL.into(), // TODO: should be configurable
        Duration::from_secs(180),
    );
    let testnet_epoch = testnet_rpc_client.get_epoch_info()?.epoch;
    info!(
        "Using testnet epoch {:?} as most recent epoch for testnet metrics",
        testnet_epoch
    );

    let testnet_participation: Option<HashMap<Pubkey, bool>> =
        match get_testnet_participation(config, &testnet_epoch)? {
            Some(tn_participation) => {
                // We have a map from testnet pubkey to whether testnet participation requirements were met. Convert to a map from
                // mainnet pubkeys to whether testnet requirements were met
                let mb_to_tn: HashMap<Pubkey, Pubkey> = non_rejected_participants
                    .iter()
                    .map(|(_, participant)| {
                        (participant.testnet_identity, participant.mainnet_identity)
                    })
                    .collect();

                Some(
                    tn_participation
                        .iter()
                        .filter_map(|(tn_pubkey, passed)| {
                            mb_to_tn
                                .get(tn_pubkey)
                                .map(|mb_pubkey| (*mb_pubkey, *passed))
                        })
                        .collect(),
                )
            }
            _ => None,
        };

    let data_centers = match data_center_info::get(config.cluster) {
        Ok(data_centers) => {
            // Sanity check the infrastructure stake percent data.  More than 35% indicates there's
            // probably a bug in the data source. Abort if so.
            let max_infrastucture_stake_percent = data_centers
                .info
                .iter()
                .map(|dci| dci.stake_percent.round() as usize)
                .max()
                .unwrap_or(100);

            info!(
                "Largest data center stake concentration: ~{}%",
                max_infrastucture_stake_percent
            );
            if max_infrastucture_stake_percent > 35 {
                return Err("Largest data center stake concentration is too high".into());
            }
            data_centers
        }
        Err(err) => {
            if config.max_infrastructure_concentration.is_some() {
                return Err(err);
            }
            warn!("infrastructure concentration skipped: {}", err);
            data_center_info::DataCenters::default()
        }
    };

    let (vote_account_info, total_active_stake) = get_vote_account_info(rpc_client, last_epoch)?;

    let self_stake_by_vote_account =
        get_self_stake_by_vote_account(rpc_client, epoch, &vote_account_info)?;

    let mut cluster_nodes_with_old_version: HashMap<String, _> = HashMap::new();

    let release_versions: HashMap<Pubkey, semver::Version> = rpc_client
        .get_cluster_nodes()?
        .into_iter()
        .filter_map(|rpc_contact_info| {
            if let Ok(identity) = Pubkey::from_str(&rpc_contact_info.pubkey) {
                if validator_list.contains(&identity) {
                    if let Some(ref version) = rpc_contact_info.version {
                        if let Ok(semver) = semver::Version::parse(version) {
                            if let Some(min_release_version) = &config.min_release_version {
                                if semver < *min_release_version {
                                    cluster_nodes_with_old_version
                                        .insert(identity.to_string(), semver.clone());
                                }
                            }
                            return Some((identity, semver));
                        }
                    }
                }
            }
            None
        })
        .collect();

    let min_release_version = match &config.min_release_version {
        Some(v) => v.to_string(),
        None => "".to_string(),
    };

    if let Some(ref min_release_version) = config.min_release_version {
        info!(
            "Validators running a release older than {}: {:?}",
            min_release_version, cluster_nodes_with_old_version,
        );
    }

    let (
        quality_block_producers,
        poor_block_producers,
        block_producer_classification_reason,
        cluster_average_skip_rate,
        too_many_poor_block_producers,
        blocks_and_slots,
    ) = classify_block_producers(rpc_client, config, last_epoch)?;

    let not_in_leader_schedule: ValidatorList = validator_list
        .difference(
            &quality_block_producers
                .intersection(&poor_block_producers)
                .cloned()
                .collect(),
        )
        .cloned()
        .collect();

    let too_many_old_validators = cluster_nodes_with_old_version.len()
        > (poor_block_producers.len() + quality_block_producers.len())
            * config.max_old_release_version_percentage
            / 100;

    let (
        poor_voters,
        poor_voter_percentage,
        min_epoch_credits,
        avg_epoch_credits,
        too_many_poor_voters,
    ) = classify_poor_voters(config, &vote_account_info);

    let mut notes = vec![
        format!(
            "Minimum vote credits required for epoch {}: {} (cluster average: {}, grace: {}%)",
            last_epoch,
            min_epoch_credits,
            avg_epoch_credits,
            config.min_epoch_credit_percentage_of_average,
        ),
        format!(
            "Maximum allowed skip rate for epoch {}: {:.2}% (cluster average: {:.2}%, grace: {}%)",
            last_epoch,
            cluster_average_skip_rate + config.quality_block_producer_percentage,
            cluster_average_skip_rate,
            config.quality_block_producer_percentage,
        ),
        format!("Solana release {} or greater required", min_release_version),
        format!("Maximum commission: {}%", config.max_commission),
        format!(
            "Minimum required self stake: {}",
            Sol(config.min_self_stake_lamports)
        ),
        format!(
            "Maximum active stake allowed: {}",
            Sol(config.max_active_stake_lamports)
        ),
    ];
    if let Some(max_infrastructure_concentration) = config.max_infrastructure_concentration {
        notes.push(format!(
            "Maximum infrastructure concentration: {:0}%",
            max_infrastructure_concentration
        ));
    }
    if let Some((n, m)) = &config.min_testnet_participation {
        notes.push(format!(
            "Participants must maintain Baseline or Bonus stake level for {} of the last {} Testnet epochs",
            n, m
        ));
    }

    if cluster_average_skip_rate > config.bad_cluster_average_skip_rate {
        notes.push("Cluster average skip rate is poor".to_string());
    }
    if too_many_poor_voters {
        notes.push(format!(
            "Too many validators classified as poor voters for epoch {}: {}% (limit: {}%)",
            last_epoch, poor_voter_percentage, config.max_poor_voter_percentage
        ));
    }
    if too_many_old_validators {
        notes.push(format!(
            "Over {}% of validators classified as running an older release",
            config.max_old_release_version_percentage
        ));
    }
    if too_many_poor_block_producers {
        notes.push(format!(
            "Over {}% of validators classified as poor block producers in epoch {}",
            config.max_poor_block_producer_percentage, last_epoch,
        ));
    }

    let validator_classifications = if too_many_poor_voters
        || too_many_old_validators
        || too_many_poor_block_producers
    {
        notes.push("Stake adjustments skipped this epoch".to_string());
        None
    } else {
        let mut validator_classifications = HashMap::new();

        // Get all commission changes, so we can figure out what the validator's commission was
        let validators_app_client = validators_app::Client::new_with_cluster(config.cluster)?;
        // We need records from last_epoch+1. Epochs are approximately 3 days long, so 5 days should be more than enough
        let five_days_ago = Utc::now() - ChronoDuration::days(5);
        let all_commission_changes =
            validators_app_client.get_all_commision_changes_since(five_days_ago)?;

        let performance_metrics_for_this_epoch: Option<HashMap<Pubkey, (bool, String)>> =
            if let (Some(performance_db_url), Some(performance_db_token)) =
                (&config.performance_db_url, &config.performance_db_token)
            {
                let reported_performance_metrics = get_reported_performance_metrics(
                    performance_db_url,
                    performance_db_token,
                    &config.cluster,
                    rpc_client,
                    &(epoch - 1),
                    &non_rejected_participants,
                );

                if let Ok(metrics) = reported_performance_metrics {
                    Some(metrics)
                } else {
                    info!(
                        "Could not get reported performance metrics: {:?}",
                        reported_performance_metrics.err().unwrap()
                    );
                    None
                }
            } else {
                None
            };

        let mut reporting_counts: HashMap<Pubkey, HashMap<Epoch, bool>> = HashMap::new();

        let mut number_sampled_epochs: u64 = 0;
        if let Some(metrics) = performance_metrics_for_this_epoch.as_ref() {
            metrics.iter().for_each(|(pk, (passed, _b))| {
                reporting_counts.insert(*pk, HashMap::from([(epoch - 1, *passed)]));
            });
            number_sampled_epochs = 1;
        } else {
            notes.push("Could not get reported performance metrics".to_string());
        };

        let mut number_loops = 0;
        let mut reporting_epoch = epoch - 2;
        while number_sampled_epochs < NUM_SAMPLED_REPORTING_EPOCHS as u64 && reporting_epoch > 0 {
            // Fetch from wiki repo
            if let Some(epoch_classification) = EpochClassification::load_if_validators_classified(
                reporting_epoch + 1,
                &config.cluster_db_path(),
            )? {
                // Whether any "passed" records are found. If none are found, don't use the epoch.
                let mut some_validators_reported = false;

                let mut this_epoch_reporting_counts: HashMap<Pubkey, bool> = HashMap::new();

                epoch_classification
                    .into_current()
                    .validator_classifications
                    .unwrap()
                    .iter()
                    .for_each(|(pk, classification)| {
                        if let Some((passed, _reason)) =
                            classification.self_reported_metrics.as_ref()
                        {
                            if *passed {
                                some_validators_reported = true;
                            }
                            this_epoch_reporting_counts.insert(*pk, *passed);
                        }
                    });

                // if some validators reported, we use the epoch to determine if validators reported in n/10 of the last epochs
                if some_validators_reported {
                    this_epoch_reporting_counts.iter().for_each(|(pk, passed)| {
                        let entry = reporting_counts.entry(*pk).or_insert_with(HashMap::new);
                        entry.insert(reporting_epoch, *passed);
                    });
                    number_sampled_epochs += 1;
                }
            }
            number_loops += 1;
            reporting_epoch = epoch - 2 - number_loops;
        }

        // if mainnet, get list of validators that have been poor reporters on testnet
        let poor_testnet_reporters: Option<Vec<(Pubkey, String)>> = if config.cluster == MainnetBeta
        {
            Some(
                EpochClassification::load_previous(
                    testnet_epoch,
                    &config.cluster_db_path_for(Testnet),
                )?
                .map(|(epoch, epoch_classification)| {
                    let note = format!("Using epoch {:?} for testnet classifications", epoch - 1);
                    notes.push(note.to_string());
                    info!("{}", note);

                    epoch_classification
                })
                .unwrap()
                .into_current()
                .validator_classifications
                .unwrap()
                .iter()
                .filter_map(|(pk, vc)| {
                    vc.self_reported_metrics_summary
                        .as_ref()
                        .and_then(|(pass, explanation)| {
                            if *pass {
                                None
                            } else {
                                // get corresponding mainnet validator pk
                                match non_rejected_participants
                                    .iter()
                                    .find(|(_pk, participant)| participant.testnet_identity == *pk)
                                {
                                    Some((_, participant)) => {
                                        let failure_explanation =
                                            format!("Poor reporting on testnet: {:}", explanation);
                                        Some((participant.mainnet_identity, failure_explanation))
                                    }
                                    None => None,
                                }
                            }
                        })
                })
                .collect(),
            )
        } else {
            None
        };

        // Map of poor reporters
        let mut poor_reporters_last_10_epochs: HashMap<Pubkey, String> = HashMap::new();

        let performance_reporting: HashMap<Pubkey, (bool, String)> = reporting_counts
            .iter()
            .map(|(pk, reports)| {
                let mut failed_epochs: Vec<&Epoch> = reports
                    .iter()
                    .filter_map(|(epoch, passed)| if !passed { Some(epoch) } else { None })
                    .collect::<Vec<&Epoch>>();
                failed_epochs.sort();

                let num_passed = reports.len() - failed_epochs.len();

                let percent_passed = num_passed as f32 / reports.len() as f32;

                if let Some(reason) = poor_testnet_reporters.as_ref().and_then(|ptr| {
                    ptr.iter()
                        .find(|(failed_pk, _r)| pk == failed_pk)
                        .map(|(_pk, reason)| reason.clone())
                }) {
                    poor_reporters_last_10_epochs.insert(*pk, reason.clone());

                    (*pk, (false, reason))
                } else if percent_passed >= SUCCESS_MIN_PERCENT {
                    let pass_reason = format!(
                        "Reported correctly in {:?}/{:?} epochs",
                        num_passed,
                        reports.len()
                    );
                    (*pk, (true, pass_reason))
                } else {
                    let failure_reason = format!(
                        "Only reported correctly in {:?}/{:?} epochs. Non-reporting epochs: {:?}",
                        num_passed,
                        reports.len(),
                        failed_epochs.iter().map(|v| v.to_string()).join(", ")
                    );
                    poor_reporters_last_10_epochs.insert(*pk, failure_reason.clone());
                    (*pk, (false, failure_reason))
                }
            })
            .collect();

        if config.require_performance_metrics_reporting && poor_reporters_last_10_epochs.is_empty()
        {
            notes.push("Could not fetch reporting metrics (or everyone reported); not applying the require-performance-metrics-reporting requirement".to_string());
        }

        for VoteAccountInfo {
            identity,
            vote_address,
            commission,
            active_stake,
            epoch_credits,
        } in vote_account_info
        {
            if !validator_list.contains(&identity) {
                continue;
            }

            let participant = identity_to_participant.get(&identity).cloned();

            let current_data_center = data_centers
                .by_identity
                .get(&identity)
                .cloned()
                .unwrap_or_default();

            let previous_classification =
                previous_epoch_validator_classifications.and_then(|p| p.get(&identity));

            let commission_at_end_of_epoch = calculate_commission_at_end_of_epoch(
                epoch,
                commission,
                all_commission_changes.get(&identity),
            );
            let num_epochs_max_commission_exceeded = previous_classification
                .and_then(|vc| vc.num_epochs_max_commission_exceeded)
                .unwrap_or(0)
                + (if commission_at_end_of_epoch > config.max_commission {
                    1
                } else {
                    0
                });

            // if the commission was below max_commission at the beginning of the last epoch, and is
            // above max_commission at the beginning of the current epoch
            let commission_increased_above_max = commission_at_end_of_epoch > config.max_commission
                && previous_classification
                    .and_then(|pc| pc.commission)
                    .map_or(false, |commission| commission <= config.max_commission);

            let num_epochs_commission_increased_above_max = previous_classification
                .and_then(|vc| vc.num_epochs_commission_increased_above_max)
                .unwrap_or(0)
                + (if commission_increased_above_max { 1 } else { 0 });

            let mut previous_data_center_residency = previous_classification
                .and_then(|vc| vc.data_center_residency.clone())
                .unwrap_or_default();

            let previous_stake_state = previous_classification
                .map(|vc| vc.stake_state)
                .unwrap_or_default();

            let self_stake = self_stake_by_vote_account
                .get(&vote_address)
                .cloned()
                .unwrap_or_default();

            let block_producer_classification_reason_msg = block_producer_classification_reason
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let vote_credits_msg =
                format!("{} credits earned in epoch {}", epoch_credits, last_epoch);

            let mut validator_notes = vec![];

            let new_validator = !previous_data_center_residency.contains_key(&current_data_center);

            let insufficent_self_stake_msg =
                format!("Insufficient self stake: {}", Sol(self_stake));
            if !config.enforce_min_self_stake && self_stake < config.min_self_stake_lamports {
                validator_notes.push(insufficent_self_stake_msg.clone());
            }

            let insufficent_testnet_participation: Option<String> = testnet_participation
                .as_ref()
                .and_then(|testnet_participation| {
                    testnet_participation.get(&identity).and_then(|passed| {
                        if !passed {
                            let note = "Insufficient testnet participation".to_string();
                            if config.enforce_testnet_participation {
                                return Some(note);
                            } else {
                                validator_notes.push(note);
                            }
                        }
                        None
                    })
                });

            let (stake_state, reason) = if num_epochs_commission_increased_above_max > 1 {
                (
                    ValidatorStakeState::None,
                    format!(
                        "Commission increased above max_commission for {} epochs. Permanently destaked.",
                        num_epochs_commission_increased_above_max
                    ),
                )
            } else if config
                .blocklist_datacenter_asns
                .as_ref()
                .map_or(false, |asns| asns.contains(&current_data_center.asn))
            {
                (
                    ValidatorStakeState::None,
                    format!("Validator in blocked data center: {}", current_data_center),
                )
            } else if config.require_performance_metrics_reporting
                && !poor_reporters_last_10_epochs.is_empty() // if poor_reporters empty, either everyone is a good reporter, or we did not get any reporting metrics so need to skip this requirement
                && poor_reporters_last_10_epochs.contains_key(&identity)
            {
                (
                    ValidatorStakeState::None,
                    poor_reporters_last_10_epochs
                        .get(&identity)
                        .unwrap()
                        .clone(),
                )
            } else if config.enforce_min_self_stake
                && self_stake < config.min_self_stake_lamports
                && !config.min_self_stake_exceptions.contains(&identity)
            {
                let insufficent_self_stake_msg =
                    format!("Insufficient self stake: {}", Sol(self_stake));
                validator_notes.push(insufficent_self_stake_msg.clone());
                (ValidatorStakeState::None, insufficent_self_stake_msg)
            } else if active_stake > config.max_active_stake_lamports {
                (
                    ValidatorStakeState::None,
                    format!("Active stake is too high: {}", Sol(active_stake)),
                )
            } else if commission_at_end_of_epoch > config.max_commission {
                (
                    ValidatorStakeState::None,
                    format!(
                        "Commission is too high: {}% commission",
                        commission_at_end_of_epoch
                    ),
                )
            } else if let Some(insufficent_testnet_participation) =
                insufficent_testnet_participation
            {
                (ValidatorStakeState::None, insufficent_testnet_participation)
            } else if poor_voters.contains(&identity) {
                (
                    ValidatorStakeState::None,
                    format!("Insufficient vote credits: {}", vote_credits_msg),
                )
            } else if cluster_nodes_with_old_version.contains_key(&identity.to_string()) {
                (
                    ValidatorStakeState::None,
                    format!(
                        "Outdated Solana release: {}",
                        cluster_nodes_with_old_version
                            .get(&identity.to_string())
                            .unwrap()
                    ),
                )
            } else if quality_block_producers.contains(&identity) {
                (
                    ValidatorStakeState::Bonus,
                    format!(
                        "Good block production during epoch {}: {}",
                        last_epoch, block_producer_classification_reason_msg
                    ),
                )
            } else if poor_block_producers.contains(&identity) {
                (
                    ValidatorStakeState::Baseline,
                    format!(
                        "Poor block production during epoch {}: {}",
                        last_epoch, block_producer_classification_reason_msg
                    ),
                )
            } else {
                assert!(!poor_voters.contains(&identity));
                assert!(not_in_leader_schedule.contains(&identity));
                (
                    // If the validator is not in the leader schedule but was Bonus previously,
                    // maintain Bonus.
                    //
                    // Destaking due to delinquency will not be reflected in the leader schedule
                    // until 2 epochs later, which point the validator may have recovered and
                    // there's no need to punish the validator further by reducing it to the
                    // Baseline level.
                    if previous_stake_state == ValidatorStakeState::Bonus {
                        ValidatorStakeState::Bonus
                    } else {
                        ValidatorStakeState::Baseline
                    },
                    format!("No leader slots; {}", vote_credits_msg),
                )
            };

            // Data center seniority increases with Bonus stake and decreases
            // otherwise
            previous_data_center_residency
                .entry(current_data_center.clone())
                .or_default();

            let data_center_residency = previous_data_center_residency
                .into_iter()
                .map(|(data_center, seniority)| {
                    if data_center == current_data_center
                        && stake_state == ValidatorStakeState::Bonus
                    {
                        (data_center, seniority.saturating_add(1))
                    } else {
                        (data_center, seniority.saturating_sub(1))
                    }
                })
                .filter(|(_, i)| *i > 0)
                .collect::<HashMap<_, _>>();

            debug!(
                "\nidentity: {} ({:?})\n\
                    - vote address: {}\n\
                    - stake state: {:?}, data center: {:?} (seniority: {}), self stake: {}, commission: {}\n\
                    - {}",
                identity,
                participant,
                vote_address,
                stake_state,
                current_data_center,
                data_center_residency
                    .get(&current_data_center)
                    .cloned()
                    .unwrap_or_default(),
                Sol(self_stake),
                commission_at_end_of_epoch,
                reason
            );

            let (blocks, slots) = match blocks_and_slots.get(&identity) {
                Some((b, s)) => (Some(*b), Some(*s)),
                None => (None, None),
            };

            validator_classifications.insert(
                identity,
                ValidatorClassification {
                    identity,
                    vote_address,
                    stake_state,
                    stake_states: None, // to be added after data center concentration adjustments have been made
                    stake_action: None,
                    stake_state_reason: reason,
                    notes: validator_notes,
                    data_center_residency: Some(data_center_residency),
                    current_data_center: Some(current_data_center.clone()),
                    participant,
                    prioritize_funding_in_next_epoch: None,
                    blocks,
                    slots,
                    vote_credits: Some(epoch_credits),
                    commission: Some(commission_at_end_of_epoch),
                    self_stake: Some(self_stake),
                    new_data_center_residency: Some(new_validator),
                    release_version: release_versions.get(&identity).cloned(),
                    num_epochs_max_commission_exceeded: Some(num_epochs_max_commission_exceeded),
                    num_epochs_commission_increased_above_max: Some(
                        num_epochs_commission_increased_above_max,
                    ),
                    self_reported_metrics: performance_metrics_for_this_epoch.as_ref().and_then(
                        |metrics| metrics.get(&identity).and_then(|v| Option::from(v.clone())),
                    ),
                    self_reported_metrics_summary: performance_reporting
                        .get(&identity)
                        .and_then(|v| Option::from(v.clone())),
                },
            );
        }
        notes.push(format!(
            "{} validators processed",
            validator_classifications.len()
        ));

        // Calculating who gets destaked when the InfrastructureConcentrationAffects is DestakeOverflow requires that
        // we have the data center seniority scores of _all_ validators calculated first, so we go
        // back and adjust the stake states for the infrastructure concentration effects here.
        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            config,
        );

        // Now update the stake_states array with the state for the current epoch
        validator_classifications.iter_mut().for_each(|(k, vc)| {
            let previous_classification =
                previous_epoch_validator_classifications.and_then(|p| p.get(k));

            let mut stake_states = previous_classification
                .and_then(|vc| vc.stake_states.clone())
                .unwrap_or_default();
            stake_states.insert(0, (vc.stake_state, vc.stake_state_reason.clone()));
            vc.stake_states = Some(stake_states);
        });

        Some(validator_classifications)
    };
    notes.push(format!("Active stake: {}", Sol(total_active_stake)));

    let epoch_config = EpochConfig {
        require_classification: Some(config.require_classification),
        quality_block_producer_percentage: Some(config.quality_block_producer_percentage),
        max_poor_block_producer_percentage: Some(config.max_poor_block_producer_percentage),
        max_commission: Some(config.max_commission),
        min_release_version: config.min_release_version.clone(),
        max_old_release_version_percentage: Some(config.max_old_release_version_percentage),
        max_poor_voter_percentage: Some(config.max_poor_block_producer_percentage),
        max_infrastructure_concentration: config.max_infrastructure_concentration,
        infrastructure_concentration_affects: Some(
            config.infrastructure_concentration_affects.clone(),
        ),
        bad_cluster_average_skip_rate: Some(config.bad_cluster_average_skip_rate),
        min_epoch_credit_percentage_of_average: Some(config.min_epoch_credit_percentage_of_average),
        min_self_stake_lamports: Some(config.min_self_stake_lamports),
        max_active_stake_lamports: Some(config.max_active_stake_lamports),
        enforce_min_self_stake: Some(config.enforce_min_self_stake),
        enforce_testnet_participation: Some(config.enforce_testnet_participation),
        min_testnet_participation: config.min_testnet_participation,
        baseline_stake_amount_lamports: config.baseline_stake_amount_lamports,
        require_performance_metrics_reporting: Some(config.require_performance_metrics_reporting),
    };

    let epoch_stats = EpochStats {
        bonus_stake_amount: 0,
        min_epoch_credits,
        avg_epoch_credits,
        max_skip_rate: (cluster_average_skip_rate + config.quality_block_producer_percentage),
        cluster_average_skip_rate,
        total_active_stake,
    };

    Ok(EpochClassificationV1 {
        data_center_info: data_centers.info,
        validator_classifications,
        notes,
        config: Some(epoch_config),
        stats: Some(epoch_stats),
    })
}

// Adjusts the validator classifications based on the infrastructure concentration affect
fn adjust_validator_classification_for_data_center_concentration(
    validator_classifications: &mut HashMap<Pubkey, ValidatorClassification>,
    data_centers: &DataCenters,
    config: &Config,
) {
    let infrastructure_concentration_too_high: Vec<&data_center_info::DataCenterInfo> =
        match config.max_infrastructure_concentration {
            Some(max_infrastructure_concentration) => data_centers
                .info
                .iter()
                .filter(|dci| dci.stake_percent > max_infrastructure_concentration)
                .collect(),
            _ => {
                vec![]
            }
        };

    debug!(
        "{} data centers over max_infrastructure_concentration",
        infrastructure_concentration_too_high.len()
    );

    match &config.infrastructure_concentration_affects {
        InfrastructureConcentrationAffects::WarnAll => {
            for dci in infrastructure_concentration_too_high {
                for validator_id in &dci.validators {
                    if let Some(vc) = validator_classifications.get_mut(validator_id) {
                        warn_validator_for_infrastructure_concentration(vc, dci);
                    }
                }
            }
        }
        InfrastructureConcentrationAffects::DestakeListed(list) => {
            for dci in infrastructure_concentration_too_high {
                for validator_id in &dci.validators {
                    if let Some(vc) = validator_classifications.get_mut(validator_id) {
                        if list.contains(validator_id) {
                            destake_validator_for_infrastructure_concentration(vc, dci);
                        } else {
                            warn_validator_for_infrastructure_concentration(vc, dci);
                        }
                    }
                }
            }
        }
        InfrastructureConcentrationAffects::DestakeAll => {
            for dci in infrastructure_concentration_too_high {
                for validator_id in &dci.validators {
                    if let Some(vc) = validator_classifications.get_mut(validator_id) {
                        destake_validator_for_infrastructure_concentration(vc, dci);
                    }
                }
            }
        }
        InfrastructureConcentrationAffects::DestakeNew => {
            for dci in infrastructure_concentration_too_high {
                for validator_id in &dci.validators {
                    if let Some(vc) = validator_classifications.get_mut(validator_id) {
                        if vc.new_data_center_residency.unwrap_or(false) {
                            destake_validator_for_infrastructure_concentration(vc, dci);
                        }
                    }
                }
            }
        }
        InfrastructureConcentrationAffects::DestakeOverflow => {
            debug!("Processing InfrastructureConcentrationAffects::DestakeOverflow");
            infrastructure_concentration_too_high.iter().for_each(|&data_center_info| {
                // now order by seniority
                let validators_by_seniority: Vec<Pubkey> = validator_classifications.iter()
                    .filter_map(|(_k, vc)| {
                        if let Some(ref current_data_center) = vc.current_data_center {
                            if current_data_center == &data_center_info.id {
                                vc.data_center_residency.as_ref().map(|dcr| (vc.identity, dcr.get(current_data_center)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }).sorted_by(|(_ac, a), (_bc, b)| {
                    a.cmp(b)
                }).map(|(c, _s)| c)
                    .collect();

                let validators_stake = data_center_info.validators_stake.clone().unwrap_or_default();

                // Figure out total stake from the data center's stake_percent and stake; TODO figure this out outside the loop
                let total_stake = 100f64 * (data_center_info.stake as f64) / data_center_info.stake_percent;
                // Maximum amount of stake a data center can have without being over max_infrastructure_concentration
                let max_stake = config.max_infrastructure_concentration.unwrap() * total_stake / 100f64;
                // We will keep destaking validators and removing their stake from this value until it is under max_stake
                let mut data_center_stake = data_center_info.stake as f64;

                // destake validators and remove their stake from the total until the sum is below the threshold
                for validator_identity in validators_by_seniority {
                    if let Some(validator_classification) = validator_classifications.get_mut(&validator_identity) {
                        if validator_classification.stake_state != ValidatorStakeState::None {
                            debug!("Destake {} for being junior in a high-concentration data center", validator_classification.identity);
                            destake_validator_for_infrastructure_concentration(validator_classification, data_center_info);
                        }
                        data_center_stake -= *validators_stake.get(&validator_classification.identity).unwrap_or(&(0)) as f64;
                    };

                    if data_center_stake < max_stake {
                        break;
                    }
                };
            });
        }
    };
}

// Change ValidatorClassification.stake_state to None and adjust for violation of the infrastructure_concentration constraint
fn destake_validator_for_infrastructure_concentration(
    validator_classification: &mut ValidatorClassification,
    data_center_info: &DataCenterInfo,
) {
    if validator_classification.stake_state == ValidatorStakeState::Bonus {
        // If the validator was to receive Bonus, it received a , +1 seniority score bump.
        // Validators without Bonus (Baseline or None) recieved a -1 seniority score penalty.
        // So subtract 2 from the Validator's seinority score if it was slated to receive Bonus
        // but is getting destaked for being in a over-saturated data center.
        let dcr = validator_classification
            .data_center_residency
            .clone()
            .unwrap();
        let score = dcr.get(&data_center_info.id.clone()).unwrap_or(&1);
        validator_classification
            .data_center_residency
            .as_mut()
            .unwrap()
            .insert(data_center_info.id.clone(), score.saturating_sub(2));
    }

    validator_classification.stake_state = ValidatorStakeState::None;

    validator_classification.stake_state_reason = format!(
        "infrastructure concentration {:.1}% is too high; find a new data center",
        data_center_info.stake_percent
    );
}

// Change ValidatorClassification.stake_state to warn about violation of the infrastructure_concentration constraint
fn warn_validator_for_infrastructure_concentration(
    validator_classification: &mut ValidatorClassification,
    data_center_info: &DataCenterInfo,
) {
    validator_classification.notes.push(format!(
        "infrastructure concentration {:.1}% is too high; consider finding a new data center",
        data_center_info.stake_percent
    ));
}

fn main() -> BoxResult<()> {
    solana_logger::setup_with_default("solana=info");

    let (config, rpc_client, mut stake_pool) = get_config()?;

    info!("Loading participants...");
    let all_participants = get_participants_with_state(
        &RpcClient::new(config.participant_json_rpc_url.clone()),
        None,
    )?;

    let (approved_participants, non_rejected_participants) = all_participants.iter().fold(
        (HashMap::new(), HashMap::new()),
        |(mut approved_validators, mut not_rejected_validators), (pubkey, participant)| {
            if participant.state == ParticipantState::Approved {
                approved_validators.insert(*pubkey, participant.clone());
            };
            if participant.state != ParticipantState::Rejected {
                not_rejected_validators.insert(*pubkey, participant.clone());
            };
            (approved_validators, not_rejected_validators)
        },
    );

    debug!("{:?} approved participants", approved_participants.len());
    debug!(
        "{:?} non-rejected participants",
        non_rejected_participants.len()
    );

    let (mainnet_identity_to_participant, testnet_identity_to_participant): (
        IdentityToParticipant,
        IdentityToParticipant,
    ) = approved_participants
        .iter()
        .map(
            |(
                participant_pk,
                Participant {
                    mainnet_identity,
                    testnet_identity,
                    ..
                },
            )| {
                (
                    (*mainnet_identity, *participant_pk),
                    (*testnet_identity, *participant_pk),
                )
            },
        )
        .unzip();

    info!("{} participants loaded", approved_participants.len());
    assert!(approved_participants.len() > 450); // Hard coded sanity check...

    let (validator_list, identity_to_participant): (ValidatorList, HashMap<Pubkey, Pubkey>) =
        match config.cluster {
            Cluster::MainnetBeta => (
                mainnet_identity_to_participant.keys().cloned().collect(),
                mainnet_identity_to_participant,
            ),
            Cluster::Testnet => {
                let approved_for_validator_list = validator_list::testnet_validators();

                (
                    non_rejected_participants
                        .iter()
                        .map(|(_k, v)| v.testnet_identity)
                        .filter(|pk| approved_for_validator_list.contains(pk))
                        .collect(),
                    testnet_identity_to_participant,
                )
            }
        };

    let notifier = if config.dry_run {
        Notifier::new("DRYRUN")
    } else {
        Notifier::default()
    };

    let epoch = rpc_client.get_epoch_info()?.epoch;
    info!("Current epoch: {:?}", epoch);
    if epoch == 0 {
        return Ok(());
    }

    info!("Data directory: {}", config.cluster_db_path().display());

    let previous_epoch_classification =
        EpochClassification::load_previous(epoch, &config.cluster_db_path())?
            .map(|p| p.1)
            .unwrap_or_default()
            .into_current();

    let (mut epoch_classification, first_time, post_notifications) =
        if EpochClassification::exists(epoch, &config.cluster_db_path()) {
            info!("Classification for epoch {} already exists", epoch);
            (
                EpochClassification::load(epoch, &config.cluster_db_path())?.into_current(),
                false,
                config.require_classification,
            )
        } else {
            if config.require_classification {
                return Err(format!("Classification for epoch {} does not exist", epoch).into());
            }
            (
                classify(
                    &rpc_client,
                    &config,
                    epoch,
                    &validator_list,
                    &identity_to_participant,
                    previous_epoch_classification
                        .validator_classifications
                        .as_ref(),
                    non_rejected_participants,
                )?,
                true,
                true,
            )
        };

    let mut notifications = epoch_classification.notes.clone();

    if let Some(ref mut validator_classifications) = epoch_classification.validator_classifications
    {
        let previous_validator_classifications = previous_epoch_classification
            .validator_classifications
            .unwrap_or_default();

        let mut min_stake_node_count = 0;
        let mut bonus_stake_node_count = 0;
        let mut baseline_stake_node_count = 0;
        let mut validator_stake_change_notes = vec![];
        let mut validator_notes = vec![];
        let desired_validator_stake: Vec<_> = validator_classifications
            .values()
            .map(|vc| {
                validator_notes.extend(
                    vc.notes
                        .iter()
                        .map(|note| format!("Note: {}: {}", vc.identity, note)),
                );

                let stake_state_changed = match previous_validator_classifications
                    .get(&vc.identity)
                    .map(|prev_vc| prev_vc.stake_state)
                {
                    Some(previous_stake_state) => previous_stake_state != vc.stake_state,
                    None => true,
                };

                if stake_state_changed {
                    validator_stake_change_notes.push(format!(
                        "* {:?} stake: {}: {}",
                        vc.stake_state, vc.identity, vc.stake_state_reason
                    ));
                }

                match vc.stake_state {
                    ValidatorStakeState::None => min_stake_node_count += 1,
                    ValidatorStakeState::Bonus => bonus_stake_node_count += 1,
                    ValidatorStakeState::Baseline => baseline_stake_node_count += 1,
                }

                ValidatorStake {
                    identity: vc.identity,
                    vote_address: vc.vote_address,
                    stake_state: vc.stake_state,
                    priority: previous_validator_classifications
                        .get(&vc.identity)
                        .map(|prev_vc| prev_vc.prioritize_funding_in_next_epoch)
                        .unwrap_or_default()
                        .unwrap_or_default(),
                }
            })
            .collect();

        // if true, we are doing a preliminary dry run
        let pre_run_dry_run = config.require_dry_run_to_distribute_stake
            && !DryRunStats::exists(epoch, &config.cluster_db_path());

        let (stake_pool_notes, validator_stake_actions, unfunded_validators, bonus_stake_amount) =
            stake_pool.apply(
                rpc_client,
                &config.websocket_url,
                pre_run_dry_run || config.dry_run,
                &desired_validator_stake,
            )?;

        if first_time && !pre_run_dry_run {
            let slack_message = format!(
                "Stake bot LIVE run for {:?}/{:?}\n",
                config.cluster,
                epoch - 1
            ) + &stake_pool_notes.join("\n");

            if let Err(e) = send_slack_channel_message(&slack_message) {
                info!("Could not send slack message: {:?}", e)
            };
        }

        if pre_run_dry_run {
            let dry_run_stats = DryRunStats {
                none_count: min_stake_node_count,
                baseline_count: baseline_stake_node_count,
                bonus_count: bonus_stake_node_count,
            };
            dry_run_stats.save(epoch, &config.cluster_db_path())?;

            info!("require_dry_run_to_distribute_stake is set; this was a dry run and stake was not distributed. The next time the bot is run for this cluster, stake _will_ be distributed.");

            let slack_message = format!(
                "Stake bot DRY run estimates for {:?}/{:?}\n",
                config.cluster,
                epoch - 1
            ) + &stake_pool_notes.join("\n");

            if let Err(e) = send_slack_channel_message(&slack_message) {
                info!("Could not send slack message: {:?}", e)
            };

            return Ok(());
        }

        notifications.extend(stake_pool_notes.clone());
        epoch_classification.notes.extend(stake_pool_notes);

        for identity in unfunded_validators {
            validator_classifications
                .entry(identity)
                .and_modify(|e| e.prioritize_funding_in_next_epoch = Some(true));
        }

        for (identity, stake_action) in validator_stake_actions {
            validator_classifications
                .entry(identity)
                .and_modify(|e| e.stake_action = Some(stake_action));
        }

        validator_notes.sort();
        notifications.extend(validator_notes);

        validator_stake_change_notes.sort();
        notifications.extend(validator_stake_change_notes);

        if let Some(ref mut stats) = epoch_classification.stats {
            stats.bonus_stake_amount = bonus_stake_amount;
        }
    }

    match (first_time, config.epoch_classification) {
        (true, OutputMode::First) | (_, OutputMode::Yes) => {
            EpochClassification::new(epoch_classification)
                .save(epoch, &config.cluster_db_path())?;
        }
        _ => {}
    }
    match (first_time, config.csv_output_mode) {
        (true, OutputMode::First) | (_, OutputMode::Yes) => {
            generate_csv(epoch, &config)?;
        }
        _ => {}
    }

    for notification in notifications {
        info!("notification: {}", notification);
        // Only notify the user if this is the first run for this epoch
        if first_time && post_notifications {
            notifier.send(&notification);
        }
    }

    Ok(())
}

fn generate_csv(epoch: Epoch, config: &Config) -> BoxResult<()> {
    info!("generate_csv()");
    let mut list = vec![(
        epoch,
        EpochClassification::load(epoch, &config.cluster_db_path())?.into_current(),
    )];

    while let Some((epoch, epoch_classification)) =
        EpochClassification::load_previous(list.last().unwrap().0, &config.cluster_db_path())?
    {
        list.push((epoch, epoch_classification.into_current()));
    }

    let validator_summary_csv = {
        let mut validator_summary_csv = vec![];

        let mut csv = vec!["Identity".to_string()];
        let mut validator_stakes: HashMap<Pubkey, HashMap<Epoch, ValidatorStakeState>> =
            HashMap::default();
        let mut validator_epochs = vec![];
        for (epoch, epoch_classification) in list.iter() {
            csv.push(format!("Epoch {}", epoch));
            validator_epochs.push(epoch);
            if let Some(ref validator_classifications) =
                epoch_classification.validator_classifications
            {
                for (identity, classification) in validator_classifications {
                    validator_stakes
                        .entry(*identity)
                        .or_default()
                        .insert(*epoch, classification.stake_state);
                }
            }
        }
        validator_summary_csv.push(csv.join(","));

        let mut validator_stakes = validator_stakes.into_iter().collect::<Vec<_>>();
        validator_stakes.sort_by(|a, b| a.0.cmp(&b.0));
        for (identity, epoch_stakes) in validator_stakes {
            let mut csv = vec![identity.to_string()];
            for epoch in &validator_epochs {
                if let Some(stake_state) = epoch_stakes.get(epoch) {
                    csv.push(format!("{:?}", stake_state));
                } else {
                    csv.push("-".to_string());
                }
            }
            validator_summary_csv.push(csv.join(","));
        }
        validator_summary_csv.join("\n")
    };
    let filename = config.cluster_db_path().join("validator-summary.csv");
    info!("Writing {}", filename.display());
    let mut file = File::create(filename)?;
    file.write_all(&validator_summary_csv.into_bytes())?;

    Ok(())
}

/// Given a validator's current commission and history of commission changes, returns the validator's commission at the end of `epoch`
/// Only works if the commission change history includes all changes for `epoch` and `epoch + 1`.
fn calculate_commission_at_end_of_epoch(
    epoch: u64,
    current_commission: u8,
    commission_change_history: Option<&Vec<CommissionChangeIndexHistoryEntry>>,
) -> u8 {
    match commission_change_history {
        Some(records) => {
            // First check if there is a commission change record in `epoch`. The last one will
            // give us the commision at the end of the epoch.
            let mut rs: Vec<&CommissionChangeIndexHistoryEntry> =
                records.iter().filter(|r| r.epoch <= epoch).collect();

            if !rs.is_empty() {
                rs.sort_by(|a, b| {
                    a.epoch
                        .cmp(&b.epoch)
                        .then(a.epoch_completion.partial_cmp(&b.epoch_completion).unwrap())
                });
                rs.last().unwrap().commission_after.unwrap() as u8
            } else {
                // If we didn't find a commission change in `epoch`, check for commission changes in
                // `epoch + 1`. The first one will give us the commission at the end of `epoch`.
                let mut rs: Vec<&CommissionChangeIndexHistoryEntry> = records
                    .iter()
                    .filter(|r| r.commission_before.is_some() && r.epoch > epoch)
                    .collect();
                if rs.is_empty() {
                    // no commission changes in epoch `epoch + 1`; commission is the current
                    // commission.
                    current_commission
                } else {
                    rs.sort_by(|a, b| {
                        a.epoch
                            .cmp(&b.epoch)
                            .then(a.epoch_completion.partial_cmp(&b.epoch_completion).unwrap())
                    });
                    rs.first().unwrap().commission_before.unwrap() as u8
                }
            }
        }
        // If there are no commission changes, the commission is the current commission
        None => current_commission,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data_center_info::{DataCenterId, DataCenterInfo};
    use std::iter::FromIterator;

    #[test]
    fn test_quality_producer_with_average_skip_rate() {
        solana_logger::setup();
        let config = Config {
            quality_block_producer_percentage: 10,
            max_poor_block_producer_percentage: 40,
            ..Config::default_for_test()
        };

        let confirmed_blocks: HashSet<Slot> = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 14, 21, 22, 43, 44, 45, 46, 47, 48,
        ]
        .iter()
        .cloned()
        .collect();
        let mut leader_schedule = HashMap::default();
        let l1 = Pubkey::new_unique();
        let l2 = Pubkey::new_unique();
        let l3 = Pubkey::new_unique();
        let l4 = Pubkey::new_unique();
        let l5 = Pubkey::new_unique();
        leader_schedule.insert(l1.to_string(), (0..10).collect());
        leader_schedule.insert(l2.to_string(), (10..20).collect());
        leader_schedule.insert(l3.to_string(), (20..30).collect());
        leader_schedule.insert(l4.to_string(), (30..40).collect());
        leader_schedule.insert(l5.to_string(), (40..50).collect());
        let (
            quality,
            poor,
            _reason_msg,
            cluster_average_skip_rate,
            too_many_poor_block_producers,
            blocks_and_slots,
        ) = classify_producers(0, confirmed_blocks, leader_schedule, &config).unwrap();

        assert_eq!(cluster_average_skip_rate, 58);
        assert!(quality.contains(&l1));
        assert!(quality.contains(&l5));
        assert!(quality.contains(&l2));
        assert_eq!(quality.len(), 3);
        assert!(poor.contains(&l3));
        assert!(poor.contains(&l4));
        assert_eq!(poor.len(), 2);
        assert!(!too_many_poor_block_producers);

        // spot-check that returned slots and blocks are correct
        let l1_blocks_and_slots = blocks_and_slots.get(&l1).unwrap();
        assert_eq!(l1_blocks_and_slots.0, 9);
        assert_eq!(l1_blocks_and_slots.1, 10);

        let l2_blocks_and_slots = blocks_and_slots.get(&l2).unwrap();
        assert_eq!(l2_blocks_and_slots.0, 4);
        assert_eq!(l2_blocks_and_slots.1, 10);
    }

    #[test]
    fn test_quality_producer_when_all_good() {
        solana_logger::setup();
        let config = Config {
            quality_block_producer_percentage: 10,
            ..Config::default_for_test()
        };

        let confirmed_blocks: HashSet<Slot> = (0..50).collect();
        let mut leader_schedule = HashMap::new();
        let l1 = Pubkey::new_unique();
        let l2 = Pubkey::new_unique();
        let l3 = Pubkey::new_unique();
        let l4 = Pubkey::new_unique();
        let l5 = Pubkey::new_unique();
        leader_schedule.insert(l1.to_string(), (0..10).collect());
        leader_schedule.insert(l2.to_string(), (10..20).collect());
        leader_schedule.insert(l3.to_string(), (20..30).collect());
        leader_schedule.insert(l4.to_string(), (30..40).collect());
        leader_schedule.insert(l5.to_string(), (40..50).collect());
        let (
            quality,
            poor,
            _reason_msg,
            cluster_average_skip_rate,
            too_many_poor_block_producers,
            blocks_and_slots,
        ) = classify_producers(0, confirmed_blocks, leader_schedule, &config).unwrap();
        assert_eq!(cluster_average_skip_rate, 0);
        assert!(poor.is_empty());
        assert_eq!(quality.len(), 5);
        assert!(!too_many_poor_block_producers);

        // spot-check that returned slots and blocks are correct
        let l1_blocks_and_slots = blocks_and_slots.get(&l1).unwrap();
        assert_eq!(l1_blocks_and_slots.0, 10);
        assert_eq!(l1_blocks_and_slots.1, 10);

        let l5_blocks_and_slots = blocks_and_slots.get(&l5).unwrap();
        assert_eq!(l5_blocks_and_slots.0, 10);
        assert_eq!(l5_blocks_and_slots.1, 10);
    }

    #[test]
    fn test_calculate_commission_at_end_of_epoch_no_history() {
        let expected_commission = 100;

        // If there is no change history, commission should be the current commission
        assert_eq!(
            expected_commission,
            calculate_commission_at_end_of_epoch(123, expected_commission, None) as u8
        );
    }

    #[test]
    fn test_calculate_commission_at_end_of_epoch_recent_change() {
        let expected_commission = 100;
        let epoch: u64 = 123;

        let history = [
            // If there is a commission change in an epoch > `epoch + 1`, that should also be used
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(expected_commission as f32),
                commission_after: Some(10.0),
                epoch: epoch + 2,
                epoch_completion: 50.0,
                ..Default::default()
            },
        ]
        .to_vec();
        assert_eq!(
            expected_commission,
            calculate_commission_at_end_of_epoch(epoch, 10, Some(&history)) as u8
        );
    }

    #[test]
    fn test_calculate_commission_at_end_of_epoch_long_history() {
        let epoch: u64 = 123;
        let expected_commission = 100.0;

        // Changes:
        // null -> 10 10% through epoch 120
        // 10 -> 100 90% through epoch 123
        // 100 -> 50 10% through epoch 124
        // 50 -> 40 50% through epoch 124
        //
        // records deliberately placed out of chronological order
        let history = [
            // fourth
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(50.0),
                commission_after: Some(40.0),
                epoch: epoch + 1,
                epoch_completion: 50.0,
                ..Default::default()
            },
            // first
            CommissionChangeIndexHistoryEntry {
                commission_before: None,
                commission_after: Some(10.0),
                epoch: 120,
                epoch_completion: 10.0,
                ..Default::default()
            },
            // second
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(10.0),
                commission_after: Some(expected_commission),
                epoch,
                epoch_completion: 99.0,
                ..Default::default()
            },
            // third
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(expected_commission),
                commission_after: Some(50.0),
                epoch: epoch + 1,
                epoch_completion: 10.0,
                ..Default::default()
            },
        ]
        .to_vec();

        let commission_at_end = calculate_commission_at_end_of_epoch(epoch, 75, Some(&history));
        assert_eq!(commission_at_end, expected_commission as u8);
    }

    // Test the case where there is one record of changing a commission immediately after the end of an epoch
    #[test]
    fn test_calculate_commission_at_end_of_epoch_short_history() {
        let epoch: u64 = 123;
        let current_commission = 10.0;
        let expected_commission = 100.0;

        // Changes:
        // 100 -> 10 10% through epoch 124
        let history = [CommissionChangeIndexHistoryEntry {
            commission_before: Some(expected_commission),
            commission_after: Some(current_commission),
            epoch: epoch + 1,
            epoch_completion: 50.0,
            ..Default::default()
        }]
        .to_vec();

        let commission_at_end =
            calculate_commission_at_end_of_epoch(epoch, current_commission as u8, Some(&history));
        assert_eq!(commission_at_end, expected_commission as u8);
    }

    #[test]
    fn test_calculate_commission_at_end_of_epoch_irrelevant_history() {
        let epoch: u64 = 123;
        let current_commission = 10.0;
        let expected_commission = 100.0;

        // Changes:
        // 100 -> 10 50% through epoch 124.
        // 10 -> 50 60% through epoch 124. Shouldn't matter.
        let history = [
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(expected_commission),
                commission_after: Some(10.0),
                epoch: epoch + 1,
                epoch_completion: 50.0,
                ..Default::default()
            },
            CommissionChangeIndexHistoryEntry {
                commission_before: Some(10.0),
                commission_after: Some(50.0),
                epoch: epoch + 1,
                epoch_completion: 60.0,
                ..Default::default()
            },
        ]
        .to_vec();

        let commission_at_end =
            calculate_commission_at_end_of_epoch(epoch, current_commission as u8, Some(&history));
        assert_eq!(commission_at_end, expected_commission as u8);
    }

    #[test]
    fn test_adjust_validator_classification_for_data_center_concentration_warn_all() {
        let (mut validator_classifications, data_centers) =
            mocks_for_data_center_concentration_tests();

        let config = Config {
            max_infrastructure_concentration: Some(50.0),
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::WarnAll,
            ..Config::default_for_test()
        };

        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            &config,
        );

        let num_destaked_validators = validator_classifications
            .iter()
            .map(|(_id, vc)| vc.clone())
            .filter(|vc| vc.stake_state == ValidatorStakeState::None)
            .count();

        assert_eq!(num_destaked_validators, 0);
    }

    #[test]
    fn test_adjust_validator_classification_for_data_center_concentration_destake_listed() {
        let (mut validator_classifications, data_centers) =
            mocks_for_data_center_concentration_tests();

        let max_infrastructure_concentration = 50.0;

        // get five validators from the oversaturated data center and put them in the list to be destaked
        let destake_list: ValidatorList = HashSet::from_iter(
            data_centers
                .info
                .iter()
                .find(|dci| dci.stake_percent > max_infrastructure_concentration)
                .map(|dci| dci.validators.iter().copied().take(5).collect::<Vec<_>>())
                .unwrap(),
        );

        let config = Config {
            max_infrastructure_concentration: Some(max_infrastructure_concentration),
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::DestakeListed(
                destake_list,
            ),
            ..Config::default_for_test()
        };

        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            &config,
        );

        let num_destaked_validators = validator_classifications
            .iter()
            .map(|(_id, vc)| vc.clone())
            .filter(|vc| vc.stake_state == ValidatorStakeState::None)
            .count();

        assert_eq!(num_destaked_validators, 5);
    }

    #[test]
    fn test_adjust_validator_classification_for_data_center_concentration_destake_all() {
        let (mut validator_classifications, data_centers) =
            mocks_for_data_center_concentration_tests();

        let config = Config {
            max_infrastructure_concentration: Some(50.0),
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::DestakeAll,
            ..Config::default_for_test()
        };

        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            &config,
        );

        let num_destaked_validators = validator_classifications
            .iter()
            .map(|(_id, vc)| vc.clone())
            .filter(|vc| vc.stake_state == ValidatorStakeState::None)
            .count();

        assert_eq!(num_destaked_validators, 10);
    }

    #[test]
    fn test_adjust_validator_classification_for_data_center_concentration_destake_new() {
        let (mut validator_classifications, data_centers) =
            mocks_for_data_center_concentration_tests();

        let config = Config {
            max_infrastructure_concentration: Some(50.0),
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::DestakeNew,
            ..Config::default_for_test()
        };

        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            &config,
        );

        let destaked_validators: Vec<ValidatorClassification> = validator_classifications
            .iter()
            .map(|(_id, vc)| vc.clone())
            .filter(|vc| vc.stake_state == ValidatorStakeState::None)
            .collect();

        // Only one validator has `new_data_center_residency`==true
        assert_eq!(destaked_validators.len(), 1);
        assert_eq!(
            destaked_validators
                .first()
                .unwrap()
                .new_data_center_residency,
            Some(true)
        );
    }

    #[test]
    fn test_adjust_validator_classification_for_data_center_concentration_destake_overflow() {
        let (mut validator_classifications, data_centers) =
            mocks_for_data_center_concentration_tests();

        let config = Config {
            max_infrastructure_concentration: Some(50.0),
            infrastructure_concentration_affects:
                InfrastructureConcentrationAffects::DestakeOverflow,
            ..Config::default_for_test()
        };

        adjust_validator_classification_for_data_center_concentration(
            &mut validator_classifications,
            &data_centers,
            &config,
        );

        let destaked_validators: Vec<ValidatorClassification> = validator_classifications
            .iter()
            .map(|(_id, vc)| vc.clone())
            .filter(|vc| vc.stake_state == ValidatorStakeState::None)
            .collect();

        // two validators would need to be removed to get the data center under the max_infrastructure_concentration of 50%
        assert_eq!(destaked_validators.len(), 2);

        // verify that the destaked validators were junior, and that their seniority score were reduced.
        // Since their initial seniority scores were 1 and 2, they should have been reduced to 0 and 1
        for val in destaked_validators {
            let &seniority_score = val
                .data_center_residency
                .unwrap()
                .get(&val.current_data_center.unwrap_or_default())
                .unwrap_or(&(100usize));
            assert!(seniority_score < 2usize);
        }
    }

    fn mocks_for_data_center_concentration_tests(
    ) -> (HashMap<Pubkey, ValidatorClassification>, DataCenters) {
        // Creates ValidatorClassifications and DataCenters to model a cluster+epoch for the purposes of testing different InfrastructureConcentrationAffects
        // Creates two data centers and 11 ValidatorClassifications.
        //
        // Data Center 1 ("data_center_oversaturated") (will be oversaturated if max_infrastructure_concentration is < 60)
        // Total stake: 600 / 60% of total
        // 10 validators with 60 stake each
        //  - one validator has `new_data_center_residency` set to true
        //  - 60 stake each
        //  - Seniority scores from 1--10
        //
        // Data center 2 ("data_center_not_oversaturated")
        // Total stake: 400 / 40% of total
        // 1 validator with 600 stake

        let data_center_oversaturated_id = DataCenterId {
            asn: 1234,
            location: "oversaturated".to_string(),
        };
        let data_center_oversaturated_stake = 600;
        let data_center_not_oversaturated_id = DataCenterId {
            asn: 9876,
            location: "not oversaturated".to_string(),
        };
        let data_center_not_oversaturated_stake = 400;

        let validator_in_not_oversaturated = ValidatorClassification {
            identity: Pubkey::new_unique(),
            vote_address: Pubkey::new_unique(),
            stake_state: ValidatorStakeState::Bonus,
            stake_state_reason: "Test bonus reason".to_string(),
            data_center_residency: Some(HashMap::from([(
                data_center_not_oversaturated_id.clone(),
                123,
            )])),
            current_data_center: Some(data_center_not_oversaturated_id.clone()),
            ..ValidatorClassification::default()
        };

        let mut validator_classifications = HashMap::new();

        let num_validators_in_oversaturated_data_center = 10;
        // Create 10 validators for the oversaturated data center
        for idx in 1..=num_validators_in_oversaturated_data_center {
            let identity = Pubkey::new_unique();
            validator_classifications.insert(
                identity,
                ValidatorClassification {
                    identity,
                    vote_address: Pubkey::new_unique(),
                    new_data_center_residency: Some(idx == 1),
                    stake_state: ValidatorStakeState::Bonus,
                    stake_state_reason: "Test bonus reason".to_string(),
                    data_center_residency: Some(HashMap::from([(
                        data_center_oversaturated_id.clone(),
                        idx,
                    )])),

                    current_data_center: Some(data_center_oversaturated_id.clone()),
                    ..ValidatorClassification::default()
                },
            );
        }

        let data_center_oversaturated = DataCenterInfo {
            id: data_center_oversaturated_id,
            stake: data_center_oversaturated_stake,
            stake_percent: 60.0,
            validators: validator_classifications
                .iter()
                .map(|(id, _vc)| *id)
                .collect(),
            // data_center_oversaturated_stake / num_validators_in_oversaturated_data_center == 60
            validators_stake: Some(
                validator_classifications
                    .iter()
                    .map(|(id, _vc)| (*id, 60))
                    .collect(),
            ),
        };

        let data_center_not_oversaturated = DataCenterInfo {
            id: data_center_not_oversaturated_id,
            stake: data_center_not_oversaturated_stake,
            stake_percent: 40.0,
            validators: vec![validator_in_not_oversaturated.identity],
            validators_stake: Some(HashMap::from([(
                validator_in_not_oversaturated.identity,
                data_center_not_oversaturated_stake,
            )])),
        };

        let data_centers = DataCenters {
            info: vec![data_center_oversaturated, data_center_not_oversaturated],
            by_identity: validator_classifications
                .iter()
                .map(|(id, vc)| (*id, vc.current_data_center.as_ref().unwrap().clone()))
                .collect(),
        };

        (validator_classifications, data_centers)
    }
}
