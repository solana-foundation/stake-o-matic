use {
    crate::{db::*, generic_stake_pool::*, rpc_client_utils::*},
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, values_t, App, AppSettings, Arg,
        ArgMatches, SubCommand,
    },
    log::*,
    registry_cli::get_participants_with_state,
    registry_program::state::{Participant, ParticipantState},
    solana_clap_utils::{
        input_parsers::{keypair_of, lamports_of_sol, pubkey_of},
        input_validators::{
            is_amount, is_keypair, is_parsable, is_pubkey_or_keypair, is_url, is_valid_percentage,
        },
    },
    solana_client::rpc_client::RpcClient,
    solana_notifier::Notifier,
    solana_sdk::{
        account::from_account,
        account_utils::StateMut,
        clock::{Epoch, Slot},
        commitment_config::CommitmentConfig,
        native_token::*,
        pubkey::Pubkey,
        slot_history::{self, SlotHistory},
        stake_history::StakeHistory,
        sysvar,
    },
    solana_stake_program::stake_state::StakeState,
    solana_vote_program::vote_state::VoteState,
    std::{
        collections::{HashMap, HashSet},
        error,
        fs::{self, File},
        io::Write,
        path::PathBuf,
        process,
        str::FromStr,
        time::Duration,
    },
    thiserror::Error,
};

mod data_center_info;
mod db;
mod generic_stake_pool;
mod legacy_stake_pool;
mod rpc_client_utils;
mod stake_pool;
mod stake_pool_v0;
mod validator_list;
mod validators_app;

type BoxResult<T> = Result<T, Box<dyn error::Error>>;
type ValidatorList = HashSet<Pubkey>;
type IdentityToParticipant = HashMap<Pubkey, Pubkey>;

enum InfrastructureConcentrationAffectKind {
    Destake(String),
    Warn(String),
}

#[derive(Debug)]
enum InfrastructureConcentrationAffects {
    WarnAll,
    DestakeListed(ValidatorList),
    DestakeAll,
    DestakeNew,
}

impl InfrastructureConcentrationAffects {
    fn destake_memo(concentration: f64) -> String {
        format!(
            "infrastructure concentration {:.1}% is too high",
            concentration
        )
    }
    fn warning_memo(concentration: f64) -> String {
        format!(
            "infrastructure concentration {:.1}% is too high; \
            consider finding a new data center",
            concentration
        )
    }
    pub fn memo(
        &self,
        validator_id: &Pubkey,
        new_validator: bool,
        concentration: f64,
    ) -> InfrastructureConcentrationAffectKind {
        match self {
            Self::DestakeAll => {
                InfrastructureConcentrationAffectKind::Destake(Self::destake_memo(concentration))
            }
            Self::DestakeNew => {
                if new_validator {
                    InfrastructureConcentrationAffectKind::Destake(Self::destake_memo(
                        concentration,
                    ))
                } else {
                    InfrastructureConcentrationAffectKind::Warn(Self::warning_memo(concentration))
                }
            }
            Self::WarnAll => {
                InfrastructureConcentrationAffectKind::Warn(Self::warning_memo(concentration))
            }
            Self::DestakeListed(ref list) => {
                if list.contains(validator_id) {
                    InfrastructureConcentrationAffectKind::Destake(Self::destake_memo(
                        concentration,
                    ))
                } else {
                    InfrastructureConcentrationAffectKind::Warn(Self::warning_memo(concentration))
                }
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("cannot convert to InfrastructureConcentrationAffects: {0}")]
struct InfrastructureConcentrationAffectsFromStrError(String);

impl FromStr for InfrastructureConcentrationAffects {
    type Err = InfrastructureConcentrationAffectsFromStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "warn" => Ok(Self::WarnAll),
            "destake-all" => Ok(Self::DestakeAll),
            "destake-new" => Ok(Self::DestakeNew),
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cluster {
    Testnet,
    MainnetBeta,
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

#[derive(Debug)]
struct Config {
    json_rpc_url: String,
    cluster: Cluster,
    db_path: PathBuf,
    markdown_path: Option<PathBuf>,

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
    max_infrastructure_concentration: f64,

    /// How validators with infrastruction concentration above `max_infrastructure_concentration`
    /// will be affected. Accepted values are:
    /// 1) "warn"       - Stake unaffected. A warning message is notified
    /// 2) "destake"    - Removes all validator stake
    /// 3) PATH_TO_YAML - Reads a list of validator identity pubkeys from the specified YAML file
    ///                   destaking those in the list and warning any others
    infrastructure_concentration_affects: InfrastructureConcentrationAffects,

    bad_cluster_average_skip_rate: usize,

    /// Destake if the validator's vote credits for the latest full epoch are less than this percentage
    /// of the cluster average
    min_epoch_credit_percentage_of_average: usize,

    /// Minimum amount of lamports a validator must stake on itself to be eligible for a delegation
    min_self_stake_lamports: u64,

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
}

impl Config {
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self {
            json_rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
            cluster: Cluster::MainnetBeta,
            db_path: PathBuf::default(),
            markdown_path: None,
            dry_run: true,
            quality_block_producer_percentage: 15,
            max_poor_block_producer_percentage: 20,
            max_commission: 100,
            min_release_version: None,
            max_old_release_version_percentage: 10,
            max_poor_voter_percentage: 20,
            confirmed_block_cache_path: default_confirmed_block_cache_path(),
            max_infrastructure_concentration: 100.0,
            infrastructure_concentration_affects: InfrastructureConcentrationAffects::WarnAll,
            bad_cluster_average_skip_rate: 50,
            min_epoch_credit_percentage_of_average: 50,
            min_self_stake_lamports: 0,
            enforce_min_self_stake: false,
            enforce_testnet_participation: false,
            min_testnet_participation: None,
        }
    }

    fn cluster_db_path_for(&self, cluster: Cluster) -> PathBuf {
        self.db_path.join(format!("data-{}", cluster))
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

fn get_config() -> BoxResult<(Config, RpcClient, Box<dyn GenericStakePool>)> {
    let default_confirmed_block_cache_path = default_confirmed_block_cache_path()
        .to_str()
        .unwrap()
        .to_string();
    let app_version = &*app_version();
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
                .validator(is_url)
                .help("JSON RPC URL for the cluster")
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
            Arg::with_name("markdown")
                .long("markdown")
                .takes_value(false)
                .help("Output markdown")
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
                .default_value("100")
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
            Arg::with_name("enforce_min_self_stake")
                .long("enforce-min-self-stake")
                .takes_value(false)
                .help("Enforce the minimum self-stake requirement")
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
        .subcommand(
            SubCommand::with_name("legacy").about("Use the legacy staking solution")
            .arg(
                Arg::with_name("source_stake_address")
                    .index(1)
                    .value_name("SOURCE_STAKE_ADDRESS")
                    .takes_value(true)
                    .required(true)
                    .validator(is_pubkey_or_keypair)
                    .help("The source stake account for splitting individual validator stake accounts from")
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
                Arg::with_name("baseline_stake_amount")
                    .long("baseline-stake-amount")
                    .value_name("SOL")
                    .takes_value(true)
                    .default_value("5000")
                    .validator(is_amount)
            )
            .arg(
                Arg::with_name("bonus_stake_amount")
                    .long("bonus-stake-amount")
                    .value_name("SOL")
                    .takes_value(true)
                    .default_value("50000")
                    .validator(is_amount)
            )
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
                    .default_value("1")
                    .validator(is_amount)
                    .help("The minimum balance to keep in the reserve stake account")
            )
            .arg(
                Arg::with_name("baseline_stake_amount")
                    .long("baseline-stake-amount")
                    .value_name("SOL")
                    .takes_value(true)
                    .default_value("5000")
                    .validator(is_amount)
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
                Arg::with_name("baseline_stake_amount")
                    .long("baseline-stake-amount")
                    .value_name("SOL")
                    .takes_value(true)
                    .default_value("5000")
                    .validator(is_amount)
            )
        )
        .get_matches();

    let dry_run = !matches.is_present("confirm");
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

    let enforce_testnet_participation = matches.is_present("enforce_testnet_participation");
    let min_testnet_participation = values_t!(matches, "min_testnet_participation", usize)
        .ok()
        .map(|v| (v[0], v[1]));
    if min_testnet_participation.is_some() && cluster != Cluster::MainnetBeta {
        error!("--min-testnet-participation only available for `--cluster mainnet-beta`");
        process::exit(1);
    }

    let json_rpc_url = match cluster {
        Cluster::MainnetBeta => value_t!(matches, "json_rpc_url", String)
            .unwrap_or_else(|_| "http://api.mainnet-beta.solana.com".into()),
        Cluster::Testnet => value_t!(matches, "json_rpc_url", String)
            .unwrap_or_else(|_| "http://testnet.solana.com".into()),
    };
    let db_path = value_t_or_exit!(matches, "db_path", PathBuf);
    let markdown_path = if matches.is_present("markdown") {
        Some(db_path.join("md"))
    } else {
        None
    };

    let confirmed_block_cache_path = matches
        .value_of("confirmed_block_cache_path")
        .map(PathBuf::from)
        .unwrap();

    let bad_cluster_average_skip_rate =
        value_t!(matches, "bad_cluster_average_skip_rate", usize).unwrap_or(50);
    let max_infrastructure_concentration =
        value_t!(matches, "max_infrastructure_concentration", f64).unwrap();
    let infrastructure_concentration_affects = value_t!(
        matches,
        "infrastructure_concentration_affects",
        InfrastructureConcentrationAffects
    )
    .unwrap();

    let config = Config {
        json_rpc_url,
        cluster,
        db_path,
        markdown_path,
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
        enforce_min_self_stake,
        enforce_testnet_participation,
        min_testnet_participation,
    };

    info!("RPC URL: {}", config.json_rpc_url);
    let rpc_client =
        RpcClient::new_with_timeout(config.json_rpc_url.clone(), Duration::from_secs(180));

    // Sanity check that the RPC endpoint is healthy before performing too much work
    rpc_client
        .get_health()
        .map_err(|err| format!("RPC endpoint is unhealthy: {:?}", err))?;

    let stake_pool: Box<dyn GenericStakePool> = match matches.subcommand() {
        ("legacy", Some(matches)) => {
            let authorized_staker = keypair_of(&matches, "authorized_staker").unwrap();
            let source_stake_address = pubkey_of(&matches, "source_stake_address").unwrap();
            let baseline_stake_amount =
                sol_to_lamports(value_t_or_exit!(matches, "baseline_stake_amount", f64));
            let bonus_stake_amount =
                sol_to_lamports(value_t_or_exit!(matches, "bonus_stake_amount", f64));

            Box::new(legacy_stake_pool::new(
                &rpc_client,
                authorized_staker,
                baseline_stake_amount,
                bonus_stake_amount,
                source_stake_address,
            )?)
        }
        ("stake-pool-v0", Some(matches)) => {
            let authorized_staker = keypair_of(&matches, "authorized_staker").unwrap();
            let reserve_stake_address = pubkey_of(&matches, "reserve_stake_address").unwrap();
            let min_reserve_stake_balance =
                sol_to_lamports(value_t_or_exit!(matches, "min_reserve_stake_balance", f64));
            let baseline_stake_amount =
                sol_to_lamports(value_t_or_exit!(matches, "baseline_stake_amount", f64));
            Box::new(stake_pool_v0::new(
                &rpc_client,
                authorized_staker,
                baseline_stake_amount,
                reserve_stake_address,
                min_reserve_stake_balance,
            )?)
        }
        ("stake-pool", Some(matches)) => {
            let authorized_staker = keypair_of(&matches, "authorized_staker").unwrap();
            let pool_address = pubkey_of(&matches, "pool_address").unwrap();
            let baseline_stake_amount =
                sol_to_lamports(value_t_or_exit!(matches, "baseline_stake_amount", f64));
            Box::new(stake_pool::new(
                &rpc_client,
                authorized_staker,
                pool_address,
                baseline_stake_amount,
            )?)
        }
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
    for (validator_identity, (blocks, slots)) in blocks_and_slots {
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
        avg_epoch_credits * (config.min_epoch_credit_percentage_of_average as u64) / 100;

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
    let all_stake_accounts = rpc_client.get_program_accounts(&solana_stake_program::id())?;

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
                        .stake_activating_and_deactivating(epoch, Some(&stake_history), true)
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

fn get_testnet_participation(config: &Config) -> BoxResult<Option<HashMap<Pubkey, bool>>> {
    if let Some((n, m)) = &config.min_testnet_participation {
        assert_eq!(config.cluster, Cluster::MainnetBeta);
        let latest_testnet_epoch_classification =
            EpochClassification::load_latest(&config.cluster_db_path_for(Cluster::Testnet))?
                .ok_or("Unable to load testnet epoch classification")?
                .1
                .into_current();

        let testnet_participation = latest_testnet_epoch_classification
            .validator_classifications
            .unwrap()
            .drain()
            .filter_map(|(_, validator_classification)| {
                validator_classification
                    .participant
                    .map(|participant| (participant, validator_classification.staked_for(*n, *m)))
            })
            .collect::<HashMap<_, _>>();

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
) -> BoxResult<EpochClassificationV1> {
    let last_epoch = epoch - 1;

    let testnet_participation = get_testnet_participation(config)?;

    let data_centers = data_center_info::get(&config.cluster.to_string())
        .map_err(|e| {
            warn!("infrastructure concentration skipped: {}", e);
            e
        })
        .unwrap_or_default();

    let infrastructure_concentration_too_high = data_centers
        .info
        .iter()
        .filter_map(|dci| {
            if dci.stake_percent > config.max_infrastructure_concentration {
                Some((dci.validators.clone(), dci.stake_percent))
            } else {
                None
            }
        })
        .flat_map(|(v, sp)| v.into_iter().map(move |v| (v, sp)))
        .collect::<HashMap<_, _>>();

    let vote_account_info = get_vote_account_info(&rpc_client, last_epoch)?;

    let self_stake_by_vote_account =
        get_self_stake_by_vote_account(rpc_client, epoch, &vote_account_info)?;

    let (cluster_nodes_with_old_version, min_release_version): (HashMap<String, _>, _) =
        match config.min_release_version {
            Some(ref min_release_version) => (
                rpc_client
                    .get_cluster_nodes()?
                    .into_iter()
                    .filter_map(|rpc_contact_info| {
                        if let Ok(identity) = Pubkey::from_str(&rpc_contact_info.pubkey) {
                            if validator_list.contains(&identity) {
                                if let Some(ref version) = rpc_contact_info.version {
                                    if let Ok(semver) = semver::Version::parse(version) {
                                        if semver < *min_release_version {
                                            return Some((rpc_contact_info.pubkey, semver));
                                        }
                                    }
                                }
                            }
                        }
                        None
                    })
                    .collect(),
                min_release_version.to_string(),
            ),
            None => (HashMap::default(), "".to_string()),
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
    ) = classify_block_producers(&rpc_client, &config, last_epoch)?;

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
    ) = classify_poor_voters(&config, &vote_account_info);

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
            "Maximum infrastructure concentration: {:0}%",
            config.max_infrastructure_concentration
        ),
        format!(
            "Minimum required self stake: {}",
            Sol(config.min_self_stake_lamports)
        ),
    ];

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

        for VoteAccountInfo {
            identity,
            vote_address,
            commission,
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

            let previous_classification = previous_epoch_validator_classifications
                .map(|p| p.get(&identity))
                .flatten();

            let mut previous_data_center_residency = previous_classification
                .map(|vc| vc.data_center_residency.clone())
                .flatten()
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

            let infrastructure_concentration_destake_reason = infrastructure_concentration_too_high
                .get(&identity)
                .map(|concentration| {
                    config.infrastructure_concentration_affects.memo(
                        &identity,
                        !previous_data_center_residency.contains_key(&current_data_center),
                        *concentration,
                    )
                })
                .and_then(|affect| match affect {
                    InfrastructureConcentrationAffectKind::Destake(reason) => Some(reason),
                    InfrastructureConcentrationAffectKind::Warn(reason) => {
                        validator_notes.push(reason);
                        None
                    }
                });

            let insufficent_self_stake_msg =
                format!("insufficient self stake: {}", Sol(self_stake));
            if !config.enforce_min_self_stake && self_stake < config.min_self_stake_lamports {
                validator_notes.push(insufficent_self_stake_msg.clone());
            }

            let insufficent_testnet_participation = testnet_participation
                .as_ref()
                .map(|testnet_participation| {
                    if let Some(participant) = participant {
                        if !testnet_participation.get(&participant).unwrap_or(&true) {
                            let note = "insufficient testnet participation".to_string();
                            if config.enforce_testnet_participation {
                                return Some(note);
                            } else {
                                validator_notes.push(note);
                            }
                        }
                    }
                    None
                })
                .flatten();

            let (stake_state, reason) = if let Some(reason) =
                infrastructure_concentration_destake_reason
            {
                (ValidatorStakeState::None, reason)
            } else if config.enforce_min_self_stake && self_stake < config.min_self_stake_lamports {
                (ValidatorStakeState::None, insufficent_self_stake_msg)
            } else if commission > config.max_commission {
                (
                    ValidatorStakeState::None,
                    format!("commission is too high: {}% commission", commission),
                )
            } else if let Some(insufficent_testnet_participation) =
                insufficent_testnet_participation
            {
                (ValidatorStakeState::None, insufficent_testnet_participation)
            } else if poor_voters.contains(&identity) {
                (
                    ValidatorStakeState::None,
                    format!("insufficient vote credits: {}", vote_credits_msg),
                )
            } else if cluster_nodes_with_old_version.contains_key(&identity.to_string()) {
                (
                    ValidatorStakeState::None,
                    format!(
                        "Outdated solana release: {}",
                        cluster_nodes_with_old_version
                            .get(&identity.to_string())
                            .unwrap()
                    ),
                )
            } else if quality_block_producers.contains(&identity) {
                (
                    ValidatorStakeState::Bonus,
                    format!(
                        "good block production during epoch {}: {}",
                        last_epoch, block_producer_classification_reason_msg
                    ),
                )
            } else if poor_block_producers.contains(&identity) {
                (
                    ValidatorStakeState::Baseline,
                    format!(
                        "poor block production during epoch {}: {} ",
                        last_epoch, block_producer_classification_reason_msg
                    ),
                )
            } else {
                assert!(!poor_voters.contains(&identity));
                assert!(not_in_leader_schedule.contains(&identity));
                (
                    ValidatorStakeState::Baseline,
                    format!("no leader slots; {}", vote_credits_msg),
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
                    - stake state: {:?} - data center: {:?} (seniority: {})\n\
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
                reason
            );

            let mut stake_states = previous_classification
                .map(|vc| vc.stake_states.clone())
                .flatten()
                .unwrap_or_default();
            stake_states.insert(0, (stake_state, reason.clone()));

            validator_classifications.insert(
                identity,
                ValidatorClassification {
                    identity,
                    vote_address,
                    stake_state,
                    stake_states: Some(stake_states),
                    stake_state_reason: reason,
                    notes: validator_notes,
                    data_center_residency: Some(data_center_residency),
                    current_data_center: Some(current_data_center.clone()),
                    participant,
                },
            );
        }
        notes.push(format!(
            "{} validators processed",
            validator_classifications.len()
        ));

        Some(validator_classifications)
    };

    Ok(EpochClassificationV1 {
        data_center_info: data_centers.info,
        validator_classifications,
        notes,
    })
}

fn main() -> BoxResult<()> {
    solana_logger::setup_with_default("solana=info");

    let (config, rpc_client, mut stake_pool) = get_config()?;

    info!("Loading participants...");
    let participants = get_participants_with_state(
        &RpcClient::new("https://api.mainnet-beta.solana.com".to_string()),
        Some(ParticipantState::Approved),
    )?;

    let (mainnet_identity_to_participant, testnet_identity_to_participant): (
        IdentityToParticipant,
        IdentityToParticipant,
    ) = participants
        .iter()
        .map(
            |(
                participant,
                Participant {
                    mainnet_identity,
                    testnet_identity,
                    ..
                },
            )| {
                (
                    (*mainnet_identity, *participant),
                    (*testnet_identity, *participant),
                )
            },
        )
        .unzip();

    info!("{} participants loaded", participants.len());
    assert!(participants.len() > 450); // Hard coded sanity check...

    let (validator_list, identity_to_participant) = match config.cluster {
        Cluster::MainnetBeta => (
            mainnet_identity_to_participant.keys().cloned().collect(),
            mainnet_identity_to_participant,
        ),
        Cluster::Testnet => (
            validator_list::testnet_validators().into_iter().collect(),
            testnet_identity_to_participant,
        ),
    };

    let notifier = if config.dry_run {
        Notifier::new("DRYRUN")
    } else {
        Notifier::default()
    };

    if !config.dry_run && notifier.is_empty() {
        return Err("A notifier must be active with --confirm".into());
    }

    let epoch = rpc_client.get_epoch_info()?.epoch;
    info!("Epoch: {:?}", epoch);
    if epoch == 0 {
        return Ok(());
    }

    info!("Data directory: {}", config.cluster_db_path().display());

    let previous_epoch_classification =
        EpochClassification::load_previous(epoch, &config.cluster_db_path())?
            .map(|p| p.1)
            .unwrap_or_default()
            .into_current();

    let (mut epoch_classification, first_time) =
        if EpochClassification::exists(epoch, &config.cluster_db_path()) {
            info!("Classification for {} already exists", epoch);
            (
                EpochClassification::load(epoch, &config.cluster_db_path())?.into_current(),
                false,
            )
        } else {
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
                )?,
                true,
            )
        };

    let mut notifications = epoch_classification.notes.clone();

    let success = if let Some(ref validator_classifications) =
        epoch_classification.validator_classifications
    {
        let previous_validator_classifications = previous_epoch_classification
            .validator_classifications
            .unwrap_or_default();

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

                ValidatorStake {
                    identity: vc.identity,
                    vote_address: vc.vote_address,
                    stake_state: vc.stake_state,
                }
            })
            .collect();

        let (stake_pool_notes, success) =
            stake_pool.apply(&rpc_client, config.dry_run, &desired_validator_stake)?;
        notifications.extend(stake_pool_notes.clone());
        epoch_classification.notes.extend(stake_pool_notes);

        validator_notes.sort();
        notifications.extend(validator_notes);

        validator_stake_change_notes.sort();
        notifications.extend(validator_stake_change_notes);

        success
    } else {
        true
    };

    if first_time {
        EpochClassification::new(epoch_classification).save(epoch, &config.cluster_db_path())?;
        generate_markdown(epoch, &config)?;

        // Only notify the user if this is the first run for this epoch
        for notification in notifications {
            info!("notification: {}", notification);
            notifier.send(&notification);
        }
    }

    if success {
        Ok(())
    } else {
        Err("something failed".into())
    }
}

fn generate_markdown(epoch: Epoch, config: &Config) -> BoxResult<()> {
    let markdown_path = match config.markdown_path.as_ref() {
        Some(d) => d,
        None => return Ok(()),
    };
    fs::create_dir_all(&markdown_path)?;

    let mut list = vec![(
        epoch,
        EpochClassification::load(epoch, &config.cluster_db_path())?.into_current(),
    )];

    let cluster_md = match config.cluster {
        Cluster::MainnetBeta => "Mainnet",
        Cluster::Testnet => "Testnet",
    };

    while let Some((epoch, epoch_classification)) =
        EpochClassification::load_previous(list.last().unwrap().0, &config.cluster_db_path())?
    {
        list.push((epoch, epoch_classification.into_current()));
    }

    let mut validators_markdown: HashMap<_, Vec<_>> = HashMap::new();

    let mut cluster_markdown = vec![];

    for (epoch, epoch_classification) in list {
        cluster_markdown.push(format!("### Epoch {}", epoch));
        for note in epoch_classification.notes {
            cluster_markdown.push(format!("* {}", note));
        }

        if let Some(validator_classifications) = epoch_classification.validator_classifications {
            let mut validator_classifications =
                validator_classifications.into_iter().collect::<Vec<_>>();
            validator_classifications.sort_by(|a, b| a.0.cmp(&b.0));
            for (identity, classification) in validator_classifications {
                let validator_markdown = validators_markdown.entry(identity).or_default();

                validator_markdown.push(format!(
                    "### [[{1} Epoch {0}|{1}#Epoch-{0}]]",
                    epoch, cluster_md
                ));
                validator_markdown.push(classification.stake_state_reason.clone());

                let stake_state_streak = classification.stake_state_streak();
                validator_markdown.push(format!(
                    "* Stake level: **{:?}**{}",
                    classification.stake_state,
                    if stake_state_streak > 1 {
                        format!(" (for {} epochs)", stake_state_streak)
                    } else {
                        "".to_string()
                    }
                ));
                validator_markdown.push(format!(
                    "* Vote account address: {}",
                    classification.vote_address
                ));
                if let (Some(current_data_center), Some(data_center_residency)) = (
                    classification.current_data_center,
                    classification.data_center_residency,
                ) {
                    validator_markdown.push(format!("* Data Center: {}", current_data_center));

                    if data_center_residency.len() > 1
                        || (data_center_residency.len() == 1
                            && !data_center_residency.contains_key(&current_data_center))
                    {
                        let data_center_residency = data_center_residency
                            .keys()
                            .cloned()
                            .map(|data_center| data_center.to_string())
                            .collect::<Vec<_>>();
                        validator_markdown.push(format!(
                            "* Resident Data Center: {}",
                            data_center_residency.join(",")
                        ));
                    }
                }

                for note in classification.notes {
                    validator_markdown.push(format!("* {}", note));
                }
            }
        }
    }

    for (identity, validator_markdown) in validators_markdown {
        let markdown = validator_markdown.join("\n");
        let filename = markdown_path.join(format!("Validator-{}.md", identity));
        info!("Writing {}", filename.display());
        let mut file = File::create(filename)?;
        file.write_all(&markdown.into_bytes())?;
    }

    let markdown = cluster_markdown.join("\n");
    let filename = markdown_path.join(format!("{}.md", cluster_md));
    info!("Writing {}", filename.display());
    let mut file = File::create(filename)?;
    file.write_all(&markdown.into_bytes())?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

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
        let (quality, poor, _reason_msg, cluster_average_skip_rate, too_many_poor_block_producers) =
            classify_producers(0, confirmed_blocks, leader_schedule, &config).unwrap();
        assert_eq!(cluster_average_skip_rate, 58);
        assert!(quality.contains(&l1));
        assert!(quality.contains(&l5));
        assert!(quality.contains(&l2));
        assert_eq!(quality.len(), 3);
        assert!(poor.contains(&l3));
        assert!(poor.contains(&l4));
        assert_eq!(poor.len(), 2);
        assert!(!too_many_poor_block_producers);
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
        let (quality, poor, _reason_msg, cluster_average_skip_rate, too_many_poor_block_producers) =
            classify_producers(0, confirmed_blocks, leader_schedule, &config).unwrap();
        assert_eq!(cluster_average_skip_rate, 0);
        assert!(poor.is_empty());
        assert_eq!(quality.len(), 5);
        assert!(!too_many_poor_block_producers);
    }
}
