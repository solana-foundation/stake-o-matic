use crate::Cluster::{MainnetBeta, Testnet};
use crate::{Cluster, Config, Pubkey, ValidatorList};
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use log::info;
use solana_foundation_delegation_program_registry::state::Participant;
use std::collections::HashMap;
use std::str::FromStr;

/// Validators must have reported within OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT of the mode optimistic slot in at least NUM_SUCCESSFUL_REPORTING_SAMPLES
/// out of every NUM_REPORTING_SAMPLES samples
const NUM_REPORTING_SAMPLES: i64 = 10;
const NUM_SUCCESSFUL_REPORTING_SAMPLES: i32 = 8;
const OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT: i32 = 128;

type GetKeyFromParticipant = fn(&Participant) -> Pubkey;

pub fn get_testnet(participant: &Participant) -> Pubkey {
    participant.testnet_identity
}

pub fn get_mainnet(participant: &Participant) -> Pubkey {
    participant.mainnet_identity
}

/// Checks if validators have been reporting their optimistic slots regularly and accurately.
/// Returns a map of validator keys -> reasons for destaking
/// Validators that are reporting correctly will not be in the map
pub fn get_reported_performance_metrics(
    config: &Config,
    cluster: &Cluster,
    validator_list: &ValidatorList,
    all_participants: &HashMap<Pubkey, Participant>,
) -> Result<Option<HashMap<Pubkey, String>>, Box<dyn std::error::Error>> {
    if !(cluster == &MainnetBeta || cluster == &Testnet) {
        return Err(
            "get_reported_performance_metrics() only works for clusters Testnet and MainnetBeta"
                .into(),
        );
    }

    if let (Some(performance_db_url), Some(performance_db_token)) =
        (&config.performance_db_url, &config.performance_db_token)
    {
        let mut performance_failures: HashMap<Pubkey, String> = HashMap::new();

        let (get_pk_from_this_cluster, get_pk_from_other_cluster): (
            GetKeyFromParticipant,
            GetKeyFromParticipant,
        ) = if cluster == &MainnetBeta {
            (get_mainnet, get_testnet)
        } else {
            (get_testnet, get_mainnet)
        };

        let other_cluster_validators: ValidatorList = all_participants
            .iter()
            .map(|(_k, p)| get_pk_from_other_cluster(p))
            .collect();

        let mut okay_counts: HashMap<Pubkey, i32> = HashMap::new();
        let now = Utc::now();
        info!("Starting to collect samples at {:?}", now);

        for idx in 0..NUM_REPORTING_SAMPLES {
            let data = fetch_data(
                performance_db_url,
                performance_db_token,
                cluster,
                now - Duration::days(idx),
            )?;

            // filter out validators not in the SFDP
            let sfdp_data: HashMap<Pubkey, i32> = data
                .into_iter()
                .filter(|(pubkey, _v)| {
                    // if the validator is from the other cluster, destake
                    if other_cluster_validators.contains(pubkey) {
                        let participant = all_participants
                            .iter()
                            .find(|(_k, p)| get_pk_from_other_cluster(p) == *pubkey);

                        if let Some((_k, participant)) = participant {
                            performance_failures.insert(
                                get_pk_from_this_cluster(participant),
                                format!(
                                    "{:?} validator {:?} reported to {:?}",
                                    if cluster == &MainnetBeta {
                                        Testnet
                                    } else {
                                        MainnetBeta
                                    },
                                    pubkey,
                                    cluster
                                ),
                            );
                        };
                    };
                    validator_list.contains(pubkey)
                })
                .collect();

            let optimistic_slot_modes = get_modes(sfdp_data.values().cloned().collect());
            let min_optimistic_slot = optimistic_slot_modes
                .first()
                .ok_or("Could not get mode of optimistic slots")?
                - OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT;
            let max_optimistic_slot = optimistic_slot_modes
                .last()
                .ok_or("Could not get mode of optimistic slots")?
                + OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT;

            sfdp_data.iter().for_each(|(k, v)| {
                if v >= &min_optimistic_slot && v <= &max_optimistic_slot {
                    let count = okay_counts.entry(*k).or_insert(0);
                    *count += 1;
                }
            })
        }

        info!("{}: Validators: {:?}", cluster, validator_list.len());

        let num_reporting_to_wrong_cluster = performance_failures.len();
        info!(
            "{}: Validators reporting from wrong cluster: {:?}",
            cluster, num_reporting_to_wrong_cluster
        );

        let mut non_reporters: i32 = 0;

        validator_list.iter().for_each(|validator_pubkey| {
            if let Some(ok_count) = okay_counts.get(validator_pubkey) {
                if ok_count < &NUM_SUCCESSFUL_REPORTING_SAMPLES {
                    performance_failures.entry(*validator_pubkey)
                        .or_insert(
                            format!("Good reporting in only {:?} out of {:?} samples taken. Must report well in at least {:?}/{:?}",
                                    ok_count,
                                    NUM_REPORTING_SAMPLES,
                                    NUM_SUCCESSFUL_REPORTING_SAMPLES,
                                    NUM_REPORTING_SAMPLES
                            )
                        );
                }
            } else {
                non_reporters += 1;
                performance_failures.entry(*validator_pubkey).or_insert_with(|| "No performance reporting from validator".into());
            }
        });

        // To receive stake on MainnetBeta, the corresponding Testnet validator must also be reporting
        if cluster == &MainnetBeta {
            let testnet_reported_metrics = get_reported_performance_metrics(
                config,
                &Testnet,
                &other_cluster_validators, // if `cluster` is MainnetBeta, other_cluster_validators holds the keys for all Testnet validators
                all_participants,
            )?
            .ok_or("Could not get Testnet reported metrics")?;
            all_participants.iter().for_each(|(_k, participant)| {
                if let Some(testnet_failure_reason) =
                    testnet_reported_metrics.get(&participant.testnet_identity)
                {
                    performance_failures
                        .entry(participant.mainnet_identity)
                        .or_insert(format!(
                            "Insufficient reported testnet performance: {}",
                            testnet_failure_reason
                        ));
                }
            })
        }

        info!("{}: Non-reporters: {:?}", cluster, non_reporters);
        info!(
            "{}: Reporting validators with < {:?}/{:?} successful reports: {:?}",
            cluster,
            NUM_REPORTING_SAMPLES,
            NUM_SUCCESSFUL_REPORTING_SAMPLES,
            okay_counts
                .iter()
                .filter(|(_k, v)| v < &&NUM_SUCCESSFUL_REPORTING_SAMPLES)
                .collect::<HashMap<&Pubkey, &i32>>()
                .len()
        );

        info!(
            "{}: Total to be destaked for poor reporting: {:?}",
            cluster,
            performance_failures.len()
        );

        Ok(Some(performance_failures))
    } else {
        info!("Not checking for self-reported performance data.");
        Ok(None)
    }
}

/// Fetch self-reported data from the InfluxDB database for the time range (end - 1minute..end)
/// Validators that are reporting tend to report more than once a second, so if there are no reports
/// from a validator in a minute, we can assume they are under-reporting.
fn fetch_data(
    performance_db_url: &String,
    performance_db_token: &String,
    cluster: &Cluster,
    end: DateTime<Utc>,
) -> Result<HashMap<Pubkey, i32>, Box<dyn std::error::Error>> {
    let query = format!(
        "from(bucket:\"{}\")
        |> range(start: {}, stop: {})
        |> filter(fn: (r) => r[\"_measurement\"] == \"optimistic_slot\")
        |> filter(fn: (r) => r[\"_field\"] == \"slot\")
        |> group(columns: [\"host_id\"])
        |> max(column: \"_time\")
                |> group(columns: [\"host_id\"])
        |> max()
        |> yield(name: \"mean\")",
        cluster,
        (end - Duration::minutes(1)).timestamp(),
        end.timestamp(),
    );

    let mut return_data: HashMap<Pubkey, i32> = HashMap::new();

    let client = reqwest::blocking::Client::new();

    let body = client
        .post(performance_db_url)
        .header("Authorization", format!("Token {}", performance_db_token))
        .header("Accept", "application/csv")
        .header("Content-type", "application/vnd.flux")
        .body(query)
        .send()?
        .text()?;

    let mut reader = csv::Reader::from_reader(body.as_bytes());

    for result in reader.records() {
        let record = result?;
        let optimistic_slot: i32 = record
            .get(6)
            .ok_or("Could not parse CSV record")?
            .parse()
            .unwrap();
        let pk = Pubkey::from_str(record.get(9).ok_or("Could not parse CSV record")?)?;
        return_data.insert(pk, optimistic_slot);
    }

    Ok(return_data)
}

/// Gets the modes for a list of integers, sorted ascending
fn get_modes(values: Vec<i32>) -> Vec<i32> {
    let mut counts: HashMap<i32, i32> = HashMap::new();

    for v in values.iter() {
        let counter = counts.entry(*v).or_insert(0);
        *counter += 1;
    }

    let max_count = *counts.values().max().unwrap();

    counts
        .into_iter()
        .filter(|(_k, v)| *v == max_count)
        .map(|(k, _v)| k)
        .sorted()
        .collect()
}
