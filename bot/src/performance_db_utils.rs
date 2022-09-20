use crate::Cluster::{MainnetBeta, Testnet};
use crate::{Cluster, Epoch, Pubkey, ValidatorList};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use itertools::Itertools;
use log::{debug, info, trace};
use solana_client::client_error::ClientErrorKind;
use solana_client::rpc_client::RpcClient;
use solana_foundation_delegation_program_registry::state::Participant;
use solana_sdk::clock::DEFAULT_SLOTS_PER_EPOCH;
use std::collections::HashMap;
use std::str::FromStr;

/// Validators must have reported within OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT of the mode optimistic slot in at least NUM_SUCCESSFUL_REPORTING_SAMPLES
/// out of every NUM_REPORTING_SAMPLES samples
pub const NUM_SAMPLED_REPORTING_EPOCHS: f32 = 10.0;
pub const NUM_SUCCESSFUL_REPORTING_SAMPLES: f32 = 8.0;
pub const SUCCESS_MIN_PERCENT: f32 =
    NUM_SUCCESSFUL_REPORTING_SAMPLES / NUM_SAMPLED_REPORTING_EPOCHS;
const OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT: u32 = 128;

type GetKeyFromParticipant = fn(&Participant) -> Pubkey;

pub fn get_testnet_pk_from_participant(participant: &Participant) -> Pubkey {
    participant.testnet_identity
}

pub fn get_mainnet_pk_from_participant(participant: &Participant) -> Pubkey {
    participant.mainnet_identity
}

/// Checks if validators have been reporting their optimistic slots regularly and accurately.
/// Returns a map of validator keys -> (passsed, reason)
pub fn get_reported_performance_metrics(
    performance_db_url: &String,
    performance_db_token: &String,
    cluster: &Cluster,
    rpc_client: &RpcClient,
    epoch: &Epoch,
    all_participants: &HashMap<Pubkey, Participant>,
) -> Result<HashMap<Pubkey, (bool, String)>, Box<dyn std::error::Error>> {
    if !(cluster == &MainnetBeta || cluster == &Testnet) {
        return Err(
            "get_reported_performance_metrics() only works for clusters Testnet and MainnetBeta"
                .into(),
        );
    };

    let (get_pk_from_this_cluster, get_pk_from_other_cluster): (
        GetKeyFromParticipant,
        GetKeyFromParticipant,
    ) = if cluster == &MainnetBeta {
        (
            get_mainnet_pk_from_participant,
            get_testnet_pk_from_participant,
        )
    } else {
        (
            get_testnet_pk_from_participant,
            get_mainnet_pk_from_participant,
        )
    };

    let other_cluster_validators: ValidatorList = all_participants
        .iter()
        .map(|(_k, p)| get_pk_from_other_cluster(p))
        .collect();

    let reporters = find_reporters_for_epoch(
        performance_db_url,
        performance_db_token,
        cluster,
        epoch,
        rpc_client,
    )?;

    let performance_reports: HashMap<Pubkey, (bool, String)> = reporters
        .iter()
        .map(|(pubkey, _v)| {
            // if the validator is from the other cluster, destake
            if other_cluster_validators.contains(pubkey) {
                (
                    *pubkey,
                    (
                        false,
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
                    ),
                )
            } else {
                (
                    *pubkey,
                    (true, format!("Reported successfully in epoch {:?}", epoch)),
                )
            }
        })
        .collect();

    let all_reports: HashMap<Pubkey, (bool, String)> = all_participants
        .iter()
        .map(|(_pk, participant)| {
            let validator_key = get_pk_from_this_cluster(participant);
            match performance_reports.get(&validator_key) {
                Some(report) => (validator_key, report.clone()),
                None => (
                    validator_key,
                    (
                        false,
                        format!("No report from validator for epoch {:?}", epoch),
                    ),
                ),
            }
        })
        .collect();

    Ok(all_reports)
}

/// Get a list of validators who reported during the specified epoch
fn find_reporters_for_epoch(
    performance_db_url: &String,
    performance_db_token: &String,
    cluster: &Cluster,
    epoch: &Epoch,
    rpc_client: &RpcClient,
) -> Result<HashMap<Pubkey, bool>, Box<dyn std::error::Error>> {
    // List of validators and whether they reported correctly at least once
    let mut reporters: HashMap<Pubkey, bool> = HashMap::new();

    // To check if a validator has been reporting during an epoch, we take four samples from the epoch at 0%, 25%, 50%,
    // and 75%, and if the validator reported correctly during any one  of the sample periodsd, the validator passes.
    for n in 0..4 {
        let slot_to_try: i64 = match cluster {
            MainnetBeta => (DEFAULT_SLOTS_PER_EPOCH as i64) * (*epoch as i64),
            // Testnet epoch boundaries aren't on multiples of DEFAULT_SLOTS_PER_EPOCH for some reason
            // Epoch 341 starts at slot 141788256; use that as our anchor.
            // Testnet => (141_788_256 as u64).wrapping_add( DEFAULT_SLOTS_PER_EPOCH.wrapping_mul((341 as u64).wrapping_sub(*epoch)))
            Testnet => 141_788_256 + (DEFAULT_SLOTS_PER_EPOCH as i64) * ((*epoch as i64) - 341),
        } + n * (DEFAULT_SLOTS_PER_EPOCH as i64) / 4;

        let slot_time = match get_slot_time(slot_to_try as u64, rpc_client) {
            Ok(st) => st,
            Err(_e) => {
                debug!("Could not find slot time for {:?}", slot_to_try);
                continue;
            }
        };

        debug!("Found a time: {:?}", slot_time);

        let reported_data =
            fetch_data(performance_db_url, performance_db_token, cluster, slot_time)?;

        if reported_data.is_empty() {
            info!("No records found for time {:?}", slot_time);
            continue;
        }

        let optimistic_slot_modes = get_modes(reported_data.values().cloned().collect());
        let min_optimistic_slot = optimistic_slot_modes
            .first()
            .ok_or("Could not get mode of optimistic slots")?
            - OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT as i32;
        let max_optimistic_slot = optimistic_slot_modes
            .last()
            .ok_or("Could not get mode of optimistic slots")?
            + OPTIMISTIC_SLOT_MODE_DEVIATION_AMUONT as i32;

        reported_data.iter().for_each(|(k, v)| {
            if v >= &min_optimistic_slot && v <= &max_optimistic_slot {
                reporters.insert(*k, true);
            } else {
                reporters.entry(*k).or_insert(false);
            }
        })
    }

    if reporters.is_empty() {
        Err(format!("Could get no slot times for {:?}/{:?}", cluster, epoch).into())
    } else {
        Ok(reporters)
    }
}

/// Fetch self-reported data from the InfluxDB database for the time range (date_time -- date_time + 1 minute)
/// Validators that are reporting tend to report more than once a second, so if there are no reports
/// from a validator in a minute, we can assume they are under-reporting.
fn fetch_data(
    performance_db_url: &String,
    performance_db_token: &String,
    cluster: &Cluster,
    date_time: DateTime<Utc>,
) -> Result<HashMap<Pubkey, i32>, Box<dyn std::error::Error>> {
    let query = format!(
        "from(bucket:\"{}\")
        |> range(start: {}, stop: {})
        |> filter(fn: (r) => r[\"_measurement\"] == \"optimistic_slot\" and r[\"_field\"] == \"slot\")
        |> group(columns: [\"host_id\"])
        |> max(column: \"_time\")
        |> keep(columns: [\"host_id\", \"_value\"])
        |> group()",
        cluster,
        date_time.timestamp(),
        (date_time + ChronoDuration::minutes(1)).timestamp(),
    );

    debug!("QUERY: {:?}", query);

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

    let mut reader = csv::ReaderBuilder::new().from_reader(body.as_bytes());

    for result in reader.records() {
        let record = result?;
        let optimistic_slot: i32 = record
            .get(3)
            .ok_or("Could not parse CSV record")?
            .parse()
            .unwrap();
        let pk = Pubkey::from_str(record.get(4).ok_or("Could not parse CSV record")?)?;
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

/// gets the time of the slot in the epoch. If there was no block for the slot, checks the next slot
fn get_slot_time(
    slot: u64,
    rpc_client: &RpcClient,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let mut idx = 0;
    loop {
        // try 200 times
        if idx > 200 {
            return Err(format!("Could not get slot time for slot {:?}", slot).into());
        }
        let slot_to_try = slot + idx;

        debug!("Trying get_block_time for slot {:?}", slot_to_try);

        let time_stamp = rpc_client.get_block_time(slot_to_try);

        match time_stamp {
            Ok(time) => {
                return Ok(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(time, 0),
                    Utc,
                ));
            }
            Err(e) => match e.kind() {
                ClientErrorKind::RpcError(_) => {
                    // Not documented, but if the error is RpcError, there was no block for the slot, so we should try another slot
                    trace!("No time found for slot {}", slot + idx);
                    idx += 1;
                }
                _ => {
                    return Err("Unknown error".into());
                }
            },
        }
    }
}
