use {
    log::*,
    reqwest::StatusCode,
    solana_client::{
        client_error,
        rpc_client::RpcClient,
        rpc_config::RpcSimulateTransactionConfig,
        rpc_request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
        rpc_response::{RpcVoteAccountInfo, RpcVoteAccountStatus},
    },
    solana_sdk::{
        clock::Epoch,
        native_token::*,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    std::{collections::HashMap, error, str::FromStr, thread::sleep, time::Duration},
};

pub fn retry_rpc_operation<T, F>(mut retries: usize, op: F) -> client_error::Result<T>
where
    F: Fn() -> client_error::Result<T>,
{
    loop {
        let result = op();

        if let Err(client_error::ClientError {
            kind: client_error::ClientErrorKind::Reqwest(ref reqwest_error),
            ..
        }) = result
        {
            let can_retry = reqwest_error.is_timeout()
                || reqwest_error
                    .status()
                    .map(|s| s == StatusCode::BAD_GATEWAY || s == StatusCode::GATEWAY_TIMEOUT)
                    .unwrap_or(false);
            if can_retry && retries > 0 {
                info!("RPC request timeout, {} retries remaining", retries);
                retries -= 1;
                continue;
            }
        }
        return result;
    }
}

/// Simulate a list of transactions and filter out the ones that will fail
#[allow(dead_code)]
pub fn simulate_transactions(
    rpc_client: &RpcClient,
    candidate_transactions: Vec<(Transaction, String)>,
) -> client_error::Result<Vec<(Transaction, String)>> {
    info!("Simulating {} transactions", candidate_transactions.len());
    let mut simulated_transactions = vec![];
    for (mut transaction, memo) in candidate_transactions {
        transaction.message.recent_blockhash =
            retry_rpc_operation(10, || rpc_client.get_recent_blockhash())?.0;

        let sim_result = rpc_client.simulate_transaction_with_config(
            &transaction,
            RpcSimulateTransactionConfig {
                sig_verify: false,
                ..RpcSimulateTransactionConfig::default()
            },
        )?;

        if sim_result.value.err.is_some() {
            warn!(
                "filtering out transaction due to simulation failure: {:?}: {}",
                sim_result, memo
            );
        } else {
            simulated_transactions.push((transaction, memo))
        }
    }
    info!(
        "Successfully simulating {} transactions",
        simulated_transactions.len()
    );
    Ok(simulated_transactions)
}

pub fn send_and_confirm_transactions(
    rpc_client: &RpcClient,
    dry_run: bool,
    transactions: Vec<(Transaction, String)>,
    authorized_staker: &Keypair,
    notifications: &mut Vec<String>,
) -> Result<bool, Box<dyn error::Error>> {
    let authorized_staker_balance = rpc_client.get_balance(&authorized_staker.pubkey())?;
    info!(
        "Authorized staker balance: {} SOL",
        lamports_to_sol(authorized_staker_balance)
    );

    let (blockhash, fee_calculator) = rpc_client.get_recent_blockhash()?;
    info!("{} transactions to send", transactions.len());

    let required_fee = transactions.iter().fold(0, |fee, (transaction, _)| {
        fee + fee_calculator.calculate_fee(&transaction.message)
    });
    info!("Required fee: {} SOL", lamports_to_sol(required_fee));
    if required_fee > authorized_staker_balance {
        return Err("Authorized staker has insufficient funds".into());
    }

    let mut pending_transactions = HashMap::new();
    for (mut transaction, memo) in transactions.into_iter() {
        transaction.sign(&[authorized_staker], blockhash);

        pending_transactions.insert(transaction.signatures[0], memo);
        if !dry_run {
            rpc_client.send_transaction(&transaction)?;
        }
    }

    struct ConfirmedTransaction {
        success: bool,
        signature: Signature,
        memo: String,
    }

    let mut confirmed_transactions = vec![];
    loop {
        if pending_transactions.is_empty() {
            break;
        }

        let blockhash_expired = rpc_client
            .get_fee_calculator_for_blockhash(&blockhash)?
            .is_none();
        if blockhash_expired {
            error!(
                "Blockhash {} expired with {} pending transactions",
                blockhash,
                pending_transactions.len()
            );

            for (signature, memo) in pending_transactions.into_iter() {
                confirmed_transactions.push(ConfirmedTransaction {
                    success: false,
                    signature,
                    memo,
                });
            }
            break;
        }

        let pending_signatures = pending_transactions.keys().cloned().collect::<Vec<_>>();
        let mut statuses = vec![];
        for pending_signatures_chunk in
            pending_signatures.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS - 1)
        {
            trace!(
                "checking {} pending_signatures",
                pending_signatures_chunk.len()
            );
            statuses.extend(
                rpc_client
                    .get_signature_statuses(&pending_signatures_chunk)?
                    .value
                    .into_iter(),
            )
        }
        assert_eq!(statuses.len(), pending_signatures.len());

        for (signature, status) in pending_signatures.into_iter().zip(statuses.into_iter()) {
            info!("{}: status={:?}", signature, status);
            let completed = if dry_run {
                Some(true)
            } else if let Some(status) = &status {
                if status.satisfies_commitment(rpc_client.commitment()) {
                    Some(status.err.is_none())
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(success) = completed {
                warn!("{}: completed.  success={}", signature, success);
                let memo = pending_transactions.remove(&signature).unwrap();
                confirmed_transactions.push(ConfirmedTransaction {
                    success,
                    signature,
                    memo,
                });
            }
        }
        sleep(Duration::from_millis(250));
    }

    confirmed_transactions.sort_by(|a, b| a.memo.cmp(&b.memo));

    let mut ok = true;

    for ConfirmedTransaction {
        success,
        signature,
        memo,
    } in confirmed_transactions
    {
        if success {
            info!("OK:   {}: {}", signature, memo);
            notifications.push(memo)
        } else {
            error!("FAIL: {}: {}", signature, memo);
            ok = false
        }
    }
    Ok(ok)
}

pub struct VoteAccountInfo {
    pub identity: Pubkey,
    pub vote_address: Pubkey,
    pub commission: u8,

    /// Credits earned in the epoch
    pub epoch_credits: u64,
}

pub fn get_vote_account_info(
    rpc_client: &RpcClient,
    epoch: Epoch,
) -> Result<Vec<VoteAccountInfo>, Box<dyn error::Error>> {
    let RpcVoteAccountStatus {
        current,
        delinquent,
    } = rpc_client.get_vote_accounts()?;

    let mut latest_vote_account_info = HashMap::<String, _>::new();

    for vote_account_info in current.into_iter().chain(delinquent.into_iter()) {
        let entry = latest_vote_account_info
            .entry(vote_account_info.node_pubkey.clone())
            .or_insert_with(|| vote_account_info.clone());

        // If the validator has multiple staked vote accounts then select the vote account that
        // voted most recently
        if entry.last_vote < vote_account_info.last_vote {
            *entry = vote_account_info.clone();
        }
    }

    Ok(latest_vote_account_info
        .values()
        .map(
            |RpcVoteAccountInfo {
                 commission,
                 node_pubkey,
                 vote_pubkey,
                 epoch_credits,
                 ..
             }| {
                let epoch_credits = if let Some((_last_epoch, credits, prev_credits)) =
                    epoch_credits.iter().find(|ec| ec.0 == epoch)
                {
                    credits.saturating_sub(*prev_credits)
                } else {
                    0
                };
                let identity = Pubkey::from_str(&node_pubkey).unwrap();
                let vote_address = Pubkey::from_str(&vote_pubkey).unwrap();

                VoteAccountInfo {
                    identity,
                    vote_address,
                    commission: *commission,
                    epoch_credits,
                }
            },
        )
        .collect())
}

#[cfg(test)]
pub mod test {
    use {
        super::*,
        indicatif::{ProgressBar, ProgressStyle},
        solana_sdk::{clock::Epoch, pubkey::Pubkey},
        solana_stake_program::{
            stake_instruction,
            stake_state::{Authorized, Lockup},
        },
        solana_vote_program::{vote_instruction, vote_state::VoteInit},
    };

    fn new_spinner_progress_bar() -> ProgressBar {
        let progress_bar = ProgressBar::new(42);
        progress_bar
            .set_style(ProgressStyle::default_spinner().template("{spinner:.green} {wide_msg}"));
        progress_bar.enable_steady_tick(100);
        progress_bar
    }

    pub fn wait_for_next_epoch(rpc_client: &RpcClient) -> client_error::Result<Epoch> {
        let current_epoch = rpc_client.get_epoch_info()?.epoch;

        let progress_bar = new_spinner_progress_bar();
        loop {
            let epoch_info = rpc_client.get_epoch_info()?;
            if epoch_info.epoch > current_epoch {
                return Ok(epoch_info.epoch);
            }
            progress_bar.set_message(&format!(
                "Waiting for epoch {} ({} slots remaining)",
                current_epoch + 1,
                epoch_info
                    .slots_in_epoch
                    .saturating_sub(epoch_info.slot_index),
            ));

            sleep(Duration::from_millis(200));
        }
    }

    pub fn create_vote_account(
        rpc_client: &RpcClient,
        payer: &Keypair,
        identity_keypair: &Keypair,
        vote_keypair: &Keypair,
    ) -> client_error::Result<()> {
        let mut transaction = Transaction::new_with_payer(
            &vote_instruction::create_account(
                &payer.pubkey(),
                &vote_keypair.pubkey(),
                &VoteInit {
                    node_pubkey: identity_keypair.pubkey(),
                    authorized_voter: identity_keypair.pubkey(),
                    authorized_withdrawer: identity_keypair.pubkey(),
                    commission: 10,
                },
                sol_to_lamports(1.),
            ),
            Some(&payer.pubkey()),
        );

        transaction.sign(
            &[payer, identity_keypair, vote_keypair],
            rpc_client.get_recent_blockhash()?.0,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }

    pub fn create_stake_account(
        rpc_client: &RpcClient,
        payer: &Keypair,
        amount: u64,
    ) -> client_error::Result<Keypair> {
        let stake_keypair = Keypair::new();
        let mut transaction = Transaction::new_with_payer(
            &stake_instruction::create_account(
                &payer.pubkey(),
                &stake_keypair.pubkey(),
                &Authorized::auto(&payer.pubkey()),
                &Lockup::default(),
                amount,
            ),
            Some(&payer.pubkey()),
        );

        transaction.sign(
            &[payer, &stake_keypair],
            rpc_client.get_recent_blockhash()?.0,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| stake_keypair)
    }

    pub struct ValidatorAddressPair {
        pub identity: Pubkey,
        pub vote_address: Pubkey,
    }

    pub fn create_validators(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        num_validators: usize,
    ) -> client_error::Result<Vec<ValidatorAddressPair>> {
        let mut validators = vec![];

        for _ in 0..num_validators {
            let identity_keypair = Keypair::new();
            let vote_keypair = Keypair::new();

            create_vote_account(
                &rpc_client,
                &authorized_staker,
                &identity_keypair,
                &vote_keypair,
            )?;

            validators.push(ValidatorAddressPair {
                identity: identity_keypair.pubkey(),
                vote_address: vote_keypair.pubkey(),
            });
        }

        Ok(validators)
    }
}
