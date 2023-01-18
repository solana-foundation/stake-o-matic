use solana_client::rpc_filter;
use std::borrow::Borrow;
use {
    indicatif::{ProgressBar, ProgressStyle},
    log::*,
    solana_client::{
        pubsub_client::PubsubClientError,
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig},
        rpc_request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
        rpc_response::{RpcVoteAccountInfo, RpcVoteAccountStatus},
        tpu_client::{TpuClient, TpuClientConfig, TpuSenderError},
    },
    solana_sdk::{
        clock::Epoch,
        pubkey::Pubkey,
        signature::Keypair,
        stake,
        transaction::{Transaction, TransactionError},
    },
    std::{
        collections::{HashMap, HashSet},
        error,
        str::FromStr,
        sync::Arc,
        thread::sleep,
        time::{Duration, Instant},
    },
};

fn new_spinner_progress_bar() -> ProgressBar {
    let progress_bar = ProgressBar::new(42);
    progress_bar
        .set_style(ProgressStyle::default_spinner().template("{spinner:.green} {wide_msg}"));
    progress_bar.enable_steady_tick(100);
    progress_bar
}

#[cfg(test)]
fn new_tpu_client_with_retry(
    rpc_client: &Arc<RpcClient>,
    websocket_url: &str,
) -> Result<TpuClient, TpuSenderError> {
    use solana_client::{
        client_error::{ClientError, ClientErrorKind},
        rpc_request::RpcError,
    };
    let mut retries = 128; // connecting with a 32-slot epoch can sometimes take awhile
    let sleep_ms = 200;
    while retries > 0 {
        match TpuClient::new(
            rpc_client.clone(),
            websocket_url,
            TpuClientConfig { fanout_slots: 1 },
        ) {
            // only retry on connection error or get slot leaders error
            Err(TpuSenderError::PubsubError(PubsubClientError::ConnectionError(_)))
            | Err(TpuSenderError::RpcError(ClientError {
                kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32602, .. }),
                ..
            })) => {
                warn!(
                    "Error creating Tpu Client, retrying in {}ms, {} retries remaining",
                    sleep_ms, retries
                );
                retries -= 1;
                sleep(Duration::from_millis(sleep_ms));
            }
            // everything else, Ok or Err, can pass through
            result => return result,
        }
    }
    // Let's force using the TpuClient for the tests
    Err(TpuSenderError::Custom(
        "Could not create TpuClient; time out".into(),
    ))
}

#[cfg(not(test))]
fn new_tpu_client_with_retry(
    rpc_client: &Arc<RpcClient>,
    websocket_url: &str,
) -> Result<TpuClient, TpuSenderError> {
    let mut retries = 5;
    let sleep_seconds = 5;
    while retries > 0 {
        match TpuClient::new(
            rpc_client.clone(),
            websocket_url,
            TpuClientConfig::default(),
        ) {
            // only retry on connection error
            Err(TpuSenderError::PubsubError(PubsubClientError::ConnectionError(_))) => {
                warn!(
                    "Error creating Tpu Client, retrying in {}s, {} retries remaining",
                    sleep_seconds, retries
                );
                retries -= 1;
                sleep(Duration::from_secs(sleep_seconds));
            }
            // everything else, Ok or Err, can pass through
            result => return result,
        }
    }
    TpuClient::new(
        rpc_client.clone(),
        websocket_url,
        TpuClientConfig::default(),
    )
}

pub fn send_and_confirm_transactions_with_spinner(
    rpc_client: Arc<RpcClient>,
    websocket_url: &str,
    dry_run: bool,
    transactions: Vec<Transaction>,
    signer: &Keypair,
) -> Result<Vec<Option<TransactionError>>, Box<dyn error::Error>> {
    if transactions.is_empty() {
        return Ok(vec![]);
    }
    let progress_bar = new_spinner_progress_bar();
    let mut expired_blockhash_retries = 100;
    let send_transaction_interval = Duration::from_millis(10); /* Send at ~100 TPS */
    let transaction_resend_interval = Duration::from_secs(4); /* Retry batch send after 4 seconds */

    progress_bar.set_message("Connecting...");
    let tpu_client = new_tpu_client_with_retry(&rpc_client, websocket_url)?;

    let mut transactions = transactions.into_iter().enumerate().collect::<Vec<_>>();
    let num_transactions = transactions.len() as f64;
    let mut transaction_errors = vec![None; transactions.len()];
    let set_message = |confirmed_transactions,
                       block_height: Option<u64>,
                       last_valid_block_height: u64,
                       status: &str| {
        progress_bar.set_message(format!(
            "{:>5.1}% | {:<40}{}",
            confirmed_transactions as f64 * 100. / num_transactions,
            status,
            match block_height {
                Some(block_height) => format!(
                    " [block height {}; re-sign in {} blocks]",
                    block_height,
                    last_valid_block_height.saturating_sub(block_height),
                ),
                None => String::new(),
            },
        ));
    };

    let mut confirmed_transactions = 0;
    let mut block_height = rpc_client.get_block_height()?;
    while expired_blockhash_retries > 0 {
        let blockhash = rpc_client.get_latest_blockhash()?;
        let last_valid_block_height = rpc_client.get_block_height()?;

        let mut pending_transactions = HashMap::new();
        for (i, mut transaction) in transactions {
            transaction.try_sign(&[signer], blockhash)?;
            pending_transactions.insert(transaction.signatures[0], (i, transaction));
        }

        let mut last_resend = Instant::now() - transaction_resend_interval;
        while block_height <= last_valid_block_height {
            let num_transactions = pending_transactions.len();

            // Periodically re-send all pending transactions
            if Instant::now().duration_since(last_resend) > transaction_resend_interval {
                for (index, (_i, transaction)) in pending_transactions.values().enumerate() {
                    let method = if dry_run {
                        "DRY RUN"
                    } else if tpu_client.send_transaction(transaction) {
                        "TPU"
                    } else {
                        let _ = rpc_client.send_transaction_with_config(
                            transaction,
                            RpcSendTransactionConfig {
                                skip_preflight: true,
                                ..RpcSendTransactionConfig::default()
                            },
                        );
                        "RPC"
                    };
                    set_message(
                        confirmed_transactions,
                        None, //block_height,
                        last_valid_block_height,
                        &format!(
                            "Sending {}/{} transactions (via {})",
                            index + 1,
                            num_transactions,
                            method
                        ),
                    );
                    sleep(send_transaction_interval);
                }
                last_resend = Instant::now();
            }

            // Wait for the next block before checking for transaction statuses
            set_message(
                confirmed_transactions,
                Some(block_height),
                last_valid_block_height,
                &format!("Waiting for next block, {} pending...", num_transactions),
            );

            let mut new_block_height = block_height;
            while block_height == new_block_height {
                sleep(Duration::from_millis(500));
                new_block_height = rpc_client.get_block_height()?;
            }
            block_height = new_block_height;
            if dry_run {
                return Ok(transaction_errors);
            }

            // Collect statuses for the transactions, drop those that are confirmed
            let pending_signatures = pending_transactions.keys().cloned().collect::<Vec<_>>();
            for pending_signatures_chunk in
                pending_signatures.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS)
            {
                match rpc_client.get_signature_statuses(pending_signatures_chunk) {
                    Ok(result) => {
                        let statuses = result.value;
                        for (signature, status) in
                            pending_signatures_chunk.iter().zip(statuses.into_iter())
                        {
                            if let Some(status) = status {
                                if status.satisfies_commitment(rpc_client.commitment()) {
                                    if let Some((i, _)) = pending_transactions.remove(signature) {
                                        confirmed_transactions += 1;
                                        if status.err.is_some() {
                                            progress_bar.println(format!(
                                                "Failed transaction {}: {:?}",
                                                signature, status
                                            ));
                                        }
                                        transaction_errors[i] = status.err;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Could not get signature statuses: {:?}", e);
                    }
                }

                set_message(
                    confirmed_transactions,
                    Some(block_height),
                    last_valid_block_height,
                    "Checking transaction status...",
                );
            }

            if pending_transactions.is_empty() {
                return Ok(transaction_errors);
            }
        }

        transactions = pending_transactions.into_iter().map(|(_k, v)| v).collect();
        progress_bar.println(format!(
            "Blockhash expired. {} retries remaining",
            expired_blockhash_retries
        ));
        expired_blockhash_retries -= 1;
    }
    Err("Max retries exceeded".into())
}

pub struct VoteAccountInfo {
    pub identity: Pubkey,
    pub vote_address: Pubkey,
    pub commission: u8,
    pub active_stake: u64,

    /// Credits earned in the epoch
    pub epoch_credits: u64,
}

pub fn get_vote_account_info(
    rpc_client: &RpcClient,
    epoch: Epoch,
) -> Result<(Vec<VoteAccountInfo>, u64), Box<dyn error::Error>> {
    let RpcVoteAccountStatus {
        current,
        delinquent,
    } = rpc_client.get_vote_accounts()?;

    let mut latest_vote_account_info = HashMap::<String, _>::new();

    let mut total_active_stake = 0;
    for vote_account_info in current.into_iter().chain(delinquent.into_iter()) {
        total_active_stake += vote_account_info.activated_stake;

        let entry = latest_vote_account_info
            .entry(vote_account_info.node_pubkey.clone())
            .or_insert_with(|| vote_account_info.clone());

        // If the validator has multiple staked vote accounts then select the vote account that
        // voted most recently
        if entry.last_vote < vote_account_info.last_vote {
            *entry = vote_account_info.clone();
        }
    }

    Ok((
        latest_vote_account_info
            .values()
            .filter_map(
                |RpcVoteAccountInfo {
                     commission,
                     node_pubkey,
                     vote_pubkey,
                     epoch_credits,
                     activated_stake,
                     ..
                 }| {
                    let credits_last_four_epochs: u64 = epoch_credits
                        .iter()
                        .filter_map(|(credit_epoch, credits, prev_credits)| {
                            if credit_epoch > (epoch - 4).borrow() {
                                Some(credits - prev_credits)
                            } else {
                                None
                            }
                        })
                        .sum();

                    if credits_last_four_epochs > 0 {
                        let epoch_credits = if let Some((_last_epoch, credits, prev_credits)) =
                            epoch_credits.iter().find(|ec| ec.0 == epoch)
                        {
                            credits.saturating_sub(*prev_credits)
                        } else {
                            0
                        };
                        let identity = Pubkey::from_str(node_pubkey).unwrap();
                        let vote_address = Pubkey::from_str(vote_pubkey).unwrap();

                        Some(VoteAccountInfo {
                            identity,
                            vote_address,
                            active_stake: *activated_stake,
                            commission: *commission,
                            epoch_credits,
                        })
                    } else {
                        None
                    }
                },
            )
            .collect(),
        total_active_stake,
    ))
}

pub fn get_all_stake(
    rpc_client: &RpcClient,
    authorized_staker: Pubkey,
) -> Result<(HashSet<Pubkey>, u64), Box<dyn error::Error>> {
    let mut all_stake_addresses = HashSet::new();
    let mut total_stake_balance = 0;

    let all_stake_accounts = rpc_client.get_program_accounts_with_config(
        &stake::program::id(),
        RpcProgramAccountsConfig {
            filters: Some(vec![rpc_filter::RpcFilterType::Memcmp(
                rpc_filter::Memcmp {
                    offset: 12,
                    bytes: rpc_filter::MemcmpEncodedBytes::Base58(authorized_staker.to_string()),
                    encoding: Some(rpc_filter::MemcmpEncoding::Binary),
                },
            )]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(rpc_client.commitment()),
                ..RpcAccountInfoConfig::default()
            },
            ..RpcProgramAccountsConfig::default()
        },
    )?;

    for (address, account) in all_stake_accounts {
        all_stake_addresses.insert(address);
        total_stake_balance += account.lamports;
    }

    Ok((all_stake_addresses, total_stake_balance))
}

pub fn check_rpc_health(rpc_client: &RpcClient) -> Result<(), Box<dyn error::Error>> {
    let mut retries = 12u8;
    let retry_delay = Duration::from_secs(10);
    loop {
        match rpc_client.get_health() {
            Ok(()) => {
                info!("RPC endpoint healthy");
                return Ok(());
            }
            Err(err) => {
                warn!("RPC endpoint is unhealthy: {:?}", err);
            }
        };
        if retries == 0 {
            return Err("Exhausted retries; connection to server is unhealthy".into());
        }
        retries = retries.saturating_sub(1);
        info!(
            "{} retries remaining, sleeping for {} seconds",
            retries,
            retry_delay.as_secs()
        );
        std::thread::sleep(retry_delay);
    }
}

#[cfg(test)]
pub mod test {
    use {
        super::*,
        borsh::BorshSerialize,
        solana_client::client_error,
        solana_sdk::{
            borsh::get_packed_len,
            clock::Epoch,
            native_token,
            program_pack::Pack,
            pubkey::Pubkey,
            signature::Signer,
            stake::{
                instruction as stake_instruction,
                state::{Authorized, Lockup},
            },
            system_instruction,
        },
        solana_vote_program::{vote_instruction, vote_state::VoteInit},
        spl_stake_pool::{
            find_stake_program_address, find_withdraw_authority_program_address,
            state::{Fee, StakePool, ValidatorList},
        },
        spl_token::state::{Account, Mint},
    };

    pub fn wait_for_next_epoch(rpc_client: &RpcClient) -> client_error::Result<Epoch> {
        let current_epoch = rpc_client.get_epoch_info()?.epoch;

        let progress_bar = new_spinner_progress_bar();
        loop {
            let epoch_info = rpc_client.get_epoch_info()?;
            if epoch_info.epoch > current_epoch {
                return Ok(epoch_info.epoch);
            }
            progress_bar.set_message(format!(
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
                native_token::sol_to_lamports(1.),
            ),
            Some(&payer.pubkey()),
        );

        transaction.sign(
            &[payer, identity_keypair, vote_keypair],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }

    pub fn create_stake_account(
        rpc_client: &RpcClient,
        payer: &Keypair,
        authority: &Pubkey,
        amount: u64,
    ) -> client_error::Result<Keypair> {
        let stake_keypair = Keypair::new();
        let mut transaction = Transaction::new_with_payer(
            &stake_instruction::create_account(
                &payer.pubkey(),
                &stake_keypair.pubkey(),
                &Authorized::auto(authority),
                &Lockup::default(),
                amount,
            ),
            Some(&payer.pubkey()),
        );

        transaction.sign(&[payer, &stake_keypair], rpc_client.get_latest_blockhash()?);
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| stake_keypair)
    }

    pub fn delegate_stake(
        rpc_client: &RpcClient,
        authority: &Keypair,
        stake_address: &Pubkey,
        vote_address: &Pubkey,
    ) -> client_error::Result<()> {
        let transaction = Transaction::new_signed_with_payer(
            &[stake_instruction::delegate_stake(
                stake_address,
                &authority.pubkey(),
                vote_address,
            )],
            Some(&authority.pubkey()),
            &[authority],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }

    pub struct ValidatorAddressPair {
        pub identity: Pubkey,
        pub vote_address: Pubkey,
    }

    pub fn create_validators(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        num_validators: u32,
    ) -> client_error::Result<Vec<ValidatorAddressPair>> {
        let mut validators = vec![];

        for _ in 0..num_validators {
            let identity_keypair = Keypair::new();
            let vote_keypair = Keypair::new();

            create_vote_account(
                rpc_client,
                authorized_staker,
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

    pub fn create_mint(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        manager: &Pubkey,
    ) -> client_error::Result<Pubkey> {
        let mint_rent = rpc_client.get_minimum_balance_for_rent_exemption(Mint::LEN)?;
        let mint_keypair = Keypair::new();

        let mut transaction = Transaction::new_with_payer(
            &[
                system_instruction::create_account(
                    &authorized_staker.pubkey(),
                    &mint_keypair.pubkey(),
                    mint_rent,
                    Mint::LEN as u64,
                    &spl_token::id(),
                ),
                spl_token::instruction::initialize_mint(
                    &spl_token::id(),
                    &mint_keypair.pubkey(),
                    manager,
                    None,
                    0,
                )
                .unwrap(),
            ],
            Some(&authorized_staker.pubkey()),
        );

        transaction.sign(
            &[authorized_staker, &mint_keypair],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| mint_keypair.pubkey())
    }

    pub fn create_token_account(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        mint: &Pubkey,
        owner: &Pubkey,
    ) -> client_error::Result<Pubkey> {
        let account_rent = rpc_client.get_minimum_balance_for_rent_exemption(Account::LEN)?;
        let account_keypair = Keypair::new();

        let mut transaction = Transaction::new_with_payer(
            &[
                system_instruction::create_account(
                    &authorized_staker.pubkey(),
                    &account_keypair.pubkey(),
                    account_rent,
                    Account::LEN as u64,
                    &spl_token::id(),
                ),
                spl_token::instruction::initialize_account(
                    &spl_token::id(),
                    &account_keypair.pubkey(),
                    mint,
                    owner,
                )
                .unwrap(),
            ],
            Some(&authorized_staker.pubkey()),
        );

        transaction.sign(
            &[authorized_staker, &account_keypair],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| account_keypair.pubkey())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_stake_pool(
        rpc_client: &RpcClient,
        payer: &Keypair,
        stake_pool: &Keypair,
        reserve_stake: &Pubkey,
        pool_mint: &Pubkey,
        pool_token_account: &Pubkey,
        manager: &Keypair,
        staker: &Pubkey,
        max_validators: u32,
        stake_pool_withdraw_authority: &Pubkey,
    ) -> client_error::Result<()> {
        let stake_pool_size = get_packed_len::<StakePool>();
        let stake_pool_rent = rpc_client
            .get_minimum_balance_for_rent_exemption(stake_pool_size)
            .unwrap();
        let validator_list = ValidatorList::new(max_validators);
        let validator_list_size = validator_list.try_to_vec().unwrap().len();
        let validator_list_rent = rpc_client
            .get_minimum_balance_for_rent_exemption(validator_list_size)
            .unwrap();
        let validator_list = Keypair::new();
        let fee = Fee {
            numerator: 10,
            denominator: 100,
        };

        let mut transaction = Transaction::new_with_payer(
            &[
                system_instruction::create_account(
                    &payer.pubkey(),
                    &stake_pool.pubkey(),
                    stake_pool_rent,
                    stake_pool_size as u64,
                    &spl_stake_pool::id(),
                ),
                system_instruction::create_account(
                    &payer.pubkey(),
                    &validator_list.pubkey(),
                    validator_list_rent,
                    validator_list_size as u64,
                    &spl_stake_pool::id(),
                ),
                spl_stake_pool::instruction::initialize(
                    &spl_stake_pool::id(),
                    &stake_pool.pubkey(),
                    &manager.pubkey(),
                    staker,
                    stake_pool_withdraw_authority,
                    &validator_list.pubkey(),
                    reserve_stake,
                    pool_mint,
                    pool_token_account,
                    &spl_token::id(),
                    /* deposit_authority = */ None,
                    fee,
                    fee,
                    fee,
                    /* referral_fee = */ 10u8,
                    max_validators,
                ),
            ],
            Some(&payer.pubkey()),
        );
        transaction.sign(
            &[payer, stake_pool, &validator_list, manager],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }

    pub fn deposit_stake_into_stake_pool(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        stake_pool_address: &Pubkey,
        stake_pool: &StakePool,
        vote_address: &Pubkey,
        stake_address: &Pubkey,
        pool_token_address: &Pubkey,
    ) -> client_error::Result<()> {
        let validator_stake_address =
            find_stake_program_address(&spl_stake_pool::id(), vote_address, stake_pool_address).0;
        let pool_withdraw_authority =
            find_withdraw_authority_program_address(&spl_stake_pool::id(), stake_pool_address).0;
        let transaction = Transaction::new_signed_with_payer(
            &spl_stake_pool::instruction::deposit_stake(
                &spl_stake_pool::id(),
                stake_pool_address,
                &stake_pool.validator_list,
                &pool_withdraw_authority,
                stake_address,
                &authorized_staker.pubkey(),
                &validator_stake_address,
                &stake_pool.reserve_stake,
                pool_token_address,
                &stake_pool.manager_fee_account,
                pool_token_address,
                &stake_pool.pool_mint,
                &spl_token::id(),
            ),
            Some(&authorized_staker.pubkey()),
            &[authorized_staker],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }

    pub fn deposit_sol_into_stake_pool(
        rpc_client: &RpcClient,
        authorized_staker: &Keypair,
        stake_pool_address: &Pubkey,
        stake_pool: &StakePool,
        pool_token_address: &Pubkey,
        lamports: u64,
    ) -> client_error::Result<()> {
        let pool_withdraw_authority =
            find_withdraw_authority_program_address(&spl_stake_pool::id(), stake_pool_address).0;
        let transaction = Transaction::new_signed_with_payer(
            &[spl_stake_pool::instruction::deposit_sol(
                &spl_stake_pool::id(),
                stake_pool_address,
                &pool_withdraw_authority,
                &stake_pool.reserve_stake,
                &authorized_staker.pubkey(),
                pool_token_address,
                &stake_pool.manager_fee_account,
                pool_token_address,
                &stake_pool.pool_mint,
                &spl_token::id(),
                lamports,
            )],
            Some(&authorized_staker.pubkey()),
            &[authorized_staker],
            rpc_client.get_latest_blockhash()?,
        );
        rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .map(|_| ())
    }
}
