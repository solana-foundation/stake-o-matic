use {
    log::*,
    reqwest::StatusCode,
    solana_client::{
        client_error, rpc_client::RpcClient, rpc_config::RpcSimulateTransactionConfig,
        rpc_request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
    },
    solana_sdk::{
        native_token::*,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    std::{collections::HashMap, error, thread::sleep, time::Duration},
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
        sleep(Duration::from_secs(5));
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
