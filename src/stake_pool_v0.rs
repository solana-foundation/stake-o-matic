use {
    crate::{generic_stake_pool::*, rpc_client_utils::send_and_confirm_transactions},
    log::*,
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter,
        rpc_response::StakeActivationState,
    },
    solana_sdk::{
        account::Account,
        native_token::{Sol, LAMPORTS_PER_SOL},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_stake_program::{stake_instruction, stake_state::StakeState},
    std::{collections::HashSet, error},
};

// Minimum amount of lamports in a stake pool account
pub const MIN_STAKE_ACCOUNT_BALANCE: u64 = LAMPORTS_PER_SOL;

// Don't bother adjusting stake if less than this amount of lamports will be affected
// (must be >= MIN_STAKE_ACCOUNT_BALANCE)
const MIN_STAKE_CHANGE_AMOUNT: u64 = MIN_STAKE_ACCOUNT_BALANCE;

#[derive(Debug)]
pub struct StakePool {
    authorized_staker: Keypair,
    baseline_stake_amount: u64,
    reserve_stake_address: Pubkey,
    min_reserve_stake_balance: u64,
}

pub fn new(
    _rpc_client: &RpcClient,
    authorized_staker: Keypair,
    baseline_stake_amount: u64,
    reserve_stake_address: Pubkey,
    min_reserve_stake_balance: u64,
) -> Result<StakePool, Box<dyn error::Error>> {
    if baseline_stake_amount < MIN_STAKE_CHANGE_AMOUNT {
        return Err(format!(
            "baseline stake amount too small: {}",
            Sol(baseline_stake_amount)
        )
        .into());
    }

    if min_reserve_stake_balance < MIN_STAKE_ACCOUNT_BALANCE {
        return Err(format!(
            "minimum reserve stake balance is too small: {}",
            Sol(min_reserve_stake_balance)
        )
        .into());
    }

    Ok(StakePool {
        authorized_staker,
        baseline_stake_amount,
        reserve_stake_address,
        min_reserve_stake_balance,
    })
}

fn validator_stake_address_seed(vote_address: Pubkey) -> String {
    format!("S{}", vote_address)[..32].to_string()
}

fn validator_transient_stake_address_seed(vote_address: Pubkey) -> String {
    format!("T{}", vote_address)[..32].to_string()
}

fn validator_stake_address(authorized_staker: Pubkey, vote_address: Pubkey) -> Pubkey {
    Pubkey::create_with_seed(
        &authorized_staker,
        &validator_stake_address_seed(vote_address),
        &solana_stake_program::id(),
    )
    .unwrap()
}

fn validator_transient_stake_address(authorized_staker: Pubkey, vote_address: Pubkey) -> Pubkey {
    Pubkey::create_with_seed(
        &authorized_staker,
        &validator_transient_stake_address_seed(vote_address),
        &solana_stake_program::id(),
    )
    .unwrap()
}

impl GenericStakePool for StakePool {
    fn apply(
        &mut self,
        rpc_client: &RpcClient,
        dry_run: bool,
        desired_validator_stake: &[ValidatorStake],
    ) -> Result<(Vec<String>, bool), Box<dyn error::Error>> {
        if dry_run {
            return Err("dryrun not supported".into());
        }

        let mut inuse_stake_addresses = HashSet::new();
        inuse_stake_addresses.insert(self.reserve_stake_address);

        let mut min_stake_node_count = 0;
        let mut bonus_stake_node_count = 0;
        let mut baseline_stake_node_count = 0;

        for ValidatorStake {
            vote_address,
            stake_state,
            ..
        } in desired_validator_stake
        {
            let stake_address =
                validator_stake_address(self.authorized_staker.pubkey(), *vote_address);
            let transient_stake_address =
                validator_transient_stake_address(self.authorized_staker.pubkey(), *vote_address);

            inuse_stake_addresses.insert(stake_address);
            inuse_stake_addresses.insert(transient_stake_address);

            match stake_state {
                ValidatorStakeState::No => min_stake_node_count += 1,
                ValidatorStakeState::Bonus => bonus_stake_node_count += 1,
                ValidatorStakeState::Baseline => baseline_stake_node_count += 1,
            }
        }

        let (all_stake_addresses, all_stake_total_amount) =
            get_all_stake(rpc_client, self.authorized_staker.pubkey())?;

        info!("Merge orphaned stake into the reserve");
        merge_orphaned_stake_accounts(
            rpc_client,
            &self.authorized_staker,
            &all_stake_addresses - &inuse_stake_addresses,
            self.reserve_stake_address,
        )?;

        info!("Merge transient stake back into either the reserve or validator stake");
        let mut busy_validators = HashSet::new();
        merge_transient_stake_accounts(
            rpc_client,
            &self.authorized_staker,
            desired_validator_stake,
            self.reserve_stake_address,
            &mut busy_validators,
        )?;

        info!("Create validator stake accounts if needed");
        create_validator_stake_accounts(
            rpc_client,
            &self.authorized_staker,
            desired_validator_stake,
            self.reserve_stake_address,
            self.min_reserve_stake_balance,
            &mut busy_validators,
        )?;

        // `total_stake_amount` excludes the amount that always remains in the reserve account
        let total_stake_amount = all_stake_total_amount - self.min_reserve_stake_balance;

        info!("Total stake pool balance: {}", Sol(total_stake_amount));

        let total_min_stake_amount = min_stake_node_count * MIN_STAKE_ACCOUNT_BALANCE;
        info!("Min node count: {}", min_stake_node_count);
        info!("Min stake amount: {}", Sol(total_min_stake_amount));

        let total_baseline_stake_amount = baseline_stake_node_count * self.baseline_stake_amount;
        info!("Baseline node count: {}", baseline_stake_node_count);
        info!("Baseline stake amount: {}", Sol(self.baseline_stake_amount));
        info!(
            "Total baseline stake amount: {}",
            Sol(total_baseline_stake_amount)
        );

        if total_stake_amount < total_baseline_stake_amount {
            return Err("Not enough stake to cover the baseline".into());
        }

        info!("Bonus node count: {}", bonus_stake_node_count);
        let total_bonus_stake_amount =
            total_stake_amount.saturating_sub(total_min_stake_amount + total_baseline_stake_amount);
        info!(
            "Total bonus stake amount: {}",
            Sol(total_bonus_stake_amount)
        );

        let bonus_stake_amount = if bonus_stake_node_count == 0 {
            0
        } else {
            total_bonus_stake_amount / (bonus_stake_node_count as u64)
        };

        info!("Bonus stake amount: {}", Sol(bonus_stake_amount));

        let notes = vec![
            format!("Baseline stake amount: {}", Sol(self.baseline_stake_amount)),
            format!("Bonus stake amount: {}", Sol(bonus_stake_amount)),
        ];
        Ok((
            notes,
            distribute_validator_stake(
                rpc_client,
                &self.authorized_staker,
                desired_validator_stake
                    .iter()
                    .filter(|vs| !busy_validators.contains(&vs.identity))
                    .cloned(),
                self.reserve_stake_address,
                self.min_reserve_stake_balance,
                self.baseline_stake_amount,
                bonus_stake_amount,
            )?,
        ))
    }
}

// Get the balance of a stake account excluding the reserve
fn get_available_stake_balance(
    rpc_client: &RpcClient,
    stake_address: Pubkey,
    reserve_stake_balance: u64,
) -> Result<u64, Box<dyn error::Error>> {
    let balance = rpc_client.get_balance(&stake_address).map_err(|err| {
        format!(
            "Unable to get stake account balance: {}: {}",
            stake_address, err
        )
    })?;
    if balance < reserve_stake_balance {
        Err(format!(
            "Stake account {} balance too low, {}. Minimum is {}",
            stake_address,
            Sol(balance),
            Sol(reserve_stake_balance)
        )
        .into())
    } else {
        Ok(balance.saturating_sub(reserve_stake_balance))
    }
}

fn get_all_stake(
    rpc_client: &RpcClient,
    authorized_staker: Pubkey,
) -> Result<(HashSet<Pubkey>, u64), Box<dyn error::Error>> {
    let mut all_stake_addresses = HashSet::new();
    let mut total_stake_balance = 0;

    let all_stake_accounts = rpc_client.get_program_accounts_with_config(
        &solana_stake_program::id(),
        RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by `Meta::authorized::staker`, which begins at byte offset 12
                rpc_filter::RpcFilterType::Memcmp(rpc_filter::Memcmp {
                    offset: 12,
                    bytes: rpc_filter::MemcmpEncodedBytes::Binary(authorized_staker.to_string()),
                    encoding: Some(rpc_filter::MemcmpEncoding::Binary),
                }),
            ]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(rpc_client.commitment()),
                ..RpcAccountInfoConfig::default()
            },
        },
    )?;

    for (address, account) in all_stake_accounts {
        all_stake_addresses.insert(address);
        total_stake_balance += account.lamports;
    }

    Ok((all_stake_addresses, total_stake_balance))
}

fn merge_orphaned_stake_accounts(
    rpc_client: &RpcClient,
    authorized_staker: &Keypair,
    source_stake_addresses: HashSet<Pubkey>,
    reserve_stake_address: Pubkey,
) -> Result<(), Box<dyn error::Error>> {
    let mut transactions = vec![];
    for stake_address in source_stake_addresses {
        let stake_activation = rpc_client
            .get_stake_activation(stake_address, None)
            .map_err(|err| {
                format!(
                    "Unable to get stake activation for {}: {}",
                    stake_address, err
                )
            })?;

        match stake_activation.state {
            StakeActivationState::Activating | StakeActivationState::Deactivating => {}
            StakeActivationState::Active => {
                transactions.push(Transaction::new_with_payer(
                    &[stake_instruction::deactivate_stake(
                        &stake_address,
                        &authorized_staker.pubkey(),
                    )],
                    Some(&authorized_staker.pubkey()),
                ));
                info!("Deactivating stake {}", stake_address);
            }
            StakeActivationState::Inactive => {
                transactions.push(Transaction::new_with_payer(
                    &stake_instruction::merge(
                        &reserve_stake_address,
                        &stake_address,
                        &authorized_staker.pubkey(),
                    ),
                    Some(&authorized_staker.pubkey()),
                ));

                info!(
                    "Merging orphaned stake, {}, into reserve {}",
                    stake_address, reserve_stake_address
                );
            }
        }
    }

    if !send_and_confirm_transactions(rpc_client, false, transactions, authorized_staker)?
        .failed
        .is_empty()
    {
        Err("Failed to merge orphaned stake accounts".into())
    } else {
        Ok(())
    }
}

fn merge_transient_stake_accounts(
    rpc_client: &RpcClient,
    authorized_staker: &Keypair,
    desired_validator_stake: &[ValidatorStake],
    reserve_stake_address: Pubkey,
    busy_validators: &mut HashSet<Pubkey>,
) -> Result<(), Box<dyn error::Error>> {
    let mut transactions = vec![];
    for ValidatorStake {
        identity,
        vote_address,
        ..
    } in desired_validator_stake
    {
        let stake_address = validator_stake_address(authorized_staker.pubkey(), *vote_address);
        let transient_stake_address =
            validator_transient_stake_address(authorized_staker.pubkey(), *vote_address);

        let transient_stake_account = rpc_client
            .get_account_with_commitment(&transient_stake_address, rpc_client.commitment())?
            .value;

        if let Some(transient_stake_account) = transient_stake_account {
            let transient_stake_activation = rpc_client
                .get_stake_activation(transient_stake_address, None)
                .map_err(|err| {
                    format!(
                        "Unable to get activation information for transient stake account: {}: {}",
                        transient_stake_address, err
                    )
                })?;

            match transient_stake_activation.state {
                StakeActivationState::Activating | StakeActivationState::Deactivating => {
                    warn!(
                        "Validator {} busy due to transient stake activation/deactivation of {}: {:?}",
                        identity,
                        transient_stake_address,
                        transient_stake_activation,
                    );
                    busy_validators.insert(*identity);
                }
                StakeActivationState::Active => {
                    let stake_account = rpc_client
                        .get_account_with_commitment(&stake_address, rpc_client.commitment())?
                        .value
                        .unwrap_or_default();

                    if stake_accounts_have_same_credits_observed(
                        &stake_account,
                        &transient_stake_account,
                    )? {
                        transactions.push(Transaction::new_with_payer(
                            &stake_instruction::merge(
                                &stake_address,
                                &transient_stake_address,
                                &authorized_staker.pubkey(),
                            ),
                            Some(&authorized_staker.pubkey()),
                        ));
                        info!("Merging active transient stake for {}", identity);
                    } else {
                        warn!(
                                "Unable to merge active transient stake for {} due to credits observed mismatch",
                                identity
                            );
                        busy_validators.insert(*identity);
                    }
                }
                StakeActivationState::Inactive => {
                    transactions.push(Transaction::new_with_payer(
                        &stake_instruction::merge(
                            &reserve_stake_address,
                            &transient_stake_address,
                            &authorized_staker.pubkey(),
                        ),
                        Some(&authorized_staker.pubkey()),
                    ));
                    info!("Merging inactive transient stake for {}", identity);
                }
            }
        }
    }

    if !send_and_confirm_transactions(rpc_client, false, transactions, authorized_staker)?
        .failed
        .is_empty()
    {
        Err("Failed to merge transient stake".into())
    } else {
        Ok(())
    }
}

fn stake_accounts_have_same_credits_observed(
    stake_account1: &Account,
    stake_account2: &Account,
) -> Result<bool, Box<dyn error::Error>> {
    use solana_stake_program::stake_state::Stake;

    let stake_state1 = bincode::deserialize(stake_account1.data.as_slice())
        .map_err(|err| format!("Invalid stake account 1: {}", err))?;
    let stake_state2 = bincode::deserialize(stake_account2.data.as_slice())
        .map_err(|err| format!("Invalid stake account 2: {}", err))?;

    if let (
        StakeState::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed1,
            },
        ),
        StakeState::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed2,
            },
        ),
    ) = (stake_state1, stake_state2)
    {
        return Ok(credits_observed1 == credits_observed2);
    }
    Ok(false)
}

fn create_validator_stake_accounts(
    rpc_client: &RpcClient,
    authorized_staker: &Keypair,
    desired_validator_stake: &[ValidatorStake],
    reserve_stake_address: Pubkey,
    min_reserve_stake_balance: u64,
    busy_validators: &mut HashSet<Pubkey>,
) -> Result<(), Box<dyn error::Error>> {
    let mut reserve_stake_balance =
        get_available_stake_balance(rpc_client, reserve_stake_address, min_reserve_stake_balance)
            .map_err(|err| {
            format!(
                "Unable to get reserve stake account balance: {}: {}",
                reserve_stake_address, err
            )
        })?;
    info!(
        "Reserve stake available balance: {}",
        Sol(reserve_stake_balance)
    );

    let mut transactions = vec![];
    for ValidatorStake {
        identity,
        vote_address,
        ..
    } in desired_validator_stake
    {
        let stake_address = validator_stake_address(authorized_staker.pubkey(), *vote_address);
        let stake_account = rpc_client
            .get_account_with_commitment(&stake_address, rpc_client.commitment())?
            .value;

        if stake_account.is_some() {
            // Check if the stake account is busy
            let stake_activation = rpc_client
                .get_stake_activation(stake_address, None)
                .map_err(|err| {
                    format!(
                        "Unable to get activation information for stake account: {}: {}",
                        stake_address, err
                    )
                })?;

            if stake_activation.state != StakeActivationState::Active {
                warn!(
                    "Validator {} busy due to stake activation of {}: {:?}",
                    identity, stake_address, stake_activation
                );
                busy_validators.insert(*identity);
            }
        } else {
            if reserve_stake_balance < MIN_STAKE_ACCOUNT_BALANCE {
                // Try again next epoch
                warn!(
                    "Insufficient funds in reserve stake account to create stake account: {} required, {} balance",
                    Sol(MIN_STAKE_ACCOUNT_BALANCE), Sol(reserve_stake_balance)
                );
            } else {
                // Create a stake account for the validator
                reserve_stake_balance -= MIN_STAKE_ACCOUNT_BALANCE;

                let mut instructions = stake_instruction::split_with_seed(
                    &reserve_stake_address,
                    &authorized_staker.pubkey(),
                    MIN_STAKE_ACCOUNT_BALANCE,
                    &stake_address,
                    &authorized_staker.pubkey(),
                    &validator_stake_address_seed(*vote_address),
                );
                instructions.push(stake_instruction::delegate_stake(
                    &stake_address,
                    &authorized_staker.pubkey(),
                    vote_address,
                ));

                transactions.push(Transaction::new_with_payer(
                    &instructions,
                    Some(&authorized_staker.pubkey()),
                ));
                info!(
                    "Creating stake account for validator {} ({})",
                    identity, stake_address
                );
            }
            warn!("Validator {} busy due to no stake account", identity);
            busy_validators.insert(*identity);
        }
    }

    if !send_and_confirm_transactions(rpc_client, false, transactions, authorized_staker)?
        .failed
        .is_empty()
    {
        Err("Failed to create validator stake accounts".into())
    } else {
        Ok(())
    }
}

fn distribute_validator_stake<V>(
    rpc_client: &RpcClient,
    authorized_staker: &Keypair,
    desired_validator_stake: V,
    reserve_stake_address: Pubkey,
    min_reserve_stake_balance: u64,
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
) -> Result<bool, Box<dyn error::Error>>
where
    V: IntoIterator<Item = ValidatorStake>,
{
    let mut reserve_stake_balance =
        get_available_stake_balance(rpc_client, reserve_stake_address, min_reserve_stake_balance)
            .map_err(|err| {
            format!(
                "Unable to get reserve stake account balance: {}: {}",
                reserve_stake_address, err
            )
        })?;

    info!(
        "Reserve stake available balance before updates: {}",
        Sol(reserve_stake_balance)
    );

    let mut transactions = vec![];
    for ValidatorStake {
        identity,
        stake_state,
        vote_address,
    } in desired_validator_stake
    {
        let desired_balance = match stake_state {
            ValidatorStakeState::No => MIN_STAKE_ACCOUNT_BALANCE,
            ValidatorStakeState::Baseline => baseline_stake_amount,
            ValidatorStakeState::Bonus => bonus_stake_amount,
        };

        let stake_address = validator_stake_address(authorized_staker.pubkey(), vote_address);
        let transient_stake_address =
            validator_transient_stake_address(authorized_staker.pubkey(), vote_address);

        let balance = rpc_client.get_balance(&stake_address).map_err(|err| {
            format!(
                "Unable to get stake account balance: {}: {}",
                stake_address, err
            )
        })?;

        info!(
            "desired stake for {} ({:?}) is {}, current balance is {}",
            identity,
            stake_state,
            Sol(desired_balance),
            Sol(balance)
        );

        let transient_stake_address_seed = validator_transient_stake_address_seed(vote_address);

        #[allow(clippy::comparison_chain)]
        if balance > desired_balance {
            let amount_to_remove = balance - desired_balance;
            if amount_to_remove < MIN_STAKE_CHANGE_AMOUNT {
                info!(
                    "Skipping deactivation since amount_to_remove is too small: {}",
                    Sol(amount_to_remove)
                );
            } else {
                info!("removing {} stake", Sol(amount_to_remove));
                let mut instructions = stake_instruction::split_with_seed(
                    &stake_address,
                    &authorized_staker.pubkey(),
                    amount_to_remove,
                    &transient_stake_address,
                    &authorized_staker.pubkey(),
                    &transient_stake_address_seed,
                );
                instructions.push(stake_instruction::deactivate_stake(
                    &transient_stake_address,
                    &authorized_staker.pubkey(),
                ));

                transactions.push(Transaction::new_with_payer(
                    &instructions,
                    Some(&authorized_staker.pubkey()),
                ));
            }
        } else if balance < desired_balance {
            let mut amount_to_add = desired_balance - balance;
            if amount_to_add > reserve_stake_balance {
                info!(
                    "note: amount_to_add > reserve_stake_balance: {} > {}",
                    amount_to_add, reserve_stake_balance
                );
                amount_to_add = reserve_stake_balance;
            }

            if amount_to_add < MIN_STAKE_CHANGE_AMOUNT {
                info!(
                    "Skipping delegation since amount_to_add is too small: {}",
                    Sol(amount_to_add)
                );
            } else {
                reserve_stake_balance -= amount_to_add;
                info!("adding {} stake", Sol(amount_to_add));

                let mut instructions = stake_instruction::split_with_seed(
                    &reserve_stake_address,
                    &authorized_staker.pubkey(),
                    amount_to_add,
                    &transient_stake_address,
                    &authorized_staker.pubkey(),
                    &transient_stake_address_seed,
                );
                instructions.push(stake_instruction::delegate_stake(
                    &transient_stake_address,
                    &authorized_staker.pubkey(),
                    &vote_address,
                ));

                transactions.push(Transaction::new_with_payer(
                    &instructions,
                    Some(&authorized_staker.pubkey()),
                ));
            }
        }
    }
    info!(
        "Reserve stake available balance after updates: {}",
        Sol(reserve_stake_balance)
    );

    let ok = send_and_confirm_transactions(rpc_client, false, transactions, authorized_staker)?
        .failed
        .is_empty();

    if !ok {
        error!("One or more transactions failed to execute")
    }
    Ok(ok)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::rpc_client_utils::test::*,
        solana_sdk::{
            clock::Epoch,
            epoch_schedule::{EpochSchedule, MINIMUM_SLOTS_PER_EPOCH},
            native_token::sol_to_lamports,
            signature::{Keypair, Signer},
        },
        solana_validator::test_validator::*,
    };

    fn num_stake_accounts(rpc_client: &RpcClient, authorized_staker: &Keypair) -> usize {
        get_all_stake(&rpc_client, authorized_staker.pubkey())
            .unwrap()
            .0
            .len()
    }

    fn validator_stake_balance(
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        validator: &ValidatorAddressPair,
    ) -> u64 {
        let stake_address = validator_stake_address(authorized_staker, validator.vote_address);
        rpc_client.get_balance(&stake_address).unwrap()
    }

    fn uniform_stake_pool_apply(
        stake_pool: &mut StakePool,
        rpc_client: &RpcClient,
        validators: &[ValidatorAddressPair],
        stake_state: ValidatorStakeState,
        expected_validator_stake_balance: u64,
        expected_reserve_stake_balance: u64,
    ) {
        let desired_validator_stake = validators
            .iter()
            .map(|vap| ValidatorStake {
                identity: vap.identity,
                vote_address: vap.vote_address,
                stake_state,
            })
            .collect::<Vec<_>>();

        stake_pool
            .apply(rpc_client, false, &desired_validator_stake)
            .unwrap();

        assert_eq!(
            num_stake_accounts(rpc_client, &stake_pool.authorized_staker),
            1 + 2 * validators.len()
        );
        let _epoch = wait_for_next_epoch(&rpc_client).unwrap();
        stake_pool
            .apply(rpc_client, false, &desired_validator_stake)
            .unwrap();

        assert_eq!(
            num_stake_accounts(rpc_client, &stake_pool.authorized_staker),
            1 + validators.len()
        );
        assert_eq!(
            rpc_client
                .get_balance(&stake_pool.reserve_stake_address)
                .unwrap(),
            expected_reserve_stake_balance
        );
        for validator in validators {
            assert_eq!(
                validator_stake_balance(
                    rpc_client,
                    stake_pool.authorized_staker.pubkey(),
                    validator
                ),
                expected_validator_stake_balance
            );
        }
    }

    #[test]
    fn this_test_is_too_big_and_slow() {
        solana_logger::setup_with_default("solana_stake_o_matic=info");

        let mut test_validator_genesis = TestValidatorGenesis::default();
        test_validator_genesis.epoch_schedule(EpochSchedule::custom(
            MINIMUM_SLOTS_PER_EPOCH,
            MINIMUM_SLOTS_PER_EPOCH,
            /* enable_warmup_epochs = */ false,
        ));
        let (test_validator, authorized_staker) = test_validator_genesis.start();

        let (rpc_client, _recent_blockhash, _fee_calculator) = test_validator.rpc_client();

        let authorized_staker_address = authorized_staker.pubkey();

        let assert_validator_stake_activation =
            |vap: &ValidatorAddressPair, epoch: Epoch, state: StakeActivationState| {
                let stake_address =
                    validator_stake_address(authorized_staker_address, vap.vote_address);
                assert_eq!(
                    rpc_client
                        .get_stake_activation(stake_address, Some(epoch))
                        .unwrap()
                        .state,
                    state,
                );
            };

        // ===========================================================
        info!("Create three validators, the reserve stake account, and a stake pool");
        let validators = create_validators(&rpc_client, &authorized_staker, 3).unwrap();

        let baseline_stake_amount = sol_to_lamports(10.);
        let min_reserve_stake_balance = MIN_STAKE_ACCOUNT_BALANCE;
        let total_stake_amount =
            (baseline_stake_amount + sol_to_lamports(100.)) * validators.len() as u64;
        let total_stake_amount_plus_min = total_stake_amount + min_reserve_stake_balance;

        let reserve_stake_address =
            create_stake_account(&rpc_client, &authorized_staker, total_stake_amount_plus_min)
                .unwrap()
                .pubkey();

        let assert_reserve_account_only = || {
            assert_eq!(
                rpc_client.get_balance(&reserve_stake_address).unwrap(),
                total_stake_amount_plus_min
            );
            {
                assert_eq!(
                    get_available_stake_balance(
                        &rpc_client,
                        reserve_stake_address,
                        min_reserve_stake_balance
                    )
                    .unwrap(),
                    total_stake_amount
                );

                let (all_stake, all_stake_total_amount) =
                    get_all_stake(&rpc_client, authorized_staker_address).unwrap();
                assert_eq!(all_stake_total_amount, total_stake_amount_plus_min);
                assert_eq!(all_stake.len(), 1);
                assert!(all_stake.contains(&reserve_stake_address));
            }
        };
        assert_reserve_account_only();

        let mut stake_pool = new(
            &rpc_client,
            authorized_staker,
            baseline_stake_amount,
            reserve_stake_address,
            min_reserve_stake_balance,
        )
        .unwrap();

        // ===========================================================
        info!("Start with no stake in the validators");
        let epoch = rpc_client.get_epoch_info().unwrap().epoch;
        stake_pool
            .apply(
                &rpc_client,
                false,
                &validators
                    .iter()
                    .map(|vap| ValidatorStake {
                        identity: vap.identity,
                        vote_address: vap.vote_address,
                        stake_state: ValidatorStakeState::No,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();

        info!("min: wait for stake activation");
        assert_eq!(
            rpc_client
                .get_balance(&stake_pool.reserve_stake_address)
                .unwrap(),
            total_stake_amount_plus_min - MIN_STAKE_ACCOUNT_BALANCE * validators.len() as u64,
        );

        for validator in &validators {
            assert_validator_stake_activation(validator, epoch, StakeActivationState::Activating);
            assert_eq!(
                validator_stake_balance(
                    &rpc_client,
                    stake_pool.authorized_staker.pubkey(),
                    validator
                ),
                MIN_STAKE_ACCOUNT_BALANCE
            );
        }
        assert_eq!(
            num_stake_accounts(&rpc_client, &stake_pool.authorized_staker),
            1 + validators.len()
        );
        let epoch = wait_for_next_epoch(&rpc_client).unwrap();
        for validator in &validators {
            assert_validator_stake_activation(validator, epoch, StakeActivationState::Active);
        }

        // ===========================================================
        info!("All validators to baseline stake level");
        uniform_stake_pool_apply(
            &mut stake_pool,
            &rpc_client,
            &validators,
            ValidatorStakeState::Baseline,
            baseline_stake_amount,
            total_stake_amount_plus_min - baseline_stake_amount * validators.len() as u64,
        );

        // ===========================================================
        info!("All the validators to bonus stake level");
        uniform_stake_pool_apply(
            &mut stake_pool,
            &rpc_client,
            &validators,
            ValidatorStakeState::Bonus,
            total_stake_amount / validators.len() as u64,
            MIN_STAKE_ACCOUNT_BALANCE,
        );

        // ===========================================================
        info!("Different stake for each validator");
        let desired_validator_stake = vec![
            ValidatorStake {
                identity: validators[0].identity,
                vote_address: validators[0].vote_address,
                stake_state: ValidatorStakeState::No,
            },
            ValidatorStake {
                identity: validators[1].identity,
                vote_address: validators[1].vote_address,
                stake_state: ValidatorStakeState::Baseline,
            },
            ValidatorStake {
                identity: validators[2].identity,
                vote_address: validators[2].vote_address,
                stake_state: ValidatorStakeState::Bonus,
            },
        ];

        stake_pool
            .apply(&rpc_client, false, &desired_validator_stake)
            .unwrap();
        let _epoch = wait_for_next_epoch(&rpc_client).unwrap();
        stake_pool
            .apply(&rpc_client, false, &desired_validator_stake)
            .unwrap();

        // after the first epoch, validators 0 and 1 are at their target levels but validator 2
        // needs one more epoch for the additional bonus stake to arrive
        for (validator, expected_sol_balance) in validators.iter().zip(&[1., 10., 110.]) {
            assert_eq!(
                sol_to_lamports(*expected_sol_balance),
                validator_stake_balance(
                    &rpc_client,
                    stake_pool.authorized_staker.pubkey(),
                    validator
                ),
                "stake balance mismatch for validator {}, expected {}",
                validator.identity,
                expected_sol_balance
            );
        }

        assert_eq!(
            rpc_client
                .get_balance(&stake_pool.reserve_stake_address)
                .unwrap(),
            MIN_STAKE_ACCOUNT_BALANCE,
        );

        let _epoch = wait_for_next_epoch(&rpc_client).unwrap();
        stake_pool
            .apply(&rpc_client, false, &desired_validator_stake)
            .unwrap();

        assert_eq!(
            rpc_client
                .get_balance(&stake_pool.reserve_stake_address)
                .unwrap(),
            MIN_STAKE_ACCOUNT_BALANCE,
        );

        // after the second epoch, validator 2 is now has all the bonus stake
        for (validator, expected_sol_balance) in validators.iter().zip(&[1., 10., 319.]) {
            assert_eq!(
                sol_to_lamports(*expected_sol_balance),
                validator_stake_balance(
                    &rpc_client,
                    stake_pool.authorized_staker.pubkey(),
                    validator
                ),
                "stake balance mismatch for validator {}",
                validator.identity
            );
        }

        // ===========================================================
        info!("remove all validators");

        // deactivate all validator stake
        stake_pool.apply(&rpc_client, false, &[]).unwrap();
        let _epoch = wait_for_next_epoch(&rpc_client).unwrap();
        // merge deactivated validator stake back into the reserve
        stake_pool.apply(&rpc_client, false, &[]).unwrap();
        // all stake has returned to the reserve account
        assert_reserve_account_only();
    }
}
