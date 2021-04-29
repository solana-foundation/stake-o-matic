use {
    crate::{generic_stake_pool::*, rpc_client_utils::send_and_confirm_transactions},
    log::*,
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{
        message::Message,
        native_token::*,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_stake_program::stake_instruction,
    std::{collections::HashSet, error},
};

pub struct LegacyStakePool {
    authorized_staker: Keypair,
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
    source_stake_address: Pubkey,
    validator_list: HashSet<Pubkey>,
}

pub fn new(
    _rpc_client: &RpcClient,
    authorized_staker: Keypair,
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
    source_stake_address: Pubkey,
    validator_list: HashSet<Pubkey>,
) -> Result<LegacyStakePool, Box<dyn error::Error>> {
    Ok(LegacyStakePool {
        authorized_staker,
        baseline_stake_amount,
        bonus_stake_amount,
        source_stake_address,
        validator_list,
    })
}

impl GenericStakePool for LegacyStakePool {
    fn is_enrolled(&self, validator_identity: &Pubkey) -> bool {
        self.validator_list.contains(validator_identity)
    }

    fn apply(
        &mut self,
        rpc_client: &RpcClient,
        dry_run: bool,
        validator_stake: &[ValidatorStake],
    ) -> Result<Vec<String>, Box<dyn error::Error>> {
        let (init_transactions, update_transactions) = self.build_transactions(
            rpc_client,
            self.authorized_staker.pubkey(),
            &validator_stake,
        )?;

        if !send_and_confirm_transactions(
            rpc_client,
            dry_run,
            init_transactions,
            &self.authorized_staker,
            &mut vec![],
        )? {
            return Err("Failed to initialize stake pool. Unable to continue".into());
        }

        let mut notifications = vec![
            format!("Baseline stake amount: {}", Sol(self.baseline_stake_amount)),
            format!("Bonus stake amount: {}", Sol(self.bonus_stake_amount)),
        ];
        let ok = send_and_confirm_transactions(
            rpc_client,
            dry_run,
            update_transactions,
            &self.authorized_staker,
            &mut notifications,
        )?;

        if !ok {
            error!("One or more transactions failed to execute")
        }
        Ok(notifications)
    }
}

type TransactionWithMemo = (Transaction, String);

impl LegacyStakePool {
    fn build_transactions(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        validator_stake: &[ValidatorStake],
    ) -> Result<(Vec<TransactionWithMemo>, Vec<TransactionWithMemo>), Box<dyn error::Error>> {
        let mut init_transactions = vec![];
        let mut update_transactions = vec![];
        let mut source_stake_lamports_required = 0;

        let epoch_info = rpc_client.get_epoch_info()?;

        let source_stake_balance = {
            let source_stake_activation = rpc_client
                .get_stake_activation(self.source_stake_address, Some(epoch_info.epoch))
                .map_err(|err| {
                    format!(
                        "Unable to get activation information for source stake account: {}: {}",
                        self.source_stake_address, err
                    )
                })?;
            if source_stake_activation.state != StakeActivationState::Inactive {
                return Err("Source stake account is not inactive".into());
            }
            source_stake_activation.inactive
        };
        info!(
            "source stake account balance: {}",
            Sol(source_stake_balance)
        );

        for ValidatorStake {
            identity,
            vote_address,
            memo,
            stake_state,
        } in validator_stake
        {
            if !self.is_enrolled(identity) {
                continue;
            }

            let baseline_seed = &vote_address.to_string()[..32];
            let bonus_seed = &format!("A{{{}", vote_address.to_string())[..32];

            let baseline_stake_address = Pubkey::create_with_seed(
                &authorized_staker,
                baseline_seed,
                &solana_stake_program::id(),
            )
            .unwrap();
            let bonus_stake_address = Pubkey::create_with_seed(
                &authorized_staker,
                bonus_seed,
                &solana_stake_program::id(),
            )
            .unwrap();

            debug!(
                "identity: {} - baseline stake: {}\n - bonus stake: {}",
                identity, baseline_stake_address, bonus_stake_address
            );

            let baseline_stake_activation_state = if rpc_client
                .get_account_with_commitment(&baseline_stake_address, rpc_client.commitment())?
                .value
                .is_some()
            {
                rpc_client
                    .get_stake_activation(baseline_stake_address, Some(epoch_info.epoch))
                    .map_err(|err| {
                        format!(
                            "Unable to get activation information for baseline stake account: {}: {}",
                            baseline_stake_address, err
                        )
                    })?.state
            } else {
                let memo = format!(
                    "Creating baseline stake account for validator {} ({})",
                    identity, baseline_stake_address
                );
                debug!("Adding transaction: {}", memo);

                source_stake_lamports_required += self.baseline_stake_amount;
                init_transactions.push((
                    Transaction::new_unsigned(Message::new(
                        &stake_instruction::split_with_seed(
                            &self.source_stake_address,
                            &authorized_staker,
                            self.baseline_stake_amount,
                            &baseline_stake_address,
                            &authorized_staker,
                            baseline_seed,
                        ),
                        Some(&authorized_staker),
                    )),
                    memo,
                ));
                StakeActivationState::Inactive
            };

            let bonus_stake_activation_state = if rpc_client
                .get_account_with_commitment(&bonus_stake_address, rpc_client.commitment())?
                .value
                .is_some()
            {
                rpc_client
                    .get_stake_activation(bonus_stake_address, Some(epoch_info.epoch))
                    .map_err(|err| {
                        format!(
                            "Unable to get activation information for bonus stake account: {}: {}",
                            bonus_stake_address, err
                        )
                    })?
                    .state
            } else {
                let memo = format!(
                    "Creating bonus stake account for validator {} ({})",
                    identity, bonus_stake_address
                );
                debug!("Adding transaction: {}", memo);
                source_stake_lamports_required += self.bonus_stake_amount;
                init_transactions.push((
                    Transaction::new_unsigned(Message::new(
                        &stake_instruction::split_with_seed(
                            &self.source_stake_address,
                            &authorized_staker,
                            self.bonus_stake_amount,
                            &bonus_stake_address,
                            &authorized_staker,
                            bonus_seed,
                        ),
                        Some(&authorized_staker),
                    )),
                    memo,
                ));
                StakeActivationState::Inactive
            };

            if let Some(transaction) = Self::build_validator_stake_state_transaction(
                authorized_staker,
                *vote_address,
                baseline_stake_address,
                bonus_stake_address,
                baseline_stake_activation_state,
                bonus_stake_activation_state,
                stake_state,
            ) {
                update_transactions.push((transaction, memo.clone()))
            }
        }

        if !init_transactions.is_empty() {
            info!(
                "{} is required to create {} stake accounts",
                Sol(source_stake_lamports_required),
                init_transactions.len()
            );

            if source_stake_balance < source_stake_lamports_required {
                return Err(format!(
                    "Source stake account has insufficient balance: {} , but {} is required",
                    Sol(source_stake_balance),
                    Sol(source_stake_lamports_required)
                )
                .into());
            }
        }

        Ok((init_transactions, update_transactions))
    }

    fn build_validator_stake_state_transaction(
        authorized_staker: Pubkey,
        vote_address: Pubkey,
        baseline_stake_address: Pubkey,
        bonus_stake_address: Pubkey,
        baseline_stake_activation_state: StakeActivationState,
        bonus_stake_activation_state: StakeActivationState,
        stake_state: &ValidatorStakeState,
    ) -> Option<Transaction> {
        let (baseline, bonus) = match stake_state {
            ValidatorStakeState::None => (false, false),
            ValidatorStakeState::Baseline => (true, false),
            ValidatorStakeState::Bonus => (true, true),
        };

        let mut instructions = vec![];
        if baseline {
            if baseline_stake_activation_state == StakeActivationState::Inactive {
                instructions.push(stake_instruction::delegate_stake(
                    &baseline_stake_address,
                    &authorized_staker,
                    &vote_address,
                ));
            }
        } else if matches!(
            baseline_stake_activation_state,
            StakeActivationState::Activating | StakeActivationState::Active
        ) {
            instructions.push(stake_instruction::deactivate_stake(
                &baseline_stake_address,
                &authorized_staker,
            ));
        }

        if bonus {
            if bonus_stake_activation_state == StakeActivationState::Inactive {
                instructions.push(stake_instruction::delegate_stake(
                    &bonus_stake_address,
                    &authorized_staker,
                    &vote_address,
                ));
            }
        } else if matches!(
            bonus_stake_activation_state,
            StakeActivationState::Activating | StakeActivationState::Active
        ) {
            instructions.push(stake_instruction::deactivate_stake(
                &bonus_stake_address,
                &authorized_staker,
            ));
        }

        if !instructions.is_empty() {
            Some(Transaction::new_unsigned(Message::new(
                &instructions,
                Some(&authorized_staker),
            )))
        } else {
            None
        }
    }
}
