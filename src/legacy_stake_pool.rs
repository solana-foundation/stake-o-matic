use {
    crate::generic_stake_pool::*,
    log::*,
    solana_client::{
        rpc_client::RpcClient,
        rpc_response::{RpcVoteAccountInfo, StakeActivationState},
    },
    solana_sdk::{
        epoch_info::EpochInfo, message::Message, native_token::*, pubkey::Pubkey,
        transaction::Transaction,
    },
    solana_stake_program::stake_instruction,
    std::{
        collections::{HashMap, HashSet},
        error,
        str::FromStr,
    },
};

struct ValidatorInfo {
    vote_pubkey: Pubkey,
    baseline_stake_address: Pubkey,
    bonus_stake_address: Pubkey,
    baseline_stake_activation_state: StakeActivationState,
    bonus_stake_activation_state: StakeActivationState,
}

pub struct LegacyStakePool {
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
    source_stake_address: Pubkey,
    validator_list: HashSet<Pubkey>,
    validator_info: HashMap<Pubkey, ValidatorInfo>,
}

pub fn new(
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
    source_stake_address: Pubkey,
    validator_list: HashSet<Pubkey>,
) -> LegacyStakePool {
    LegacyStakePool {
        baseline_stake_amount,
        bonus_stake_amount,
        source_stake_address,
        validator_list,
        validator_info: HashMap::new(),
    }
}

impl GenericStakePool for LegacyStakePool {
    fn init(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        vote_account_info: &[RpcVoteAccountInfo],
        epoch_info: &EpochInfo,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>> {
        let mut transactions = vec![];
        let mut source_stake_lamports_required = 0;

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

        for RpcVoteAccountInfo {
            node_pubkey: node_pubkey_str,
            vote_pubkey,
            ..
        } in vote_account_info
        {
            let node_pubkey = Pubkey::from_str(&node_pubkey_str).unwrap();
            if !self.is_enrolled(&node_pubkey) {
                continue;
            }

            let baseline_seed = &vote_pubkey.to_string()[..32];
            let bonus_seed = &format!("A{{{}", vote_pubkey)[..32];
            let vote_pubkey = Pubkey::from_str(&vote_pubkey).unwrap();

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
                node_pubkey, baseline_stake_address, bonus_stake_address
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
                            self.source_stake_address, err
                        )
                    })?.state
            } else {
                source_stake_lamports_required += self.baseline_stake_amount;
                transactions.push((
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
                    format!(
                        "Creating baseline stake account for validator {} ({})",
                        node_pubkey, baseline_stake_address
                    ),
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
                            self.source_stake_address, err
                        )
                    })?
                    .state
            } else {
                source_stake_lamports_required += self.bonus_stake_amount;
                transactions.push((
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
                    format!(
                        "Creating bonus stake account for validator {} ({})",
                        node_pubkey, bonus_stake_address
                    ),
                ));
                StakeActivationState::Inactive
            };

            self.validator_info.insert(
                node_pubkey,
                ValidatorInfo {
                    vote_pubkey,
                    baseline_stake_address,
                    bonus_stake_address,
                    baseline_stake_activation_state,
                    bonus_stake_activation_state,
                },
            );
        }

        if !transactions.is_empty() {
            info!(
                "{} is required to create {} stake accounts",
                Sol(source_stake_lamports_required),
                transactions.len()
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

        Ok(transactions)
    }

    fn is_enrolled(&self, validator_identity: &Pubkey) -> bool {
        self.validator_list.contains(validator_identity)
    }

    fn apply_validator_stake_state(
        &mut self,
        _rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        node_pubkey: Pubkey,
        stake_state: ValidatorStakeState,
    ) -> Result<Option<Transaction>, Box<dyn error::Error>> {
        let ValidatorInfo {
            vote_pubkey,
            baseline_stake_address,
            bonus_stake_address,
            baseline_stake_activation_state,
            bonus_stake_activation_state,
        } = self
            .validator_info
            .get(&node_pubkey)
            .ok_or_else(|| format!("Unknown validator identity: {}", node_pubkey))?;

        let (baseline, bonus) = match stake_state {
            ValidatorStakeState::None => (false, false),
            ValidatorStakeState::Baseline => (true, false),
            ValidatorStakeState::Bonus => (true, true),
        };

        let mut instructions = vec![];
        if baseline {
            if *baseline_stake_activation_state == StakeActivationState::Inactive {
                instructions.push(stake_instruction::delegate_stake(
                    &baseline_stake_address,
                    &authorized_staker,
                    &vote_pubkey,
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
            if *bonus_stake_activation_state == StakeActivationState::Inactive {
                instructions.push(stake_instruction::delegate_stake(
                    &bonus_stake_address,
                    &authorized_staker,
                    &vote_pubkey,
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

        Ok(if !instructions.is_empty() {
            Some(Transaction::new_unsigned(Message::new(
                &instructions,
                Some(&authorized_staker),
            )))
        } else {
            None
        })
    }
}
