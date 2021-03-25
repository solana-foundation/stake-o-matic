use {
    crate::stake_pool::*,
    log::*,
    solana_client::{rpc_client::RpcClient, rpc_response::RpcVoteAccountInfo},
    solana_sdk::{
        account_utils::StateMut, epoch_info::EpochInfo, message::Message, native_token::*,
        pubkey::Pubkey, transaction::Transaction,
    },
    solana_stake_program::{stake_instruction, stake_state::StakeState},
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
}

pub struct LegacyStakePool {
    baseline_stake_amount: u64,
    bonus_stake_amount: u64,
    source_stake_address: Pubkey,
    validator_list: HashSet<Pubkey>,
    validator_info: HashMap<Pubkey, ValidatorInfo>,
    stake_activated_in_current_epoch: HashSet<Pubkey>,
}

impl LegacyStakePool {
    pub fn new(
        baseline_stake_amount: u64,
        bonus_stake_amount: u64,
        source_stake_address: Pubkey,
        validator_list: HashSet<Pubkey>,
    ) -> Self {
        Self {
            baseline_stake_amount,
            bonus_stake_amount,
            source_stake_address,
            validator_list,
            validator_info: HashMap::new(),
            stake_activated_in_current_epoch: HashSet::new(),
        }
    }
}

impl StakePool for LegacyStakePool {
    fn init(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        vote_account_info: &[RpcVoteAccountInfo],
        epoch_info: &EpochInfo,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>> {
        let mut transactions = vec![];
        let mut source_stake_lamports_required = 0;

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

            self.validator_info.insert(
                node_pubkey,
                ValidatorInfo {
                    vote_pubkey,
                    baseline_stake_address,
                    bonus_stake_address,
                },
            );

            debug!(
                "identity: {} - baseline stake: {}\n - bonus stake: {}",
                node_pubkey, baseline_stake_address, bonus_stake_address
            );

            // Transactions to create the baseline and bonus stake accounts
            if let Ok((balance, stake_state)) =
                get_stake_account(rpc_client, &baseline_stake_address)
            {
                if balance <= self.baseline_stake_amount {
                    info!(
                        "Unexpected balance in stake account {}: {}, expected {}",
                        baseline_stake_address, balance, self.baseline_stake_amount
                    );
                }
                if let Some(delegation) = stake_state.delegation() {
                    if epoch_info.epoch == delegation.activation_epoch {
                        self.stake_activated_in_current_epoch
                            .insert(baseline_stake_address);
                    }
                }
            } else {
                info!(
                    "Need to create baseline stake account for validator {}",
                    node_pubkey
                );
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
            }

            if let Ok((balance, stake_state)) = get_stake_account(rpc_client, &bonus_stake_address)
            {
                if balance <= self.bonus_stake_amount {
                    info!(
                        "Unexpected balance in stake account {}: {}, expected {}",
                        bonus_stake_address, balance, self.bonus_stake_amount
                    );
                }
                if let Some(delegation) = stake_state.delegation() {
                    if epoch_info.epoch == delegation.activation_epoch {
                        self.stake_activated_in_current_epoch
                            .insert(bonus_stake_address);
                    }
                }
            } else {
                info!(
                    "Need to create bonus stake account for validator {}",
                    node_pubkey
                );
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
            }
        }

        info!(
            "{} SOL is required to create {} stake accounts",
            lamports_to_sol(source_stake_lamports_required),
            transactions.len()
        );

        // check source stake account
        let (source_stake_balance, source_stake_state) =
            get_stake_account(rpc_client, &self.source_stake_address)?;

        info!(
            "source stake account balance: {} SOL",
            lamports_to_sol(source_stake_balance)
        );

        match &source_stake_state {
            StakeState::Initialized(_) | StakeState::Stake(_, _) => {
                if source_stake_state.authorized().unwrap().staker != authorized_staker {
                    return Err(format!(
                        "The authorized staker for the source stake account is not {}",
                        authorized_staker,
                    )
                    .into());
                } else if source_stake_balance < source_stake_lamports_required {
                    return Err(format!(
                                "Source stake account has insufficient balance: {} SOL, but {} SOL is required",
                                lamports_to_sol(source_stake_balance),
                                lamports_to_sol(source_stake_lamports_required)
                            )
                            .into());
                }
            }
            _ => {
                return Err(format!(
                    "Source stake account is not in the initialized state: {:?}",
                    source_stake_state
                )
                .into())
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
    ) -> Result<Vec<Transaction>, Box<dyn error::Error>> {
        let ValidatorInfo {
            vote_pubkey,
            baseline_stake_address,
            bonus_stake_address,
        } = self
            .validator_info
            .get(&node_pubkey)
            .ok_or_else(|| format!("Unknown validator identity: {}", node_pubkey))?;

        let (baseline, bonus) = match stake_state {
            ValidatorStakeState::None => (false, false),
            ValidatorStakeState::Baseline => (true, false),
            ValidatorStakeState::Bonus => (true, true),
        };

        let mut transactions = vec![];

        if baseline {
            if !self
                .stake_activated_in_current_epoch
                .contains(&baseline_stake_address)
            {
                transactions.push(Transaction::new_unsigned(Message::new(
                    &[stake_instruction::delegate_stake(
                        &baseline_stake_address,
                        &authorized_staker,
                        &vote_pubkey,
                    )],
                    Some(&authorized_staker),
                )));
            }
        } else {
            // Deactivate baseline stake
            transactions.push(Transaction::new_unsigned(Message::new(
                &[stake_instruction::deactivate_stake(
                    &baseline_stake_address,
                    &authorized_staker,
                )],
                Some(&authorized_staker),
            )));
        }

        if bonus {
            // Activate bonus stake
            if !self
                .stake_activated_in_current_epoch
                .contains(&bonus_stake_address)
            {
                transactions.push(Transaction::new_unsigned(Message::new(
                    &[stake_instruction::delegate_stake(
                        &bonus_stake_address,
                        &authorized_staker,
                        &vote_pubkey,
                    )],
                    Some(&authorized_staker),
                )));
            }
        } else {
            // Deactivate bonus stake
            transactions.push(Transaction::new_unsigned(Message::new(
                &[stake_instruction::deactivate_stake(
                    &bonus_stake_address,
                    &authorized_staker,
                )],
                Some(&authorized_staker),
            )));
        }
        Ok(transactions)
    }
}

fn get_stake_account(
    rpc_client: &RpcClient,
    address: &Pubkey,
) -> Result<(u64, StakeState), String> {
    let account = rpc_client.get_account(address).map_err(|e| {
        format!(
            "Failed to fetch stake account {}: {}",
            address,
            e.to_string()
        )
    })?;

    if account.owner != solana_stake_program::id() {
        return Err(format!(
            "not a stake account (owned by {}): {}",
            account.owner, address
        ));
    }

    account
        .state()
        .map_err(|e| {
            format!(
                "Failed to decode stake account at {}: {}",
                address,
                e.to_string()
            )
        })
        .map(|stake_state| (account.lamports, stake_state))
}
