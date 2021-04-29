#![allow(dead_code)]
use {
    crate::generic_stake_pool::*,
    log::*,
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    std::{collections::HashMap, error},
};

#[derive(Debug)]
struct ValidatorInfo {
    vote_pubkey: Pubkey,
    baseline_stake_address: Pubkey,
    baseline_stake_activation_state: StakeActivationState,
}

pub struct SplStakePool {
    authorized_staker: Keypair,
    baseline_stake_amount: u64,
    pool_address: Pubkey,
    validator_info: HashMap<Pubkey, ValidatorInfo>,
}

pub fn new(
    _rpc_client: &RpcClient,
    authorized_staker: Keypair,
    pool_address: Pubkey,
    baseline_stake_amount: u64,
) -> Result<SplStakePool, Box<dyn error::Error>> {
    Ok(SplStakePool {
        authorized_staker,
        baseline_stake_amount,
        pool_address,
        validator_info: HashMap::new(),
    })
}

impl GenericStakePool for SplStakePool {
    fn is_enrolled(&self, validator_identity: &Pubkey) -> bool {
        info!("validator: {}", validator_identity);
        todo!();
    }
    fn apply(
        &mut self,
        _rpc_client: &RpcClient,
        _dry_run: bool,
        _desired_validator_stake: &[ValidatorStake],
    ) -> Result<Vec<String>, Box<dyn error::Error>> {
        todo!();
    }
}
