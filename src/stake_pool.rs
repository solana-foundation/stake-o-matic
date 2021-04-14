use {
    crate::generic_stake_pool::*,
    log::*,
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{epoch_info::EpochInfo, pubkey::Pubkey, transaction::Transaction},
    std::{collections::HashMap, error},
};

#[derive(Debug)]
struct ValidatorInfo {
    vote_pubkey: Pubkey,
    baseline_stake_address: Pubkey,
    baseline_stake_activation_state: StakeActivationState,
}

#[derive(Debug)]
pub struct SplStakePool {
    baseline_stake_amount: u64,
    pool_address: Pubkey,
    validator_info: HashMap<Pubkey, ValidatorInfo>,
}

pub fn new(pool_address: Pubkey, baseline_stake_amount: u64) -> SplStakePool {
    SplStakePool {
        baseline_stake_amount,
        pool_address,
        validator_info: HashMap::new(),
    }
}

impl GenericStakePool for SplStakePool {
    fn init(
        &mut self,
        _rpc_client: &RpcClient,
        _authorized_staker: Pubkey,
        _validators: &[ValidatorAddressPair],
        _epoch_info: &EpochInfo,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>> {
        info!("{:?}", self);
        todo!();
    }

    fn is_enrolled(&self, _validator_identity: &Pubkey) -> bool {
        todo!();
    }

    fn baseline_stake_amount(&self) -> u64 {
        todo!();
    }

    fn bonus_stake_amount(&self) -> u64 {
        todo!();
    }

    fn apply(
        &mut self,
        _rpc_client: &RpcClient,
        _authorized_staker: Pubkey,
        _desired_validator_stake: &[ValidatorStake],
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>> {
        todo!();
    }
}
