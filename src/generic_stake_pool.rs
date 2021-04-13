use {
    solana_client::rpc_client::RpcClient,
    solana_sdk::{epoch_info::EpochInfo, pubkey::Pubkey, transaction::Transaction},
    std::error,
};

/// The staking states that a validator can be in
#[derive(Debug, PartialEq)]
pub enum ValidatorStakeState {
    None,     // Validator should receive no stake
    Baseline, // Validator has been awarded a baseline stake
    Bonus,    // Validator has been awarded a bonus stake in addition to the baseline stake
}

pub struct ValidatorAddressPair {
    pub identity: Pubkey,
    pub vote_address: Pubkey,
}

pub struct ValidatorStake {
    pub identity: Pubkey,
    pub stake_state: ValidatorStakeState,
    pub memo: String,
}

pub trait GenericStakePool {
    fn init(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        validators: Vec<ValidatorAddressPair>,
        epoch_info: &EpochInfo,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>>;

    fn is_enrolled(&self, validator_identity: &Pubkey) -> bool;

    fn apply(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        desired_validator_stake: Vec<ValidatorStake>,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>>;
}
