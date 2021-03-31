use {
    solana_client::{rpc_client::RpcClient, rpc_response::RpcVoteAccountInfo},
    solana_sdk::{epoch_info::EpochInfo, pubkey::Pubkey, transaction::Transaction},
    std::error,
};

/// The staking states that a validator can be in
#[derive(Debug)]
pub enum ValidatorStakeState {
    None,     // Validator should receive no stake
    Baseline, // Validator has been awarded a baseline stake
    Bonus,    // Validator has been awarded a bonus stake in addition to the baseline stake
}

pub trait GenericStakePool {
    fn init(
        &mut self,
        rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        vote_account_info: &[RpcVoteAccountInfo],
        epoch_info: &EpochInfo,
    ) -> Result<Vec<(Transaction, String)>, Box<dyn error::Error>>;
    fn is_enrolled(&self, validator_identity: &Pubkey) -> bool;
    fn apply_validator_stake_state(
        &mut self,
        _rpc_client: &RpcClient,
        authorized_staker: Pubkey,
        node_pubkey: Pubkey,
        stake_state: ValidatorStakeState,
    ) -> Result<Option<Transaction>, Box<dyn error::Error>>;
}
