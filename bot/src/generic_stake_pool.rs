use {
    serde::{Deserialize, Serialize},
    solana_client::rpc_client::RpcClient,
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        error,
    },
};

#[derive(Debug, PartialEq, Clone, Copy, Deserialize, Serialize)]
pub enum ValidatorStakeState {
    None,     // Validator should receive no stake
    Baseline, // Validator has earned the baseline stake level
    Bonus,    // Validator has earned the bonus stake level
}

impl Default for ValidatorStakeState {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidatorStake {
    pub identity: Pubkey,
    pub vote_address: Pubkey,
    pub stake_state: ValidatorStakeState,
    pub priority: bool,
}

pub type EpochStakeNotes = Vec<String>;
pub type ValidatorStakeActions = HashMap<Pubkey, String>;
pub type UnfundedValidators = HashSet<Pubkey>;

pub trait GenericStakePool {
    fn apply(
        &mut self,
        rpc_client: &RpcClient,
        dry_run: bool,
        desired_validator_stake: &[ValidatorStake],
    ) -> Result<(EpochStakeNotes, ValidatorStakeActions, UnfundedValidators), Box<dyn error::Error>>;
}
