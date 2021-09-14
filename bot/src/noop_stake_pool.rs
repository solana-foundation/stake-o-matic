use solana_sdk::pubkey::Pubkey;
use {
    crate::generic_stake_pool::*,
    solana_client::rpc_client::RpcClient,
    std::{
        collections::{HashMap, HashSet},
        error,
        sync::Arc,
    },
};

pub struct NoopStakePool;

pub fn new() -> NoopStakePool {
    NoopStakePool
}

impl GenericStakePool for NoopStakePool {
    fn apply(
        &mut self,
        _rpc_client: Arc<RpcClient>,
        _websocket_url: &str,
        _dry_run: bool,
        desired_validator_stake: &[ValidatorStake],
    ) -> Result<
        (
            EpochStakeNotes,
            ValidatorStakeActions,
            UnfundedValidators,
            u64,
        ),
        Box<dyn error::Error>,
    > {
        let validator_stake_actions: HashMap<Pubkey, String> = desired_validator_stake
            .iter()
            .map(|vs| {
                (
                    vs.identity,
                    "Test action from NoopStakePool for validator".to_string(),
                )
            })
            .collect();

        Ok((vec![], validator_stake_actions, HashSet::new(), 12_345))
    }
}
