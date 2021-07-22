use {
    crate::generic_stake_pool::*,
    solana_client::rpc_client::RpcClient,
    std::{
        collections::{HashMap, HashSet},
        error,
    },
};

pub struct NoopStakePool;

pub fn new() -> NoopStakePool {
    NoopStakePool
}

impl GenericStakePool for NoopStakePool {
    fn apply(
        &mut self,
        _rpc_client: &RpcClient,
        _dry_run: bool,
        _desired_validator_stake: &[ValidatorStake],
    ) -> Result<
        (
            EpochStakeNotes,
            ValidatorStakeActions,
            UnfundedValidators,
            u64,
        ),
        Box<dyn error::Error>,
    > {
        Ok((vec![], HashMap::new(), HashSet::new(), 0))
    }
}
