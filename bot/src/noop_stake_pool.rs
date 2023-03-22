use log::info;
use solana_sdk::pubkey::Pubkey;
use {
    crate::{generic_stake_pool::*, rpc_client_utils::MultiClient},
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
        _client: &MultiClient,
        dry_run: bool,
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

        info!("NoopStakePool run with dry_run={}", dry_run);

        let notes = vec![
            "This is the noop stake pool. All number are make-believe, and stake never distributed"
                .to_string(),
        ];

        Ok((notes, validator_stake_actions, HashSet::new(), 12_345))
    }
}
