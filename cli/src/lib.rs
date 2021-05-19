use {
    registry_program::state::{Participant, ParticipantState},
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::*,
    },
    solana_sdk::{program_pack::Pack, pubkey::Pubkey},
    std::collections::HashMap,
};

pub fn get_participants_with_state(
    rpc_client: &RpcClient,
    state: Option<ParticipantState>,
) -> Result<HashMap<Pubkey, Participant>, Box<dyn std::error::Error>> {
    let accounts = rpc_client.get_program_accounts_with_config(
        &registry_program::id(),
        RpcProgramAccountsConfig {
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64Zstd),
                commitment: Some(rpc_client.commitment()), // TODO: Remove this line after updating to solana v1.6.10
                ..RpcAccountInfoConfig::default()
            },
            filters: Some(vec![RpcFilterType::DataSize(
                Participant::get_packed_len() as u64
            )]),
        },
    )?;

    Ok(accounts
        .into_iter()
        .filter_map(|(address, account)| {
            Participant::unpack_from_slice(&account.data)
                .ok()
                .map(|p| (address, p))
        })
        .filter(|(_, p)| {
            if let Some(ref state) = state {
                return p.state == *state;
            }
            true
        })
        .collect())
}

pub fn get_participants(
    rpc_client: &RpcClient,
) -> Result<HashMap<Pubkey, Participant>, Box<dyn std::error::Error>> {
    get_participants_with_state(rpc_client, None)
}
