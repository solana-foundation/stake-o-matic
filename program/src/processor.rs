//! Program state processor

use crate::{instruction::*, state::*, *};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    msg,
    program_error::ProgramError,
    program_pack::Pack,
    pubkey::Pubkey,
};

fn authenticate_admin(admin_info: &AccountInfo) -> ProgramResult {
    if crate::admin::id() != *admin_info.key {
        msg!("Error: {} is not the admin", admin_info.key);
        return Err(ProgramError::InvalidArgument);
    }
    if !admin_info.is_signer {
        msg!("Error: {} is not a signer", admin_info.key);
        return Err(ProgramError::MissingRequiredSignature);
    }

    Ok(())
}

pub fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    input: &[u8],
) -> ProgramResult {
    let instruction = RegistryInstruction::unpack_from_slice(input)?;
    let account_info_iter = &mut accounts.iter();
    let participant_info = next_account_info(account_info_iter)?;
    let mut participant = Participant::unpack_from_slice(&participant_info.data.borrow())?;

    match instruction {
        RegistryInstruction::Apply => {
            msg!("Apply");
            let mainnet_identity_info = next_account_info(account_info_iter)?;
            let testnet_identity_info = next_account_info(account_info_iter)?;

            if !mainnet_identity_info.is_signer {
                msg!("Error: {} is not a signer", mainnet_identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }
            if !testnet_identity_info.is_signer {
                msg!("Error: {} is not a signer", testnet_identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }

            participant.testnet_identity = *testnet_identity_info.key;
            participant.mainnet_identity = *mainnet_identity_info.key;
            participant.state = ParticipantState::Pending;
        }
        RegistryInstruction::Withdraw => {
            msg!("Withdraw");
            let identity_info = next_account_info(account_info_iter)?;

            if !identity_info.is_signer {
                msg!("Error: {} is not a signer", identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }

            if *identity_info.key != participant.testnet_identity
                || *identity_info.key != participant.mainnet_identity
            {
                msg!("Error: {} is not authorized", identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }
            participant.state = ParticipantState::Withdrawn;
        }
        RegistryInstruction::Approve => {
            msg!("Approve");
            authenticate_admin(next_account_info(account_info_iter)?)?;
            participant.state = ParticipantState::Approved;
        }
        RegistryInstruction::Reject => {
            msg!("Reject");
            authenticate_admin(next_account_info(account_info_iter)?)?;
            participant.state = ParticipantState::Rejected;
        }
        RegistryInstruction::Rewrite(new_participant) => {
            msg!("Rewrite");
            authenticate_admin(next_account_info(account_info_iter)?)?;
            participant = new_participant;
        }
    }
    participant.pack_into_slice(&mut participant_info.data.borrow_mut());

    Ok(())
}
