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

#[cfg(test)]
mod test_admin {
    solana_program::declare_id!("563B79TEFBRx8f6vwJH1XWo85MSsJRaV3E2EdmwUtjmG");
}

fn is_admin(address: &Pubkey) -> bool {
    if crate::admin::id() == *address {
        return true;
    }

    #[cfg(test)]
    if test_admin::id() == *address {
        return true;
    }

    false
}

fn authenticate_admin(admin_info: &AccountInfo) -> ProgramResult {
    if !is_admin(admin_info.key) {
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

#[cfg(test)]
mod test {
    use {super::*, solana_sdk::signature::{Signer,Keypair}};

    fn test_admin_keypair() -> Keypair {
        let keypair = Keypair::from_bytes(&[
            195, 121, 73, 133, 212, 8, 231, 45, 116, 99, 128, 66, 118, 174, 197, 26, 112, 146, 204,
            201, 119, 40, 97, 2, 86, 10, 98, 116, 235, 40, 163, 221, 60, 185, 28, 52, 69, 70, 108,
            96, 236, 253, 114, 203, 81, 219, 79, 136, 0, 185, 165, 101, 147, 67, 207, 255, 69, 83,
            242, 34, 36, 32, 80, 87,
        ]).unwrap();
        assert_eq!(keypair.pubkey(), test_admin::id());
        keypair
    }
}
