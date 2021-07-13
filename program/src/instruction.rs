//! Program instructions

use crate::{id, state::Participant};
use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use solana_program::{
    instruction::{AccountMeta, Instruction},
    msg,
    program_error::ProgramError,
    program_pack::{Pack, Sealed},
    pubkey::Pubkey,
};

/// Instructions supported by the Feature Proposal program
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, BorshSchema, PartialEq)]
pub enum RegistryInstruction {
    /// Apply for the program
    ///
    /// On success the participant will be moved to the `ParticipantState::Pending` state
    ///
    /// 0. `[writable]` Uninitialized `Participant` account
    /// 1. `[signer]` Mainnet validator identity
    /// 2. `[signer]` Testnet validator identity
    Apply,

    /// Withdraw from the program
    ///
    /// On success the participant account will be deleted and lamports in it refunded
    ///
    /// 0. `[writable]` `Participant` account in the `ParticipantState::Pending` or
    ///                 `ParticipantState::Enrolled` state
    /// 1. `[signer]` Mainnet or Testnet validator identity
    /// 2. `[writable]`  The account to receive the closed account's lamports.
    ///
    Withdraw,

    /// Approve a participant.
    ///
    /// On success the participant will be moved to the `ParticipantState::Enrolled` state
    ///
    /// 0. `[writable]` `Participant` account in the `ParticipantState::Pending` state
    /// 1. `[signer]` Admin account
    Approve,

    /// Reject a participant
    ///
    /// On success the participant will be moved to the `ParticipantState::Rejected` state
    ///
    /// 0. `[writable]` `Participant` account in the `ParticipantState::Pending` or
    ///                 `ParticipantState::Enrolled` state
    /// 1. `[signer]` Admin account
    Reject,

    /// Bypass the normal workflow and rewrite a participant account to the provided state
    ///
    /// 0. `[writable]` `Participant` account in any state
    /// 1. `[signer]` Admin account
    Rewrite(Participant),
}

impl Sealed for RegistryInstruction {}
impl Pack for RegistryInstruction {
    const LEN: usize = 66; // see `test::get_packed_len()` for justification of "66"

    fn pack_into_slice(&self, dst: &mut [u8]) {
        let data = self.pack_into_vec();
        dst[..data.len()].copy_from_slice(&data);
    }

    fn unpack_from_slice(src: &[u8]) -> Result<Self, ProgramError> {
        let mut mut_src: &[u8] = src;
        Self::deserialize(&mut mut_src).map_err(|err| {
            msg!(
                "Error: failed to deserialize instruction: {}",
                err
            );
            ProgramError::InvalidInstructionData
        })
    }
}

impl RegistryInstruction {
    fn pack_into_vec(&self) -> Vec<u8> {
        self.try_to_vec().expect("try_to_vec")
    }
}

/// Create a `RegistryInstruction::Apply` instruction
pub fn apply(
    participant: Pubkey,
    mainnet_validator_identity: Pubkey,
    testnet_validator_identity: Pubkey,
) -> Instruction {
    Instruction {
        program_id: id(),
        accounts: vec![
            AccountMeta::new(participant, false),
            AccountMeta::new_readonly(mainnet_validator_identity, true),
            AccountMeta::new_readonly(testnet_validator_identity, true),
        ],
        data: RegistryInstruction::Apply.pack_into_vec(),
    }
}

/// Create a `RegistryInstruction::Withdraw` instruction
pub fn withdraw(participant: Pubkey, validator_identity: Pubkey, refundee: Pubkey) -> Instruction {
    Instruction {
        program_id: id(),
        accounts: vec![
            AccountMeta::new(participant, false),
            AccountMeta::new_readonly(validator_identity, true),
            AccountMeta::new(refundee, false),
        ],
        data: RegistryInstruction::Withdraw.pack_into_vec(),
    }
}

/// Create a `RegistryInstruction::Admin` instruction
pub fn approve(participant: Pubkey, admin: Pubkey) -> Instruction {
    Instruction {
        program_id: id(),
        accounts: vec![
            AccountMeta::new(participant, false),
            AccountMeta::new_readonly(admin, true),
        ],
        data: RegistryInstruction::Approve.pack_into_vec(),
    }
}

/// Create a `RegistryInstruction::Reject` instruction
pub fn reject(participant: Pubkey, admin: Pubkey) -> Instruction {
    Instruction {
        program_id: id(),
        accounts: vec![
            AccountMeta::new(participant, false),
            AccountMeta::new_readonly(admin, true),
        ],
        data: RegistryInstruction::Reject.pack_into_vec(),
    }
}

/// Create a `RegistryInstruction::Rewrite` instruction
pub fn rewrite(participant: Pubkey, admin: Pubkey, new_state: Participant) -> Instruction {
    Instruction {
        program_id: id(),
        accounts: vec![
            AccountMeta::new(participant, false),
            AccountMeta::new_readonly(admin, true),
        ],
        data: RegistryInstruction::Rewrite(new_state).pack_into_vec(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::state::ParticipantState, solana_program::pubkey::Pubkey};

    #[test]
    fn get_packed_len() {
        assert_eq!(
            RegistryInstruction::get_packed_len(),
            solana_program::borsh::get_packed_len::<RegistryInstruction>()
        )
    }
    #[test]
    fn serialize() {
        assert_eq!(RegistryInstruction::Apply.try_to_vec().unwrap(), vec![0]);
        assert_eq!(
            RegistryInstruction::Rewrite(Participant {
                state: ParticipantState::Approved,
                testnet_identity: Pubkey::new_unique(),
                mainnet_identity: Pubkey::new_unique()
            })
            .try_to_vec()
            .unwrap(),
            [
                4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 3
            ]
        );
    }

    #[test]
    fn deserialize() {
        assert_eq!(
            RegistryInstruction::unpack_from_slice(&[2]),
            Ok(RegistryInstruction::Approve),
        );
    }
}
