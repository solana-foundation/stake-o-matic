//! Program instructions

use crate::state::Participant;
use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use solana_program::{
    msg,
    program_error::ProgramError,
    program_pack::{Pack, Sealed},
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
    /// On success the participant will be moved to the `ParticipantState::Withdrawn` state
    ///
    /// 0. `[writable]` `Participant` account in the `ParticipantState::Pending` or
    ///    `ParticipantState::Enrolled` state
    /// 1. `[signer]` Mainnet or Testnet validator identity
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
                "Error: failed to deserialize feature proposal instruction: {}",
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
