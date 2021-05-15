//! Program state
use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use solana_program::{
    msg,
    program_error::ProgramError,
    program_pack::{Pack, Sealed},
    pubkey::Pubkey,
};

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, BorshSchema, PartialEq)]
pub enum ParticipantState {
    /// Default account state after creating it
    Uninitialized,

    /// The participant's application is pending
    Pending,

    /// The participant's application was rejected
    Rejected,

    /// Participant is enrolled
    Approved,

    /// Participant has withdraw from the program
    Withdrawn,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, BorshSchema, PartialEq)]
pub struct Participant {
    pub testnet_identity: Pubkey,
    pub mainnet_identity: Pubkey,
    pub state: ParticipantState,
}

impl Sealed for Participant {}

impl Pack for Participant {
    const LEN: usize = 65; // see `test::get_packed_len()` for justification of "73"

    fn pack_into_slice(&self, dst: &mut [u8]) {
        let data = self.try_to_vec().unwrap();
        dst[..data.len()].copy_from_slice(&data);
    }

    fn unpack_from_slice(src: &[u8]) -> Result<Self, ProgramError> {
        let mut mut_src: &[u8] = src;
        Self::deserialize(&mut mut_src).map_err(|err| {
            msg!(
                "Error: failed to deserialize feature proposal account: {}",
                err
            );
            ProgramError::InvalidAccountData
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_packed_len() {
        assert_eq!(
            Participant::get_packed_len(),
            solana_program::borsh::get_packed_len::<Participant>()
        );
    }
}
