//! Program state
use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use solana_program::{
    msg,
    program_error::ProgramError,
    program_pack::{Pack, Sealed},
    pubkey::Pubkey,
};
use strum_macros::EnumString;

/// Participant states
///
/// The usual flow is as follows:
///
/// - Uninitialized
/// - SignupRequired
/// - TestnetWaitlist; (moved to this state after KYC passes and agreement is signed)
/// - ApprovedForTestnetOnly (groups of ~100 added per week in the order they were added to the TestnetWaitlist)
/// - ApprovedForTestnetAndMainnet (groups of ~100 added per week, prioritized based on performance)
///
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, BorshSchema, PartialEq, EnumString)]
pub enum ParticipantState {
    /// Default account state upon creation
    Uninitialized,

    /// The participant has applied and must go through the registration process (KYC, etc.)
    /// Previously this was the `Pending` state
    SignupRequired,

    /// The participant's application was rejected. Only admin can "fix" by changing the state back to `SignupRequired`
    /// Previously this was the `Rejected` state
    RejectedForTestnetAndMainnetRulesViolation,

    /// Participant is enrolled
    /// Previously this was the `Approved` state
    ApprovedForTestnetAndMainnet,

    /// Participant is approved for Testnet participation
    ApprovedForTestnetOnly,

    /// KYC, Agreement signing, or other prerequisite not met. To fix, the participant has to go through the signup process again.
    RejectedProgramSignupIncomplete,

    /// User passed KYC, but signup was flagged for review. This state indicates that someone at the Foundation has to review the signup.
    /// After review the ParticipantState would typically be moved to TestnetWaitlist if it passed review, or
    /// RejectedProgramSignupIncomplete if it failed review.
    SignupUnderReview,

    /// Participant is on the testnet waitlist. Typically, this is the state a participant will be put in after SignupRequired
    TestnetWaitlist,
}

impl Default for ParticipantState {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize, BorshSchema, PartialEq)]
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
            msg!("Error: failed to deserialize account: {}", err);
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
