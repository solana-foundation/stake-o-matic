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
    let mut check_mainnet_testnet_eq = true;

    match instruction {
        RegistryInstruction::Apply => {
            msg!("Apply");
            if participant.state != ParticipantState::Uninitialized {
                msg!("Error: participant account is already initialized");
                return Err(ProgramError::AccountAlreadyInitialized);
            }

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
            let refundee_info = next_account_info(account_info_iter)?;
            check_mainnet_testnet_eq = false;

            if !identity_info.is_signer {
                msg!("Error: {} is not a signer", identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }

            if *identity_info.key != participant.testnet_identity
                && *identity_info.key != participant.mainnet_identity
            {
                msg!("Error: {} is not authorized", identity_info.key);
                return Err(ProgramError::MissingRequiredSignature);
            }

            **refundee_info.lamports.borrow_mut() += participant_info.lamports();
            **participant_info.lamports.borrow_mut() = 0;
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
            check_mainnet_testnet_eq = false;
        }
        RegistryInstruction::Rewrite(new_participant) => {
            msg!("Rewrite");
            authenticate_admin(next_account_info(account_info_iter)?)?;
            participant = new_participant;
        }
    }

    if check_mainnet_testnet_eq && participant.testnet_identity == participant.mainnet_identity {
        msg!("Error: mainnet and testnet identities must be unique",);
        Err(ProgramError::InvalidAccountData)
    } else {
        participant.pack_into_slice(&mut participant_info.data.borrow_mut());
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        assert_matches::assert_matches,
        solana_program::{system_instruction::create_account, sysvar::rent::Rent},
        solana_program_test::*,
        solana_sdk::{
            signature::{Keypair, Signer},
            transaction::Transaction,
        },
    };

    fn test_admin_keypair() -> Keypair {
        let keypair = Keypair::from_bytes(&[
            195, 121, 73, 133, 212, 8, 231, 45, 116, 99, 128, 66, 118, 174, 197, 26, 112, 146, 204,
            201, 119, 40, 97, 2, 86, 10, 98, 116, 235, 40, 163, 221, 60, 185, 28, 52, 69, 70, 108,
            96, 236, 253, 114, 203, 81, 219, 79, 136, 0, 185, 165, 101, 147, 67, 207, 255, 69, 83,
            242, 34, 36, 32, 80, 87,
        ])
        .unwrap();
        assert_eq!(keypair.pubkey(), test_admin::id());
        keypair
    }

    #[tokio::test]
    async fn test_signup() {
        let program_id = crate::id();

        let participant = Keypair::new();
        let mainnet_validator_identity = Keypair::new();
        let testnet_validator_identity = Keypair::new();

        let (mut banks_client, payer, recent_blockhash) = ProgramTest::new(
            "solana_foundation_delegation_program_registry",
            program_id,
            processor!(process_instruction),
        )
        .start()
        .await;

        let rent = Rent::default().minimum_balance(Participant::get_packed_len());

        // Create/Apply...
        let mut transaction = Transaction::new_with_payer(
            &[
                create_account(
                    &payer.pubkey(),
                    &participant.pubkey(),
                    rent,
                    Participant::get_packed_len() as u64,
                    &program_id,
                ),
                apply(
                    participant.pubkey(),
                    mainnet_validator_identity.pubkey(),
                    testnet_validator_identity.pubkey(),
                ),
            ],
            Some(&payer.pubkey()),
        );
        transaction.sign(
            &[
                &payer,
                &participant,
                &mainnet_validator_identity,
                &testnet_validator_identity,
            ],
            recent_blockhash,
        );
        assert_matches!(banks_client.process_transaction(transaction).await, Ok(()));

        let participant_state = banks_client
            .get_packed_account_data::<Participant>(participant.pubkey())
            .await
            .unwrap();
        assert_eq!(
            participant_state,
            Participant {
                state: ParticipantState::Pending,
                testnet_identity: testnet_validator_identity.pubkey(),
                mainnet_identity: mainnet_validator_identity.pubkey()
            }
        );

        // Cannot Apply twice...
        let mut transaction = Transaction::new_with_payer(
            &[apply(
                participant.pubkey(),
                mainnet_validator_identity.pubkey(),
                testnet_validator_identity.pubkey(),
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(
            &[
                &payer,
                &mainnet_validator_identity,
                &testnet_validator_identity,
            ],
            recent_blockhash,
        );
        assert_matches!(banks_client.process_transaction(transaction).await, Err(_));

        // Reject..
        let mut transaction = Transaction::new_with_payer(
            &[reject(participant.pubkey(), test_admin::id())],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &test_admin_keypair()], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Ok(()));

        assert_eq!(
            banks_client
                .get_packed_account_data::<Participant>(participant.pubkey())
                .await
                .unwrap()
                .state,
            ParticipantState::Rejected
        );

        // Approve...
        let mut transaction = Transaction::new_with_payer(
            &[approve(participant.pubkey(), test_admin::id())],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &test_admin_keypair()], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Ok(()));

        assert_eq!(
            banks_client
                .get_packed_account_data::<Participant>(participant.pubkey())
                .await
                .unwrap()
                .state,
            ParticipantState::Approved
        );

        // Approve with wrong admin key, failure...
        let mut transaction = Transaction::new_with_payer(
            &[approve(
                participant.pubkey(),
                testnet_validator_identity.pubkey(),
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &testnet_validator_identity], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Err(_));

        // Rewrite with wrong admin key, failure...
        let mut transaction = Transaction::new_with_payer(
            &[rewrite(
                participant.pubkey(),
                testnet_validator_identity.pubkey(),
                Participant::default(),
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &testnet_validator_identity], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Err(_));

        // Rewrite with duplicate identities, failure...
        let mut transaction = Transaction::new_with_payer(
            &[rewrite(
                participant.pubkey(),
                test_admin::id(),
                Participant {
                    state: ParticipantState::Pending,
                    testnet_identity: testnet_validator_identity.pubkey(),
                    mainnet_identity: testnet_validator_identity.pubkey(),
                },
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &test_admin_keypair()], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Err(_));

        // Rewrite...
        let mut transaction = Transaction::new_with_payer(
            &[rewrite(
                participant.pubkey(),
                test_admin::id(),
                Participant {
                    state: ParticipantState::Pending,
                    testnet_identity: testnet_validator_identity.pubkey(),
                    mainnet_identity: Pubkey::default(),
                },
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &test_admin_keypair()], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Ok(()));

        let participant_state = banks_client
            .get_packed_account_data::<Participant>(participant.pubkey())
            .await
            .unwrap();
        assert_eq!(
            participant_state,
            Participant {
                state: ParticipantState::Pending,
                testnet_identity: testnet_validator_identity.pubkey(),
                mainnet_identity: Pubkey::default(),
            }
        );

        // Withdraw...
        assert_eq!(
            banks_client
                .get_balance(testnet_validator_identity.pubkey())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            banks_client
                .get_balance(participant.pubkey())
                .await
                .unwrap(),
            rent
        );
        let mut transaction = Transaction::new_with_payer(
            &[withdraw(
                participant.pubkey(),
                testnet_validator_identity.pubkey(),
                testnet_validator_identity.pubkey(),
            )],
            Some(&payer.pubkey()),
        );
        transaction.sign(&[&payer, &testnet_validator_identity], recent_blockhash);
        assert_matches!(banks_client.process_transaction(transaction).await, Ok(()));

        assert_eq!(
            banks_client
                .get_balance(testnet_validator_identity.pubkey())
                .await
                .unwrap(),
            rent
        );
        assert_eq!(
            banks_client
                .get_balance(participant.pubkey())
                .await
                .unwrap(),
            0
        );
    }
}
