# Solana Foundation Delegation Program Command-line Utility

This utility manages Solana Foundation Delegation Program registrations.

## Installation
```bash
$ su sol
$ cd
$ sudo apt install libudev-dev libssl-dev cargo pkg-config 
$ cargo install solana-foundation-delegation-program-cli
$ solana-foundation-delegation-program --version
```

## Usage

### New participant registration

To register you will need:
1. Two validator identity keypairs; one for the Solana Testnet and another for the Solana Mainnet
2. Approximately ◎0.002 to submit the registration


To begin, create your validator identity keypairs by running:
```bash
$ solana-keygen new -o testnet-validator-keypair.json
$ solana-keygen new -o mainnet-validator-keypair.json
```
Keep these keypairs safe; you cannot change them once you submit your
registration.

Confirm your balance is not empty,
```
$ solana -um balance
```
then run the following command to submit your registration:
```
$ solana-foundation-delegation-program apply --mainnet mainnet-validator-keypair.json --testnet testnet-validator-keypair.json
```

### Display your registration status
To view the status of your registration, run
```
$ solana-foundation-delegation-program status testnet-validator-keypair.json
```
or
```
$ solana-foundation-delegation-program status mainnet-validator-keypair.json
```

### Withdrawing your registration
If you wish to withdraw your registration, run
```
$ solana-foundation-delegation-program withdraw testnet-validator-keypair.json
```
or
```
$ solana-foundation-delegation-program withdraw mainnet-validator-keypair.json
```
Once withdrawn, your registration is deleted and cannot be recovered.
