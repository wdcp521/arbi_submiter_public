use anchor_client::solana_sdk::instruction::Instruction;
use anchor_lang::prelude::{AccountMeta, Pubkey};
use std::str::FromStr;

pub const KAMINO_LENDING_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
pub const KAMINO_ADDITIONAL_COMPUTE_UNITS: u32 = 80_000;

pub struct FlashBorrowReserveLiquidity;

impl FlashBorrowReserveLiquidity {
    pub fn instruction_data(amount: u64) -> Vec<u8> {
        let mut data = vec![135, 231, 52, 167, 7, 52, 212, 193]; // Anchor discriminator for flashBorrowReserveLiquidity
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }
}

pub struct FlashRepayReserveLiquidity;

impl FlashRepayReserveLiquidity {
    pub fn instruction_data(amount: u64, borrow_instruction_index: u8) -> Vec<u8> {
        let mut data = vec![185, 117, 0, 203, 96, 245, 180, 186]; // Anchor discriminator for flashRepayReserveLiquidity
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(borrow_instruction_index);
        data
    }
}

fn get_account_vec(
    wallet_pk: &Pubkey,
    token_account: Pubkey,
    mint: Pubkey,
) -> anyhow::Result<(Vec<AccountMeta>, Pubkey)> {
    // LENDING_MARKET
    // LENDING_MARKET_AUTHORITY
    // RESERVE
    // RESERVE_LIQUIDITY
    // FEE_RECEIVER
    let (lending_market, lending_market_authority, reserve, reserve_liquidity, fee_receiver) =
        if mint.to_string() == "So11111111111111111111111111111111111111112" {
            (
                Pubkey::from_str("H6rHXmXoCQvq8Ue81MqNh7ow5ysPa1dSozwW3PU1dDH6").unwrap(),
                Pubkey::from_str("Dx8iy2o46sK1DzWbEcznqSKeLbLVeu7otkibA3WohGAj").unwrap(),
                Pubkey::from_str("6gTJfuPHEg6uRAijRkMqNc9kan4sVZejKMxmvx2grT1p").unwrap(),
                Pubkey::from_str("ywaaLvG7t1vXJo8sT3UzE8yzzZtxLM7Fmev64Jbooye").unwrap(),
                Pubkey::from_str("EQ7hw63aBS7aPQqXsoxaaBxiwbEzaAiY9Js6tCekkqxf").unwrap(),
            )
        } else if mint.to_string() == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
            (
                Pubkey::from_str("7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF").unwrap(),
                Pubkey::from_str("9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo").unwrap(),
                Pubkey::from_str("D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59").unwrap(),
                Pubkey::from_str("Bgq7trRgVMeq33yt235zM2onQ4bRDBsY5EWiTetF4qw6").unwrap(),
                Pubkey::from_str("BbDUrk1bVtSixgQsPLBJFZEF7mwGstnD5joA1WzYvYFX").unwrap(),
            )
        } else if mint.to_string() == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" {
            (
                Pubkey::from_str("7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF").unwrap(),
                Pubkey::from_str("9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo").unwrap(),
                Pubkey::from_str("H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S").unwrap(),
                Pubkey::from_str("2Eff8Udy2G2gzNcf2619AnTx3xM4renEv4QrHKjS1o9N").unwrap(),
                Pubkey::from_str("ARCZqsnUpvPffquPjZR3sxpvScLQdbfZ5BGf3SZvyij7").unwrap(),
            )
        } else {
            return Err(anyhow::anyhow!(
                "Unsupported mint for kamino flashloan: {}",
                mint
            ));
        };

    let kamino_program_id = Pubkey::from_str(KAMINO_LENDING_PROGRAM_ID)?;
    let referrer_token_state = Pubkey::from_str(KAMINO_LENDING_PROGRAM_ID)?;
    let referrer_account = Pubkey::from_str(KAMINO_LENDING_PROGRAM_ID)?;

    let accounts = vec![
        AccountMeta::new(*wallet_pk, true), // userTransferAuthority
        AccountMeta::new_readonly(lending_market_authority, false), // lendingMarketAuthority
        AccountMeta::new_readonly(lending_market, false), // lendingMarket
        AccountMeta::new(reserve, false),   // reserve
        AccountMeta::new_readonly(mint, false), // reserveLiquidityMint
        AccountMeta::new(reserve_liquidity, false), // reserveSourceLiquidity
        AccountMeta::new(token_account, false), // userDestinationLiquidity
        AccountMeta::new(fee_receiver, false), // reserveLiquidityFeeReceiver
        AccountMeta::new_readonly(referrer_token_state, false), // referrerTokenState
        AccountMeta::new_readonly(referrer_account, false), // referrerAccount
        AccountMeta::new_readonly(
            Pubkey::from_str("Sysvar1nstructions1111111111111111111111111").unwrap(),
            false,
        ), // sysvarInfo
        AccountMeta::new_readonly(
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
            false,
        ), // tokenProgram
    ];
    Ok((accounts, kamino_program_id))
}

pub fn get_kamino_flashloan_borrow_ix(
    wallet_pk: &Pubkey,
    token_account: Pubkey,
    mint: Pubkey,
    amount: u64,
) -> anyhow::Result<Instruction> {
    let (accounts, kamino_program_id) = get_account_vec(wallet_pk, token_account, mint)?;

    Ok(Instruction {
        program_id: kamino_program_id,
        accounts,
        data: FlashBorrowReserveLiquidity::instruction_data(amount),
    })
}

pub fn get_kamino_flashloan_repay_ix(
    wallet_pk: &Pubkey,
    token_account: Pubkey,
    mint: Pubkey,
    borrow_instruction_index: u8,
    amount: u64,
) -> anyhow::Result<Instruction> {
    let (accounts, kamino_program_id) = get_account_vec(wallet_pk, token_account, mint)?;

    Ok(Instruction {
        program_id: kamino_program_id,
        accounts,
        data: FlashRepayReserveLiquidity::instruction_data(amount, borrow_instruction_index),
    })
}
