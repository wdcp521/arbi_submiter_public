use anchor_client::{
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig},
    solana_sdk::{
        address_lookup_table::AddressLookupTableAccount,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::AccountMeta,
        message::{v0::Message, VersionedMessage},
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer as _,
        system_instruction::transfer,
        transaction::VersionedTransaction,
    },
    Program,
};
use anchor_lang::prelude::*;
use anyhow::Result;
use futures::future::join_all;
use log::{debug, error, trace};
use rand::{rngs::OsRng, seq::SliceRandom};
use reqwest::Client as ReqwestClient;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
declare_program!(sol_arbitrage);
use sol_arbitrage::{client::accounts::Arbi, client::args::Arbi as ArbiArgs};

use crate::submiter::kamino::{
    get_kamino_flashloan_borrow_ix, get_kamino_flashloan_repay_ix, KAMINO_ADDITIONAL_COMPUTE_UNITS,
};

const PROGRAM_PUBKEY_STR: &str = "";
const BASE_GAS: u64 = 5_000;
const PROXY_PRESERVED_BALANCE: u64 = 1_000_000;

const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

const JITO_ENDPOINTS: [&str; 6] = [
    "https://slc.mainnet.block-engine.jito.wtf",
    "https://amsterdam.mainnet.block-engine.jito.wtf",
    "https://frankfurt.mainnet.block-engine.jito.wtf",
    "https://ny.mainnet.block-engine.jito.wtf",
    "https://tokyo.mainnet.block-engine.jito.wtf",
    "https://london.mainnet.block-engine.jito.wtf",
];
const UNIT_LIMIT: u32 = 300_000;
const JITO_TIMEOUT: u64 = 3;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DexType {
    RaydiumAmm = 1,
    RaydiumCpmm = 2,
    RaydiumClmm = 3,
    Orca = 4,
    MeteoraDlmm = 5,
    MeteoraAmm = 6,
    Solfi = 7,
    Lifinity = 8,
    Phoenix = 9,
    Pump = 10,
    Obric = 11,
    Openbook = 12,
    JupPerp = 13,
    MeteoraCpam = 14,
}

impl DexType {
    fn to_u8(&self) -> u8 {
        match self {
            DexType::RaydiumAmm => 1,
            DexType::RaydiumCpmm => 2,
            DexType::RaydiumClmm => 3,
            DexType::Orca => 4,
            DexType::MeteoraDlmm => 5,
            DexType::MeteoraAmm => 6,
            DexType::Solfi => 7,
            DexType::Lifinity => 8,
            DexType::Phoenix => 9,
            DexType::Pump => 10,
            DexType::Obric => 11,
            DexType::Openbook => 12,
            DexType::JupPerp => 13,
            DexType::MeteoraCpam => 14,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct JitoError {
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct BundleResponse {
    id: String,
    result: Option<String>,
    error: Option<JitoError>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionDetail {
    #[serde(default)]
    simulate: bool,
    min_profit: f64,
    jito_tip_ratio: u8,
    jito_tip: f64,
    priority_fee: f64,
    #[serde(default)]
    pub use_proxy_account: bool,
    #[serde(default)]
    pub use_kamino: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonAccounts {
    token_vault_a_mint: String,
    token_vault_b_mint: String,
    vault: String,
    user_token_account_a: String,
    user_token_account_b: String,
}

trait AccountMetaFormattable {
    fn to_account_metas(&self, direction: bool) -> Vec<AccountMeta>;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaydiumAmmAccounts {
    raydium_amm: String,
    raydium_amm_open_orders: String,
    raydium_amm_authority: String,
    raydium_amm_coin_vault: String,
    raydium_amm_pc_vault: String,
    raydium_amm_market: String,
    raydium_amm_market_bids: String,
    raydium_amm_market_asks: String,
    raydium_amm_market_event_queue: String,
    raydium_amm_market_coin_vault: String,
    raydium_amm_market_pc_vault: String,
    raydium_amm_market_vault_signer: String,
}

impl AccountMetaFormattable for RaydiumAmmAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(Pubkey::from_str(&self.raydium_amm).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_open_orders).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.raydium_amm_authority).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_coin_vault).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.raydium_amm_pc_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.raydium_amm_market).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_bids).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_asks).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_event_queue).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_coin_vault).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_pc_vault).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_amm_market_vault_signer).unwrap(),
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaydiumClmmAccounts {
    pub raydium_amm_config: String,
    pub raydium_pool_state: String,
    pub raydium_input_vault: String,
    pub raydium_output_vault: String,
    pub raydium_observation_state: String,
    pub raydium_a_to_b_tick_array_0: Option<String>,
    pub raydium_a_to_b_tick_array_1: Option<String>,
    pub raydium_a_to_b_tick_array_2: Option<String>,
    pub raydium_b_to_a_tick_array_0: Option<String>,
    pub raydium_b_to_a_tick_array_1: Option<String>,
    pub raydium_b_to_a_tick_array_2: Option<String>,
}

impl AccountMetaFormattable for RaydiumClmmAccounts {
    fn to_account_metas(&self, direction: bool) -> Vec<AccountMeta> {
        let (tick_array_0, tick_array_1, tick_array_2) = if direction {
            (
                self.raydium_a_to_b_tick_array_0.as_ref().unwrap(),
                self.raydium_a_to_b_tick_array_1.as_ref().unwrap(),
                self.raydium_a_to_b_tick_array_2.as_ref().unwrap(),
            )
        } else {
            (
                self.raydium_b_to_a_tick_array_0.as_ref().unwrap(),
                self.raydium_b_to_a_tick_array_1.as_ref().unwrap(),
                self.raydium_b_to_a_tick_array_2.as_ref().unwrap(),
            )
        };
        vec![
            AccountMeta::new_readonly(Pubkey::from_str(&self.raydium_amm_config).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.raydium_pool_state).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.raydium_input_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.raydium_output_vault).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.raydium_observation_state).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&tick_array_0).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_1).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_2).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaydiumCpmmAccounts {
    authority: String,
    amm_config: String,
    pool_state: String,
    input_vault: String,
    output_vault: String,
    observation_state: String,
}

impl AccountMetaFormattable for RaydiumCpmmAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(Pubkey::from_str(&self.authority).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.amm_config).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.pool_state).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.input_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.output_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.observation_state).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrcaAccounts {
    pub whirlpool: String,
    pub whirlpool_token_vault_a: String,
    pub whirlpool_token_vault_b: String,
    pub whirlpool_a_to_b_tick_array_0: Option<String>,
    pub whirlpool_a_to_b_tick_array_1: Option<String>,
    pub whirlpool_a_to_b_tick_array_2: Option<String>,
    pub whirlpool_b_to_a_tick_array_0: Option<String>,
    pub whirlpool_b_to_a_tick_array_1: Option<String>,
    pub whirlpool_b_to_a_tick_array_2: Option<String>,
    pub whirlpool_oracle: String,
}

impl AccountMetaFormattable for OrcaAccounts {
    fn to_account_metas(&self, direction: bool) -> Vec<AccountMeta> {
        let (tick_array_0, tick_array_1, tick_array_2) = if direction {
            (
                self.whirlpool_a_to_b_tick_array_0.as_ref().unwrap(),
                self.whirlpool_a_to_b_tick_array_1.as_ref().unwrap(),
                self.whirlpool_a_to_b_tick_array_2.as_ref().unwrap(),
            )
        } else {
            (
                self.whirlpool_b_to_a_tick_array_0.as_ref().unwrap(),
                self.whirlpool_b_to_a_tick_array_1.as_ref().unwrap(),
                self.whirlpool_b_to_a_tick_array_2.as_ref().unwrap(),
            )
        };
        vec![
            AccountMeta::new(Pubkey::from_str(&self.whirlpool).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.whirlpool_token_vault_a).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.whirlpool_token_vault_b).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&tick_array_0).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_1).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_2).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.whirlpool_oracle).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeteoraDlmmAccounts {
    pub meteora_lb_pair: String,
    pub meteora_reserve_x: String,
    pub meteora_reserve_y: String,
    pub meteora_oracle: String,
    pub meteora_event_authority: String,
    pub meteora_a_to_b_tick_array_0: Option<String>,
    pub meteora_a_to_b_tick_array_1: Option<String>,
    pub meteora_a_to_b_tick_array_2: Option<String>,
    pub meteora_b_to_a_tick_array_0: Option<String>,
    pub meteora_b_to_a_tick_array_1: Option<String>,
    pub meteora_b_to_a_tick_array_2: Option<String>,
}

impl AccountMetaFormattable for MeteoraDlmmAccounts {
    fn to_account_metas(&self, direction: bool) -> Vec<AccountMeta> {
        let (tick_array_0, tick_array_1, tick_array_2) = if direction {
            (
                self.meteora_a_to_b_tick_array_0.as_ref().unwrap(),
                self.meteora_a_to_b_tick_array_1.as_ref().unwrap(),
                self.meteora_a_to_b_tick_array_2.as_ref().unwrap(),
            )
        } else {
            (
                self.meteora_b_to_a_tick_array_0.as_ref().unwrap(),
                self.meteora_b_to_a_tick_array_1.as_ref().unwrap(),
                self.meteora_b_to_a_tick_array_2.as_ref().unwrap(),
            )
        };
        vec![
            AccountMeta::new(Pubkey::from_str(&self.meteora_lb_pair).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.meteora_reserve_x).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.meteora_reserve_y).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.meteora_oracle).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.meteora_event_authority).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&tick_array_0).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_1).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&tick_array_2).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeteoraAmmAccounts {
    pub pool: String,
    pub a_vault: String,
    pub b_vault: String,
    pub a_token_vault: String,
    pub b_token_vault: String,
    pub a_vault_lp: String,
    pub b_vault_lp: String,
    pub a_vault_lp_mint: String,
    pub b_vault_lp_mint: String,
    pub protocol_token_a_fee: String,
    pub protocol_token_b_fee: String,
}
impl AccountMetaFormattable for MeteoraAmmAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(Pubkey::from_str(&self.pool).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.a_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.b_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.a_token_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.b_token_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.a_vault_lp_mint).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.b_vault_lp_mint).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.a_vault_lp).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.b_vault_lp).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.protocol_token_a_fee).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.protocol_token_b_fee).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SolfiAccounts {
    pub solfi_pair: String,
    pub solfi_pool_token_a: String,
    pub solfi_pool_token_b: String,
}
impl AccountMetaFormattable for SolfiAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(Pubkey::from_str(&self.solfi_pair).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.solfi_pool_token_a).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.solfi_pool_token_b).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifinityAccounts {
    pub lifinity_authority: String,
    pub lifinity_amm: String,
    pub lifinity_swap_source: String,
    pub lifinity_swap_destination: String,
    pub lifinity_pool_mint: String,
    pub lifinity_fee_account: String,
    pub lifinity_oracle_main_account: String,
    pub lifinity_oracle_sub_account: String,
    pub lifinity_oracle_pc_account: String,
}
impl AccountMetaFormattable for LifinityAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(Pubkey::from_str(&self.lifinity_authority).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.lifinity_amm).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.lifinity_swap_source).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.lifinity_swap_destination).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.lifinity_pool_mint).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.lifinity_fee_account).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.lifinity_oracle_main_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.lifinity_oracle_sub_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.lifinity_oracle_pc_account).unwrap(),
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhoenixAccounts {
    pub phoenix_log_authority: String,
    pub phoenix_market: String,
    pub phoenix_base_vault: String,
    pub phoenix_quote_vault: String,
}
impl AccountMetaFormattable for PhoenixAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.phoenix_log_authority).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.phoenix_market).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.phoenix_base_vault).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.phoenix_quote_vault).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PumpAccounts {
    pub pump_pool: String,
    pub pump_global_config: String,
    pub pump_pool_base_token_account: String,
    pub pump_pool_quote_token_account: String,
    pub pump_protocol_fee_recipient: String,
    pub pump_protocol_fee_recipient_token_account: String,
    pub pump_event_authority: String,
    pub pump_coin_creator_vault_ata: String,
    pub pump_coin_creator_vault_authority: String,
}
impl AccountMetaFormattable for PumpAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(Pubkey::from_str(&self.pump_pool).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.pump_global_config).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.pump_pool_base_token_account).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.pump_pool_quote_token_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.pump_protocol_fee_recipient).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.pump_protocol_fee_recipient_token_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(Pubkey::from_str(&self.pump_event_authority).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.pump_coin_creator_vault_ata).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.pump_coin_creator_vault_authority).unwrap(),
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObricAccounts {
    pub obric_trading_pair: String,
    pub obric_mint_x: String,
    pub obric_mint_y: String,
    pub obric_reserve_x: String,
    pub obric_reserve_y: String,
    pub obric_protocol_fee: String,
    pub obric_x_price_feed: String,
    pub obric_y_price_feed: String,
}
impl AccountMetaFormattable for ObricAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(Pubkey::from_str(&self.obric_trading_pair).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.obric_mint_x).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.obric_mint_y).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.obric_reserve_x).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.obric_reserve_y).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.obric_protocol_fee).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.obric_x_price_feed).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.obric_y_price_feed).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenbookAccounts {
    pub openbook_market: String,
    pub openbook_market_authority: String,
    pub openbook_bids: String,
    pub openbook_asks: String,
    pub openbook_market_base_vault: String,
    pub openbook_market_quote_vault: String,
    pub openbook_event_heap: String,
    pub openbook_oracle_a: String,
    pub openbook_oracle_b: String,
    pub openbook_open_orders_admin: String,
}
impl AccountMetaFormattable for OpenbookAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(Pubkey::from_str(&self.openbook_market).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.openbook_market_authority).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.openbook_bids).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.openbook_asks).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.openbook_market_base_vault).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.openbook_market_quote_vault).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.openbook_event_heap).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(if self.openbook_oracle_a.len() == 0 {
                    PROGRAM_PUBKEY_STR
                } else {
                    &self.openbook_oracle_a
                })
                .unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(if self.openbook_oracle_b.len() == 0 {
                    PROGRAM_PUBKEY_STR
                } else {
                    &self.openbook_oracle_b
                })
                .unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(if self.openbook_open_orders_admin.len() == 0 {
                    PROGRAM_PUBKEY_STR
                } else {
                    &self.openbook_open_orders_admin
                })
                .unwrap(),
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JupPerpAccounts {
    pub transfer_authority: String,
    pub perpetuals: String,
    pub pool: String,
    pub receiving_custody: String,
    pub receiving_custody_doves_price_account: String,
    pub receiving_custody_pythnet_price_account: String,
    pub receiving_custody_token_account: String,
    pub dispensing_custody: String,
    pub dispensing_custody_doves_price_account: String,
    pub dispensing_custody_pythnet_price_account: String,
    pub dispensing_custody_token_account: String,
    pub event_authority: String,
}
impl AccountMetaFormattable for JupPerpAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(Pubkey::from_str(&self.transfer_authority).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(&self.perpetuals).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.pool).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(&self.receiving_custody).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.receiving_custody_doves_price_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.receiving_custody_pythnet_price_account).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.receiving_custody_token_account).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.dispensing_custody).unwrap(), false),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.dispensing_custody_doves_price_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.dispensing_custody_pythnet_price_account).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.dispensing_custody_token_account).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(Pubkey::from_str(&self.event_authority).unwrap(), false),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeteoraCpamAccounts {
    pub meteora_cpam_pool_authority: String,
    pub meteora_cpam_pool: String,
    pub meteora_cpam_token_a_vault: String,
    pub meteora_cpam_token_b_vault: String,
    pub meteora_cpam_event_authority: String,
}
impl AccountMetaFormattable for MeteoraCpamAccounts {
    fn to_account_metas(&self, _: bool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.meteora_cpam_pool_authority).unwrap(),
                false,
            ),
            AccountMeta::new(Pubkey::from_str(&self.meteora_cpam_pool).unwrap(), false),
            AccountMeta::new(
                Pubkey::from_str(&self.meteora_cpam_token_a_vault).unwrap(),
                false,
            ),
            AccountMeta::new(
                Pubkey::from_str(&self.meteora_cpam_token_b_vault).unwrap(),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::from_str(&self.meteora_cpam_event_authority).unwrap(),
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DexAccount {
    MeteoraDlmm(MeteoraDlmmAccounts),
    RaydiumAmm(RaydiumAmmAccounts),
    RaydiumClmm(RaydiumClmmAccounts),
    RaydiumCpmm(RaydiumCpmmAccounts),
    Orca(OrcaAccounts),
    MeteoraAmm(MeteoraAmmAccounts),
    Solfi(SolfiAccounts),
    Lifinity(LifinityAccounts),
    Phoenix(PhoenixAccounts),
    Pump(PumpAccounts),
    Obric(ObricAccounts),
    Openbook(OpenbookAccounts),
    JupPerp(JupPerpAccounts),
    MeteoraCpam(MeteoraCpamAccounts),
}

impl DexAccount {
    fn to_account_metas(&self, direction: bool) -> Vec<AccountMeta> {
        match self {
            DexAccount::MeteoraDlmm(accounts) => accounts.to_account_metas(direction),
            DexAccount::RaydiumAmm(accounts) => accounts.to_account_metas(direction),
            DexAccount::RaydiumClmm(accounts) => accounts.to_account_metas(direction),
            DexAccount::RaydiumCpmm(accounts) => accounts.to_account_metas(direction),
            DexAccount::Orca(accounts) => accounts.to_account_metas(direction),
            DexAccount::MeteoraAmm(accounts) => accounts.to_account_metas(direction),
            DexAccount::Solfi(accounts) => accounts.to_account_metas(direction),
            DexAccount::Lifinity(accounts) => accounts.to_account_metas(direction),
            DexAccount::Phoenix(accounts) => accounts.to_account_metas(direction),
            DexAccount::Pump(accounts) => accounts.to_account_metas(direction),
            DexAccount::Obric(accounts) => accounts.to_account_metas(direction),
            DexAccount::Openbook(accounts) => accounts.to_account_metas(direction),
            DexAccount::JupPerp(accounts) => accounts.to_account_metas(direction),
            DexAccount::MeteoraCpam(accounts) => accounts.to_account_metas(direction),
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapAccounts {
    common_accounts: CommonAccounts,
    dexes: Vec<DexAccount>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbiEvent {
    pub accounts: SwapAccounts,
    pub dex_types: Vec<DexType>,
    pub decimals: Vec<u8>,
    pub transaction: TransactionDetail,
    pub same_a_b: Vec<bool>,
    pub input_amounts: Vec<f64>,
    pub reverse_input_amounts: Vec<f64>,
    pub token_output_amounts: Vec<u64>,
    pub is_token_b_2022: bool,
    pub blockhash: String,
    pub trace_id: String,
    pub submit_count: u64,
    pub stream_ts: i64,
}

pub struct TransactionHelpers<'info> {
    pub alt_account: Arc<AddressLookupTableAccount>,
    pub program: Arc<Program<&'info Keypair>>,
    pub connection: Arc<RpcClient>,
    pub wallet: &'info Keypair,
}

async fn send_bundle_using_jito(
    transaction_list: Vec<VersionedTransaction>,
    endpoint: &str,
    client: Arc<ReqwestClient>,
    dex_types: Vec<DexType>,
    trace_id: String,
) -> Result<String> {
    let mut signatures = vec![];
    let transactions: Vec<String> = transaction_list
        .into_iter()
        .map(|transaction| {
            let signature_vec = bs58::encode(&transaction.signatures[0]).into_vec();
            let signature = String::from_utf8(signature_vec).unwrap();
            signatures.push(signature);
            let serialized_tx = bincode::serialize(&transaction).unwrap();
            let encoded_bytes = bs58::encode(&serialized_tx).into_vec();
            let encoded_tx = String::from_utf8(encoded_bytes).unwrap();
            encoded_tx
        })
        .collect();
    let signature = signatures.get(0).unwrap();

    let payload = json!({
        "jsonrpc": "2.0",
        "id": signature,
        "method": "sendBundle",
        "params": [transactions],
    });

    let jito_bundles_uri = "/api/v1/bundles";
    let url = format!("{}{}", endpoint, jito_bundles_uri);

    trace!("3 send_bundle_using_jito end payload {}", endpoint);
    let res = client
        .post(&url)
        .json(&payload)
        .timeout(Duration::from_secs(JITO_TIMEOUT))
        .send()
        .await?;

    trace!(
        "4 send_bundle_using_jito end ReqwestClient client {}",
        endpoint
    );

    let json: BundleResponse = res.json().await?;
    if let Some(error) = json.error {
        debug!(
            "Error: {}, dex_types:{:?}, trace_id:{}",
            error.message, dex_types, trace_id
        );
        return Ok("".to_string());
    }

    debug!("Sent jito bundle to region {}: {}", endpoint, signature);

    Ok(json.result.unwrap_or_default())
}

pub async fn assemble_and_submit_transaction<'info>(
    arbi_event: ArbiEvent,
    transaction_helpers: TransactionHelpers<'info>,
    request_client: Arc<ReqwestClient>,
) -> Result<()> {
    let start = SystemTime::now();
    debug!("Start: {}", start.elapsed().unwrap().as_millis());
    let mut remaining_accounts: Vec<AccountMeta> =
        vec![AccountMeta::new_readonly(Pubkey::from_str(PROGRAM_PUBKEY_STR).unwrap(), false); 16];

    let (user_token_account_a, user_token_account_b) = if arbi_event.transaction.use_kamino {
        (
            spl_associated_token_account::get_associated_token_address(
                &transaction_helpers.wallet.pubkey(),
                &Pubkey::from_str(&arbi_event.accounts.common_accounts.token_vault_a_mint).unwrap(),
            ),
            spl_associated_token_account::get_associated_token_address(
                &transaction_helpers.wallet.pubkey(),
                &Pubkey::from_str(&arbi_event.accounts.common_accounts.token_vault_b_mint).unwrap(),
            ),
        )
    } else {
        (
            Pubkey::from_str(&arbi_event.accounts.common_accounts.user_token_account_a).unwrap(),
            Pubkey::from_str(&arbi_event.accounts.common_accounts.user_token_account_b).unwrap(),
        )
    };

    let mut accounts = Arbi {
        associated_token_program: Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
            .unwrap(),
        system_program: Pubkey::from_str("11111111111111111111111111111111").unwrap(),
        jito_tip_account: transaction_helpers.wallet.pubkey(),
        payer: transaction_helpers.wallet.pubkey(),
        token_program: Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
        token_b_program: if arbi_event.is_token_b_2022 {
            Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap()
        } else {
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap()
        },
        token_program_2022: None,
        memo_program: None,
        instructions_sysvar: None,
        vault: Pubkey::from_str(&arbi_event.accounts.common_accounts.vault).unwrap(),
        token_vault_a_mint: Pubkey::from_str(
            &arbi_event.accounts.common_accounts.token_vault_a_mint,
        )
        .unwrap(),
        token_vault_b_mint: Pubkey::from_str(
            &arbi_event.accounts.common_accounts.token_vault_b_mint,
        )
        .unwrap(),
        user_token_account_a,
        user_token_account_b,
    };

    if arbi_event.is_token_b_2022 {
        accounts.token_program_2022 =
            Some(Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap());
    }

    if arbi_event.transaction.jito_tip_ratio != 0 {
        accounts.jito_tip_account =
            Pubkey::from_str(JITO_TIP_ACCOUNTS.choose(&mut OsRng).unwrap()).unwrap();
    }

    for (index, dex_type) in arbi_event.dex_types.iter().enumerate() {
        let direction = !(arbi_event.same_a_b[index] ^ (index < arbi_event.input_amounts.len()));
        remaining_accounts.extend(arbi_event.accounts.dexes[index].to_account_metas(direction));
        match dex_type.clone() {
            DexType::RaydiumAmm => {
                remaining_accounts[0] = AccountMeta::new_readonly(
                    Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
                    false,
                );
                remaining_accounts[1] = AccountMeta::new_readonly(
                    Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap(),
                    false,
                );
            }
            DexType::RaydiumCpmm => {
                remaining_accounts[2] = AccountMeta::new_readonly(
                    Pubkey::from_str("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C").unwrap(),
                    false,
                );
                accounts.token_program_2022 =
                    Some(Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap());
            }
            DexType::RaydiumClmm => {
                remaining_accounts[3] = AccountMeta::new_readonly(
                    Pubkey::from_str("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(),
                    false,
                );
                accounts.memo_program =
                    Some(Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap());
                accounts.token_program_2022 =
                    Some(Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap());
            }
            DexType::Orca => {
                remaining_accounts[4] = AccountMeta::new_readonly(
                    Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap(),
                    false,
                );
            }
            DexType::MeteoraDlmm => {
                remaining_accounts[5] = AccountMeta::new_readonly(
                    Pubkey::from_str("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo").unwrap(),
                    false,
                );
            }
            DexType::MeteoraAmm => {
                remaining_accounts[6] = AccountMeta::new_readonly(
                    Pubkey::from_str("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB").unwrap(),
                    false,
                );
                remaining_accounts[7] = AccountMeta::new_readonly(
                    Pubkey::from_str("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi").unwrap(),
                    false,
                );
            }

            DexType::Solfi => {
                accounts.instructions_sysvar =
                    Some(Pubkey::from_str("Sysvar1nstructions1111111111111111111111111").unwrap());
                remaining_accounts[8] = AccountMeta::new_readonly(
                    Pubkey::from_str("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe").unwrap(),
                    false,
                );
            }
            DexType::Lifinity => {
                remaining_accounts[9] = AccountMeta::new_readonly(
                    Pubkey::from_str("2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c").unwrap(),
                    false,
                );
            }
            DexType::Phoenix => {
                remaining_accounts[10] = AccountMeta::new_readonly(
                    Pubkey::from_str("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY").unwrap(),
                    false,
                );
            }
            DexType::Pump => {
                remaining_accounts[11] = AccountMeta::new_readonly(
                    Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").unwrap(),
                    false,
                );
            }
            DexType::Obric => {
                remaining_accounts[12] = AccountMeta::new_readonly(
                    Pubkey::from_str("obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y").unwrap(),
                    false,
                );
            }
            DexType::Openbook => {
                remaining_accounts[13] = AccountMeta::new_readonly(
                    Pubkey::from_str("opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb").unwrap(),
                    false,
                );
            }
            DexType::JupPerp => {
                remaining_accounts[14] = AccountMeta::new_readonly(
                    Pubkey::from_str("PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu").unwrap(),
                    false,
                );
            }
            DexType::MeteoraCpam => {
                remaining_accounts[15] = AccountMeta::new_readonly(
                    Pubkey::from_str("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG").unwrap(),
                    false,
                );
            }
        }
    }

    let mut account_metas = accounts.to_account_metas(None);
    account_metas.extend(remaining_accounts);

    let amount_per_token = match arbi_event
        .accounts
        .common_accounts
        .token_vault_a_mint
        .as_str()
    {
        "So11111111111111111111111111111111111111112" => LAMPORTS_PER_SOL,
        "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn" => LAMPORTS_PER_SOL,
        "5XZw2LKTyrfvfiskJ78AMpackRjPcyCif1WhUsPDuVqQ" => 100000000,
        _ => 1000000,
    };

    let inputs_size = arbi_event.input_amounts.len();
    let min_profit = (arbi_event.transaction.min_profit * amount_per_token as f64) as i64;
    let args = ArbiArgs {
        use_pda_vault: !arbi_event.transaction.use_kamino,
        dex_type_list: arbi_event.dex_types.iter().map(|dt| dt.to_u8()).collect(),
        same_ab_list: arbi_event.same_a_b.clone(),
        token_a_amount_list: arbi_event
            .input_amounts
            .iter()
            .enumerate()
            .map(|(i, amount)| (amount * 10u64.pow(arbi_event.decimals[i] as u32) as f64) as u64)
            .collect(),
        token_b_amount_list: arbi_event
            .reverse_input_amounts
            .iter()
            .enumerate()
            .map(|(i, amount)| {
                (amount * 10u64.pow(arbi_event.decimals[i + inputs_size] as u32) as f64) as u64
            })
            .collect(),
        token_output_amount_list: arbi_event.token_output_amounts,
        token_b_2022: arbi_event.is_token_b_2022,
        min_profit,
        jito_tip_ratio: arbi_event.transaction.jito_tip_ratio,
    };

    let swap_instruction = transaction_helpers
        .program
        .request()
        .accounts(account_metas)
        .args(args)
        .instructions()
        .unwrap()
        .get(0)
        .unwrap()
        .clone();

    let jito_slice_start;
    let jito_slice_end;

    let using_jito =
        arbi_event.transaction.jito_tip_ratio > 0 || arbi_event.transaction.jito_tip > 0.0;

    if using_jito {
        let jito_slice = std::env::var("JITO_SLICE").unwrap_or_else(|_| "0,5".to_string());
        let parts: Vec<usize> = jito_slice
            .split(',')
            .map(|s| s.trim().parse::<usize>().expect("Invalid number"))
            .collect();
        jito_slice_start = parts[0];
        jito_slice_end = parts[1];
    } else {
        jito_slice_start = 0;
        jito_slice_end = 1;
    };
    let jito_endpoints = JITO_ENDPOINTS[jito_slice_start..jito_slice_end].to_vec();

    let transactions: Vec<Vec<VersionedTransaction>> = jito_endpoints
        .into_iter()
        .enumerate()
        .map(|(i, _)| {
            let mut instructions = vec![ComputeBudgetInstruction::set_compute_unit_limit(
                UNIT_LIMIT
                    + if arbi_event.transaction.use_kamino {
                        KAMINO_ADDITIONAL_COMPUTE_UNITS
                    } else {
                        0
                    }
                    + i as u32,
            )];

            if arbi_event.transaction.priority_fee > 0.0 {
                let micro_lamports =
                    (arbi_event.transaction.priority_fee as f64 * 10f64.powi(9) * 3.3).ceil()
                        as u64;
                instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                    micro_lamports,
                ));
            }

            let kamino_borrow_amount = arbi_event
                .input_amounts
                .iter()
                .enumerate()
                .map(|(i, amount)| {
                    (amount * 10u64.pow(arbi_event.decimals[i] as u32) as f64) as u64
                })
                .sum();

            if arbi_event.transaction.use_kamino {
                instructions.push(
                    get_kamino_flashloan_borrow_ix(
                        &transaction_helpers.wallet.pubkey(),
                        user_token_account_a,
                        Pubkey::from_str(&arbi_event.accounts.common_accounts.token_vault_a_mint)
                            .unwrap(),
                        kamino_borrow_amount,
                    )
                    .unwrap(),
                );
            }

            instructions.push(swap_instruction.clone());
            let proxy_wallet = Keypair::new();

            if arbi_event.transaction.jito_tip > 0.0 {
                let jito_tip_amount =
                    (arbi_event.transaction.jito_tip * LAMPORTS_PER_SOL as f64).floor() as u64;
                if arbi_event.transaction.use_proxy_account {
                    instructions.push(transfer(
                        &transaction_helpers.wallet.pubkey(),
                        &proxy_wallet.pubkey(),
                        jito_tip_amount + PROXY_PRESERVED_BALANCE,
                    ));
                } else {
                    let jito_tip_account =
                        Pubkey::from_str(JITO_TIP_ACCOUNTS.choose(&mut OsRng).unwrap()).unwrap();
                    instructions.push(transfer(
                        &transaction_helpers.wallet.pubkey(),
                        &jito_tip_account,
                        jito_tip_amount,
                    ));
                }
            }

            if arbi_event.transaction.use_kamino {
                instructions.push(
                    get_kamino_flashloan_repay_ix(
                        &transaction_helpers.wallet.pubkey(),
                        user_token_account_a,
                        Pubkey::from_str(&arbi_event.accounts.common_accounts.token_vault_a_mint)
                            .unwrap(),
                        if arbi_event.transaction.priority_fee > 0.0 {
                            2
                        } else {
                            1
                        },
                        kamino_borrow_amount,
                    )
                    .unwrap(),
                );
            }

            let message = Message::try_compile(
                &transaction_helpers.wallet.pubkey(),
                &instructions,
                &[transaction_helpers.alt_account.as_ref().clone()],
                Hash::from_str(&arbi_event.blockhash).unwrap(),
            )
            .unwrap();

            let tx1 = VersionedTransaction::try_new(
                VersionedMessage::V0(message),
                &[&transaction_helpers.wallet],
            )
            .unwrap();
            let mut transaction_vec = vec![tx1];

            if arbi_event.transaction.jito_tip > 0.0 && arbi_event.transaction.use_proxy_account {
                let jito_tip_account =
                    Pubkey::from_str(JITO_TIP_ACCOUNTS.choose(&mut OsRng).unwrap()).unwrap();
                let jito_tip_amount =
                    (arbi_event.transaction.jito_tip * LAMPORTS_PER_SOL as f64).floor() as u64;

                let tx2_instructions = vec![
                    transfer(&proxy_wallet.pubkey(), &jito_tip_account, jito_tip_amount),
                    transfer(
                        &proxy_wallet.pubkey(),
                        &transaction_helpers.wallet.pubkey(),
                        PROXY_PRESERVED_BALANCE - BASE_GAS,
                    ),
                ];

                let tx2_message = Message::try_compile(
                    &proxy_wallet.pubkey(),
                    &tx2_instructions,
                    &[],
                    Hash::from_str(&arbi_event.blockhash).unwrap(),
                )
                .unwrap();

                let tx2 = VersionedTransaction::try_new(
                    VersionedMessage::V0(tx2_message),
                    &[&proxy_wallet],
                )
                .unwrap();
                transaction_vec.push(tx2);
            }
            transaction_vec
        })
        .collect();
    debug!(
        "trace_id: {}, assemble duration: {}",
        arbi_event.trace_id,
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );

    if arbi_event.transaction.simulate {
        let result = transaction_helpers
            .connection
            .simulate_transaction(transactions.get(0).unwrap().get(0).unwrap())
            .await
            .unwrap();
        debug!("simulate_transaction: {:#?}", result);
    } else if using_jito {
        let futures = JITO_ENDPOINTS[jito_slice_start..jito_slice_end]
            .into_iter()
            .zip(transactions.into_iter())
            .map(|(endpoint, transaction_vec)| {
                let request_client_clone = request_client.clone();
                let dex_types_clone = arbi_event.dex_types.clone();
                let trace_id_clone = arbi_event.trace_id.clone();
                tokio::spawn(async move {
                    send_bundle_using_jito(
                        transaction_vec,
                        endpoint,
                        request_client_clone,
                        dex_types_clone,
                        trace_id_clone,
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();

        let result: Vec<String> = join_all(futures)
            .await
            .into_iter()
            .map(|result| match result {
                Ok(Ok(value)) => value,
                Ok(Err(e)) => {
                    error!("Error: {}", e);
                    "".to_string()
                }
                Err(e) => {
                    error!("Error: {}", e);
                    "".to_string()
                }
            })
            .collect();
        debug!(
            "trace_id: {}, submit txids: {:?}",
            arbi_event.trace_id, result
        );
    } else {
        let result = transaction_helpers
            .connection
            .send_transaction_with_config(
                transactions.get(0).unwrap().get(0).unwrap(),
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..RpcSendTransactionConfig::default()
                },
            )
            .await;
        debug!(
            "normal submit with trace_id: {}, submit txids: [{:?}]",
            arbi_event.trace_id,
            result.unwrap()
        );
    }
    debug!(
        "trace_id: {}, total duration: {}",
        arbi_event.trace_id,
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );

    Ok(())
}
