use base64::Engine;
use log::{debug, error, info, trace};
use lz4_flex::decompress_size_prepended;
use rand::{rngs::OsRng, seq::SliceRandom};
use reqwest::Client as ReqwestClient;

use std::{str::FromStr, sync::Arc, time::SystemTime};

use anchor_client::{
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        address_lookup_table::AddressLookupTableAccount, commitment_config::CommitmentConfig,
        pubkey::Pubkey, signature::Keypair,
    },
    Client, Cluster,
};
use anyhow::{Context, Result};
use async_compression::tokio::bufread::ZlibDecoder;
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    AsyncCommands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo,
};
use tokio::io::AsyncReadExt as _;
use tokio::sync::Semaphore;

use crate::submiter::assembler::assemble_and_submit_transaction;

use super::assembler::{ArbiEvent, TransactionHelpers};

const RPC_URLS: [&str; 0] = [];
// const RPC_URLS: [&str; 1] = ["http://127.0.0.1:8899"];

const SUBMITTER_KEYS: [&str; 0] = [];

const RPC_SUBMITTER_KEYS: [&str; 1] = [""];

const PROXY_SUBMITTER_KEYS: [&str; 1] = [""];

pub async fn monitor_and_submit() -> Result<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let mut redis_conn = get_or_init_redis(redis_url, 0).await?;
    let redis_key_name =
        std::env::var("REDIS_QUEUE_NAME").unwrap_or_else(|_| "arbi_swap_queue".to_string());

    let alt_account = Arc::new(AddressLookupTableAccount {
        key: Pubkey::from_str("5JeXxBnqMU4kVPciskf4DBtdQEXPL6qowC8mSiyo4F49").unwrap(),
        addresses: vec![
            Pubkey::from_str("11111111111111111111111111111111").unwrap(),
            Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap(),
            Pubkey::from_str("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(),
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
            Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap(),
            Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap(),
            Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap(),
        ],
    });

    let connections = RPC_URLS.map(|rpc_url| {
        Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ))
    });

    // 控制子线程并发
    // 从环境变量获取并行度，默认值为 1
    let parallelism = std::env::var("PARALLELISM")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .unwrap_or(1); // 解析为 usize，解析失败时使用默认值

    let semaphore = Arc::new(Semaphore::new(parallelism));

    // 初始化ReqwestClient
    let request_client = Arc::new(ReqwestClient::new());

    loop {
        // 获取一个信号量许可，如果没有可用许可，则等待
        let _permit = semaphore.clone().acquire_owned().await.unwrap();
        let connection = connections.choose(&mut OsRng).unwrap();

        let start_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // 使用 BLPOP 阻塞等待队列中的消息
        match redis_conn.blpop(&redis_key_name, 300.0).await {
            Ok(Some((key, value))) => {
                trace!(
                    "tokio available_permits count: {}",
                    semaphore.available_permits()
                );
                // 启动子线程
                let alt_account_clone = alt_account.clone();
                let request_client_clone = request_client.clone();
                let connection_clone = connection.clone();

                let _handle = tokio::spawn(async move {
                    match execute_transaction(
                        key,
                        value,
                        alt_account_clone,
                        request_client_clone,
                        connection_clone,
                        start_ts,
                    )
                    .await
                    {
                        Ok(_) => debug!("Transaction executed successfully."),
                        Err(e) => error!("Error executing transaction: {:?}", e),
                    }
                });
            }
            Ok(None) => {
                debug!("no message, continue");
                continue;
            } // 超时无消息，继续轮询
            Err(err) => {
                error!("Redis BLPOP failed: {:?}, 100ms later retry...", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        //break;
    }
}

async fn execute_transaction(
    key: String,
    value: String,
    alt_account: Arc<AddressLookupTableAccount>,
    request_client: Arc<ReqwestClient>,
    connection: Arc<RpcClient>,
    start_ts: i64,
) -> Result<()> {
    let config_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // 解码并处理消息
    let compressed_message = base64::engine::general_purpose::STANDARD.decode(value)?;
    let arbi_data_str = lz4_inflate_message(compressed_message)?;

    let arbi_event: ArbiEvent = serde_json::from_str(&arbi_data_str)?;
    let stream_ts = arbi_event.stream_ts;
    let trace_id = arbi_event.trace_id.clone();
    // 超时退出
    if config_ts - stream_ts > 1000 {
        return Err(anyhow::anyhow!(format!(
            "{} stream_ts timeout {}",
            trace_id,
            config_ts - stream_ts
        )));
    }
    // 处理解码后的消息
    debug!("Parse message from {}: {:?}", key, arbi_event.clone());
    trace!("Received message from {}: {:?}", key, arbi_data_str.clone());

    let private_key = if std::env::var("REDIS_QUEUE_NAME")
        .unwrap_or_else(|_| "arbi_swap_queue".to_string())
        == "arbi_swap_queue_rpc"
    {
        RPC_SUBMITTER_KEYS[0]
    } else if arbi_event.transaction.use_proxy_account || arbi_event.transaction.use_kamino {
        PROXY_SUBMITTER_KEYS[0]
    } else {
        SUBMITTER_KEYS.choose(&mut OsRng).unwrap()
    };
    let wallet = &Keypair::from_base58_string(private_key);

    let client: Client<&Keypair> = Client::new(Cluster::Localnet, &wallet);
    let program_id = Pubkey::from_str("")?;
    let program = Arc::new(client.program(program_id)?);

    let transaction_helpers = TransactionHelpers {
        program,
        alt_account, // Ensure alt_account is defined in the scope
        connection,
        wallet,
    };

    let submit_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    assemble_and_submit_transaction(arbi_event, transaction_helpers, request_client).await?;

    let end_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    info!(
        "trace_id: {}, Submit Cost: {}",
        trace_id,
        serde_json::json!({
            "redis": config_ts - start_ts,
            "parse": submit_ts - config_ts,
            "submit": end_ts - submit_ts,
            "stream": config_ts - stream_ts,
        })
    );

    Ok(())
}

async fn inflate_message(compressed_data: Vec<u8>) -> Result<String> {
    // 使用 ZlibDecoder 创建解压缩器
    let mut decoder = ZlibDecoder::new(&compressed_data[..]);
    let mut decompressed_data = String::new();

    // 读取解压缩的数据
    decoder.read_to_string(&mut decompressed_data).await?;

    Ok(decompressed_data)
}

fn lz4_inflate_message(compressed_data: Vec<u8>) -> Result<String> {
    let decompressed_bytes = decompress_size_prepended(&compressed_data).context("LZ4 解压失败")?;
    let result = String::from_utf8(decompressed_bytes).context("UTF-8 解码失败")?;
    Ok(result)
}

pub async fn get_or_init_redis(redis_url: String, redis_db: i64) -> Result<ConnectionManager> {
    let ip: String;
    let port: u16;
    let password = Some("x".to_string());

    // 按 ":" 切割字符串
    let parts: Vec<&str> = redis_url.split(":").collect();

    // 确保切割结果有两个部分
    if parts.len() == 2 {
        ip = parts[0].to_string();
        let port_str = parts[1];

        // 尝试将端口转换为 u64
        match port_str.parse::<u16>() {
            Ok(port_u16) => port = port_u16,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "端口转换失败，输入的端口可能不是有效的数字"
                ))
            }
        }
    } else {
        return Err(anyhow::anyhow!(
            "端口转换失败，输入的端口可能不是有效的数字"
        ));
    }

    let conn = redis::Client::open(ConnectionInfo {
        addr: ConnectionAddr::Tcp(ip.clone(), port),
        redis: RedisConnectionInfo {
            db: redis_db,
            password,
            ..Default::default()
        },
    })
    .unwrap()
    .get_connection_manager_with_config(
        ConnectionManagerConfig::new().set_connection_timeout(std::time::Duration::new(10, 0)),
    )
    .await
    .unwrap();

    info!("Success connected to redis:{}, db:{}", redis_url, redis_db);

    Ok(conn)
}
