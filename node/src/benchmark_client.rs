// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .args_from_usage("--node-id=<INT> 'The index of this client node (0-based)'")
        .args_from_usage("--hotspot-windows=[WINDOW]... 'Hotspot time windows in format start:end (e.g., 10:20)'")
        .args_from_usage("--hotspot-nodes=[COUNT]... 'Number of hotspot nodes for each window'")
        .args_from_usage("--hotspot-rates=[RATE]... 'Rate increase multipliers for each window (e.g., 0.2 for 20% increase)'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    let node_id = matches
        .value_of("node-id")
        .unwrap_or("0")
        .parse::<usize>()
        .context("Node ID must be a non-negative integer")?;

    // Parse hotspot configuration
    let hotspot_config = parse_hotspot_config(&matches)?;

    info!("Node address: {}", target);
    info!("Node ID: {}", node_id);
    info!("Total nodes: {}", nodes.len());

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    if let Some(ref config) = hotspot_config {
        info!("Hotspot configuration enabled:");
        for (i, (( (start_end, &num_nodes), &rate_mult ))) in config.hotspot_windows.iter()
            .zip(&config.hotspot_nodes)
            .zip(&config.hotspot_rates)
            .enumerate()
        {
            let (start, end) = *start_end;
            info!("  Window {}: {}s-{}s, {} hotspot nodes, {:.1}% rate increase", 
                  i + 1, start, end, num_nodes, rate_mult * 100.0);
        }
    }

    let client = Client {
        target,
        size,
        rate,
        nodes,
        node_id,
        hotspot_config,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

fn parse_hotspot_config(matches: &clap::ArgMatches) -> Result<Option<HotspotConfig>> {
    let windows = matches.values_of("hotspot-windows");
    let nodes = matches.values_of("hotspot-nodes");
    let rates = matches.values_of("hotspot-rates");

    match (windows, nodes, rates) {
        (Some(windows), Some(nodes), Some(rates)) => {
            let windows: Result<Vec<(u64, u64)>, _> = windows
                .map(|w| {
                    let parts: Vec<&str> = w.split(':').collect();
                    if parts.len() != 2 {
                        return Err(anyhow::Error::msg("Invalid window format, use start:end"));
                    }
                    let start = parts[0].parse::<u64>()
                        .context("Invalid start time")?;
                    let end = parts[1].parse::<u64>()
                        .context("Invalid end time")?;
                    Ok((start, end))
                })
                .collect();

            let nodes: Result<Vec<usize>, _> = nodes
                .map(|n| n.parse::<usize>().context("Invalid node count"))
                .collect();

            let rates: Result<Vec<f64>, _> = rates
                .map(|r| r.parse::<f64>().context("Invalid rate multiplier"))
                .collect();

            let windows = windows?;
            let nodes = nodes?;
            let rates = rates?;

            if windows.len() != nodes.len() || windows.len() != rates.len() {
                return Err(anyhow::Error::msg(
                    "Hotspot windows, nodes, and rates must have the same length"
                ));
            }

            Ok(Some(HotspotConfig {
                hotspot_windows: windows,
                hotspot_nodes: nodes,
                hotspot_rates: rates,
            }))
        }
        (None, None, None) => Ok(None),
        _ => Err(anyhow::Error::msg(
            "If using hotspot configuration, all three parameters (windows, nodes, rates) must be provided"
        )),
    }
}

#[derive(Debug, Clone)]
pub struct HotspotConfig {
    pub hotspot_windows: Vec<(u64, u64)>, // 时间段 [start, end] in seconds
    pub hotspot_nodes: Vec<usize>,         // 每个时间段的热点节点数量
    pub hotspot_rates: Vec<f64>,           // 每个时间段的速率增长倍数
}

impl HotspotConfig {
    /// 计算指定时间和节点的到达率
    pub fn get_arrival_rate(&self, elapsed_secs: u64, base_rate: f64, node_idx: usize) -> f64 {
        for ((start_end, &num_hotspot), &rate_increase) in self.hotspot_windows.iter()
            .zip(&self.hotspot_nodes)
            .zip(&self.hotspot_rates) {
            let (start, end) = *start_end;
            if elapsed_secs >= start && elapsed_secs <= end {
                if node_idx < num_hotspot {
                    return base_rate * (1.0 + rate_increase);
                }
            }
        }
        base_rate
    }
}

struct Client {
    target: SocketAddr,                    // 指定要连接的 worker
    size: usize,                          // 指定事务的字节大小
    rate: u64,                            // 基础发送速率
    nodes: Vec<SocketAddr>,               // 所有节点的地址
    node_id: usize,                       // 当前客户端节点的索引
    hotspot_config: Option<HotspotConfig>, // 热点配置
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 9 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen::<u64>();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        
        let start_time = Instant::now();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);
        
        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();
            let elapsed_secs = start_time.elapsed().as_secs();
            
            // 计算当前时间的发送速率
            let current_rate = if let Some(ref config) = self.hotspot_config {
                config.get_arrival_rate(elapsed_secs, self.rate as f64, self.node_id)
            } else {
                self.rate as f64
            };
            
            // 计算当前突发周期内应该发送的事务数量
            let burst = (current_rate / PRECISION as f64).round() as u64;
            
            // 每秒记录一次当前速率（用于性能分析）
            if counter % (current_rate as u64).max(1) == 0 {
                info!("Current transaction rate: {:.2} tx/s at time {}s", current_rate, elapsed_secs);
            }

            // 在当前突发周期内发送事务
            for x in 0..burst {
                if counter % 100 == 0 {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);
                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                }

                tx.resize(self.size, 0u8); // Truncate any bits past size
                let bytes = tx.split().freeze(); // split() moves byte content from tx to bytes

                // 发送事务
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            
            // 检查是否发送时间过长
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}