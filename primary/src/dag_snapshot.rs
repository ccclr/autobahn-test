// dag_snapshot.rs
// DAG Snapshot Analyzer for extracting structural and performance features.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use log::{info, debug};

use crate::messages::{Certificate, Header};

#[derive(Debug, Default)]
pub struct DagSnapshot {
    pub tips: HashSet<String>,           // Nodes with no children
    pub depth_map: HashMap<String, u64>, // Depth of each node
    pub parents_map: HashMap<String, Vec<String>>, // Adjacency list
    pub commit_latency: Vec<Duration>,   // Latency stats
    pub created_timestamps: HashMap<String, Instant>,
    pub committed_timestamps: HashMap<String, Instant>,
}

impl DagSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called when a new header is proposed
    pub fn observe_header(&mut self, header: &Header, now: Instant) {
        let node_id = header.id.to_string();
        self.created_timestamps.insert(node_id.clone(), now);

        // Insert into tips, unless it's referenced
        self.tips.insert(node_id.clone());

        // Register parents
        let parent_id = header.parent_cert.header_digest.to_string();
        self.tips.remove(&parent_id); // 父节点不再是 tip

        // Update depth: depth = parent depth + 1
        let depth = self.depth_map.get(&parent_id).copied().unwrap_or(0) + 1;
        self.depth_map.insert(node_id.clone(), depth);
        self.parents_map.insert(node_id, vec![parent_id]);

        debug!(
            "[DAG SNAPSHOT][observe_header] tips: {}, depth_map: {}, created_timestamps: {}",
            self.tips.len(),
            self.depth_map.len(),
            self.created_timestamps.len()
        );
    }

    /// Called when a header is committed
    pub fn observe_commit(&mut self, header: &Header, now: Instant) {
        let id = header.id.to_string();
        self.committed_timestamps.insert(id.clone(), now);

        if let Some(start) = self.created_timestamps.get(&id) {
            let latency = now.duration_since(*start);
            self.commit_latency.push(latency);
        }

        debug!(
            "[DAG SNAPSHOT][observe_commit] tips: {}, depth_map: {}, commit_latency: {}",
            self.tips.len(),
            self.depth_map.len(),
            self.commit_latency.len()
        );
    }

    pub fn width(&self) -> usize {
        self.tips.len()
    }

    pub fn depth(&self) -> u64 {
        *self.depth_map.values().max().unwrap_or(&0)
    }

    pub fn avg_commit_latency_ms(&self) -> f64 {
        if self.commit_latency.is_empty() {
            return 0.0;
        }
        let total_ms: u128 = self.commit_latency.iter().map(|d| d.as_millis()).sum();
        total_ms as f64 / self.commit_latency.len() as f64
    }

    pub fn orphaned_tip_ratio(&self) -> f64 {
        let total = self.depth_map.len();
        if total == 0 {
            return 0.0;
        }
        self.tips.len() as f64 / total as f64
    }

    pub fn growth_rate(&self, window: Duration) -> f64 {
        let count = self.depth_map.len() as f64;
        count / window.as_secs_f64()
    }

    pub fn report(&self, window: Duration) {
        info!("\n==== DAG Snapshot Report ====");
        info!("DAG Width (tips): {}", self.width());
        info!("DAG Depth: {}", self.depth());
        info!("Avg Commit Latency: {:.2} ms", self.avg_commit_latency_ms());
        info!("DAG Growth Rate: {:.2} nodes/s", self.growth_rate(window));
        info!("Orphaned Tip Ratio: {:.3}", self.orphaned_tip_ratio());
    }
}
