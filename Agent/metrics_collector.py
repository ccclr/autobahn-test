#!/usr/bin/env python3

import os
import re
import json
import time
import psutil
import numpy as np
import subprocess
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict, field
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HardwareCapacity:
    """Layer 1: Hardware capacity metrics"""
    cpu_cores: int
    memory_gb: float
    network_bandwidth_mbps: float
    workers_per_node: int

@dataclass
class NetworkBaseline:
    """Layer 1: Network baseline metrics"""
    intra_region_latency_ms: float
    cross_region_latency_ms: float
    latency_variance: float
    # node_geographic_distribution: List[str]
    network_asymmetry: float

@dataclass
class DAGGrowthPattern:
    """Layer 2 Part I: DAG growth pattern"""
    avg_lane_growth_rate: float
    lane_growth_variance: float
    dag_width: float
    dag_depth: float

@dataclass
class ConsensusMetrics:
    """Layer 2 Part II: Consensus metrics"""
    fast_path_ratio: float
    slow_path_ratio: float
    # proposal_commit_success_rate: float
    # consensus_latency_ms: float
    # consensus_throughput_tps: float
    # timeout_events: int
    retry_events: int
    # commit_latency_by_slot_ms: Dict[int, float]
    commit_latency_summary_ms: Dict[str, float]

@dataclass
class Workload:
    """Layer 3: Workload characteristics (self-sensed)"""
    tx_size_bytes: int
    tx_arrival_rate_tps: float

    @property
    def ingress_bps(self) -> float:
        return float(self.tx_size_bytes) * float(self.tx_arrival_rate_tps) * 8.0

@dataclass
class SystemState:
    """Complete system state"""
    timestamp: float
    hardware: HardwareCapacity
    network: NetworkBaseline
    dag_growth: DAGGrowthPattern
    consensus: ConsensusMetrics
    workload: Workload
    node_params: Dict[str, Any]  # current node parameters

class MetricsCollector:
    """Main class for metrics/parameter collection"""
    
    def __init__(self, log_dir: str = "/home/ccclr0302/autobahn-test/benchmark/logs"):
        self.log_dir = Path(log_dir)
        self.committee_file = self.log_dir.parent / ".committee.json"
        self.parameters_file = self.log_dir.parent / ".parameters.json"
        
    def _measure_iperf3_bandwidth(self, server_ip: str, duration: int = 8, parallel: int = 4) -> float:
        """Run iperf3 client against server_ip and return Mbps (float). Returns 0.0 on failure."""
        try:
            cmd = [
                "iperf3", "-c", server_ip,
                "-t", str(duration),
                "-P", str(parallel),
                "-J",
            ]
            out = subprocess.check_output(cmd, text=True, timeout=duration + 15)
            data = json.loads(out)
            # Prefer sum_received; fallback to sum_sent
            bps = None
            try:
                bps = data["end"]["sum_received"]["bits_per_second"]
            except Exception:
                try:
                    bps = data["end"]["sum_sent"]["bits_per_second"]
                except Exception:
                    bps = None
            if bps is None:
                return 0.0
            return float(bps) / 1e6
        except Exception as e:
            logger.debug(f"iperf3 measurement failed for {server_ip}: {e}")
            return 0.0

    def _measure_bandwidth_via_iperf3(self, peer_ips: List[str], duration: int = 8, parallel: int = 4) -> float:
        """Measure bandwidth to multiple peers and return a robust aggregate (median) in Mbps."""
        results = []
        for ip in peer_ips:
            mbps = self._measure_iperf3_bandwidth(ip.strip(), duration=duration, parallel=parallel)
            if mbps > 0:
                results.append(mbps)
        if not results:
            return 0.0
        return float(np.median(results))

    def collect_hardware_capacity(self) -> HardwareCapacity:
        """Collect hardware capacity metrics"""
        try:
            # CPU core count
            cpu_cores = psutil.cpu_count(logical=False) or psutil.cpu_count()
            
            # Memory capacity
            memory = psutil.virtual_memory()
            memory_gb = memory.total / (1024**3)
            
            # Network bandwidth (active measurement via iperf3 if configured)
            network_bandwidth_mbps = 0.0
            peer_str = os.getenv("IPERF3_PEERS", "").strip()
            if peer_str:
                peers = [p for p in peer_str.split(",") if p.strip()]
                if peers:
                    network_bandwidth_mbps = self._measure_bandwidth_via_iperf3(peers)
            
            # Fallback: NIC link speed or conservative default
            # if network_bandwidth_mbps <= 0.0:
            #     # Try NIC reported speed
            #     nic_speed = 0.0
            #     try:
            #         # pick primary interface (heuristic)
            #         primary_nic = None
            #         for nic in os.listdir("/sys/class/net"):
            #             if nic.startswith("lo"):
            #                 continue
            #             primary_nic = nic
            #             break
            #         if primary_nic:
            #             with open(f"/sys/class/net/{primary_nic}/speed") as f:
            #                 nic_speed = float(f.read().strip())
            #     except Exception:
            #         nic_speed = 0.0
            #     if nic_speed > 0:
            #         # use 60% of link speed as a conservative estimate
            #         network_bandwidth_mbps = nic_speed * 0.6
            #     else:
            #         # final fallback default
            #         network_bandwidth_mbps = 10000.0
            
            # Workers per node (from parameter file)
            workers_per_node = 1
            if self.parameters_file.exists():
                with open(self.parameters_file, 'r') as f:
                    params = json.load(f)
                    workers_per_node = params.get('workers', 1)
            
            return HardwareCapacity(
                cpu_cores=cpu_cores,
                memory_gb=memory_gb,
                network_bandwidth_mbps=network_bandwidth_mbps,
                workers_per_node=workers_per_node
            )
        except Exception as e:
            logger.error(f"Failed to collect hardware metrics: {e}")
            return HardwareCapacity(0, 0, 0, 0, 0, 1)
    
    def collect_network_baseline(self) -> NetworkBaseline:
        """Collect network baseline metrics - integrated with fabfile version"""
        try:
            # Method 1: directly call the latency function in fabfile
            latency_matrix = self._call_fabfile_latency()
            
            if latency_matrix is not None:
                # Compute network metrics
                n = len(latency_matrix)
                intra_latencies = [latency_matrix[i][i] for i in range(n) if not np.isnan(latency_matrix[i][i])]
                cross_latencies = [latency_matrix[i][j] for i in range(n) for j in range(n) 
                                if i != j and not np.isnan(latency_matrix[i][j])]
                
                print(cross_latencies)
                intra_region_latency = np.mean(intra_latencies) if intra_latencies else 1.0
                cross_region_latency = np.mean(cross_latencies) if cross_latencies else 50.0
                latency_variance = np.var(cross_latencies) if cross_latencies else 5.0
                
                # Compute network asymmetry
                asymmetry_values = []
                for i in range(n):
                    for j in range(n):
                        if i != j and not np.isnan(latency_matrix[i][j]) and not np.isnan(latency_matrix[j][i]):
                            asymmetry = abs(latency_matrix[i][j] - latency_matrix[j][i]) / (latency_matrix[i][j] + latency_matrix[j][i])
                            asymmetry_values.append(asymmetry)
                network_asymmetry = np.mean(asymmetry_values) if asymmetry_values else 0.1
                
                return NetworkBaseline(
                    intra_region_latency_ms=float(intra_region_latency),
                    cross_region_latency_ms=float(cross_region_latency),
                    latency_variance=float(latency_variance),
                    # node_geographic_distribution=[f"region_{i}" for i in range(n)],
                    network_asymmetry=float(network_asymmetry)
                )
            else:
                logger.error(f"Failed to collect network metrics")
                return NetworkBaseline(1.0, 50.0, 5.0, 0.1)
        except Exception as e:
            logger.error(f"Failed to collect network metrics: {e}")
            return NetworkBaseline(1.0, 50.0, 5.0, 0.1)
            
    def _call_fabfile_latency(self) -> Optional[np.ndarray]:
        """Directly call the latency function from fabfile"""
        try:
            import subprocess
            import os
            
            # Run in benchmark directory (where fabfile.py resides)
            benchmark_dir = self.log_dir.parent  # /.../autobahn-test/benchmark
            original_cwd = os.getcwd()
            
            try:
                os.chdir(benchmark_dir)
                
                # Invoke fab latency command (boolean flag style)
                result = subprocess.run(
                    ['fab', 'latency', '--intra-region'],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minutes timeout
                )
                
                if result.returncode != 0:
                    logger.error(f"fab latency failed: stdout=\n{result.stdout}\nstderr=\n{result.stderr}")
                    return None
                
                # Prefer region matrix, fallback to full matrix
                region_files = sorted(
                    [f for f in os.listdir('.') if f.startswith('latency_region_matrix_') and f.endswith('.npy')],
                    key=os.path.getctime
                )
                full_files = sorted(
                    [f for f in os.listdir('.') if f.startswith('latency_full_matrix_') and f.endswith('.npy')],
                    key=os.path.getctime
                )
                pick_path: Optional[str] = None
                if region_files:
                    pick_path = region_files[-1]
                elif full_files:
                    pick_path = full_files[-1]
                
                if pick_path is None:
                    logger.error("No latency matrix files were produced by fab latency")
                    return None
                
                return np.load(pick_path)
                
            finally:
                os.chdir(original_cwd)
                
        except Exception as e:
            logger.error(f"Failed to run fab latency command: {e}")
            return None

    def collect_workload(self, window_sec: int = 60) -> Workload:
        """Self-sense workload.
        Priority 1: parse latest client-*-*.log for configured tx size and tx rate (TPS).
        Priority 2: fallback to primary logs within recent time window to estimate TPS and size.
        """
        # Try client logs first
        try:
            logs_dir = Path("/home/ccclr0302/autobahn-test/benchmark/logs/")
            client_logs = sorted(logs_dir.glob("client-*-*.log"), key=lambda p: p.stat().st_mtime)
            if client_logs:
                latest_client = client_logs[-1]
                tx_size_bytes: Optional[int] = None
                tx_rate_tps: Optional[float] = None
                with open(latest_client, 'r') as f:
                    for line in f:
                        m_size = re.search(r"Transactions size:\s*(\d+)\s*B", line)
                        if m_size:
                            try:
                                tx_size_bytes = int(m_size.group(1))
                            except Exception:
                                pass
                        m_rate = re.search(r"Transactions rate:\s*([0-9]+(?:\.[0-9]+)?)\s*tx/s", line)
                        if m_rate:
                            try:
                                tx_rate_tps = float(m_rate.group(1))
                            except Exception:
                                pass
                if tx_size_bytes is not None and tx_rate_tps is not None:
                    return Workload(tx_size_bytes=max(1, tx_size_bytes), tx_arrival_rate_tps=max(0.0, tx_rate_tps))
        except Exception as e:
            logger.debug(f"Parsing client logs for workload failed: {e}")
    
    def collect_dag_growth_pattern(self) -> DAGGrowthPattern:
        """Collect DAG growth pattern metrics - group by validator pk and keep per-lane growth rate"""
        try:
            print("=== Start collecting DAG growth pattern metrics ===")
            
            # Extract DAG info from primary logs
            log_dir = Path("/home/ccclr0302/autobahn-test/benchmark/logs/")
            primary_logs = list(log_dir.glob("primary-0.log"))
            print(f"Found {len(primary_logs)} primary log files: {[f.name for f in primary_logs]}")
            
            if not primary_logs:
                print("No primary log found, return default values")
                return DAGGrowthPattern(0, 0, 0, 0)
            
            # Collect all prepare events and group by validator pk
            prepare_events = []  # [(slot, validator_pk, proposal_height, timestamp)]
            total_lines_processed = 0
            prepare_lines_found = 0
        
            for log_file in primary_logs:
                print(f"\nProcessing log file: {log_file.name}")
                with open(log_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        total_lines_processed += 1
                        
                        # Extract prepare events
                        slot_match = re.search(r'prepare slot (\d+)', line)
                        if slot_match:
                            prepare_lines_found += 1
                            slot = int(slot_match.group(1))
                            
                            # Extract validator pk
                            validator_match = re.search(r'validator: ([A-Za-z0-9+/=]+)', line)
                            if validator_match:
                                validator_pk = validator_match.group(1)
                                
                                # Extract timestamp
                                timestamp_match = re.search(r'\[(.*?Z)', line)
                                if timestamp_match:
                                    timestamp_str = timestamp_match.group(1)
                                    try:
                                        from datetime import datetime
                                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                        timestamp = dt.timestamp()
                                        
                                        # Extract proposal height
                                        height_match = re.search(r'proposal height (\d+)', line)
                                        if height_match:
                                            proposal_height = int(height_match.group(1))
                                            prepare_events.append((slot, validator_pk, proposal_height, timestamp))
                                            
                                            # Print sample events
                                            # if len(prepare_events) <= 5:
                                            #     print(f"  Event {len(prepare_events)}: slot={slot}, validator={validator_pk[:16]}..., height={proposal_height}, time={timestamp_str}")
                                        
                                    except Exception as e:
                                        logger.debug(f"Failed to parse timestamp: {e}")
                                        continue
                        # progress logging omitted
            
            print(f"\nLog processing finished:")
            print(f"  Total processed lines: {total_lines_processed}")
            print(f"  Prepare event lines found: {prepare_lines_found}")
            print(f"  Successfully parsed events: {len(prepare_events)}")
            
            if not prepare_events:
                print("No valid prepare events found, return default values")
                return DAGGrowthPattern(0, 0, 0, 0)
            
            # Sort by time
            prepare_events.sort(key=lambda x: x[3])
            print(f"\nEvents sorted by time, time range: {prepare_events[0][3]} to {prepare_events[-1][3]}")
            
            # Group by validator pk and compute per-validator lane growth
            validator_lanes = {}  # {validator_pk: [(slot, height, timestamp), ...]}
            
            for slot, validator_pk, proposal_height, timestamp in prepare_events:
                if validator_pk not in validator_lanes:
                    validator_lanes[validator_pk] = []
                validator_lanes[validator_pk].append((slot, proposal_height, timestamp))
            
            print(f"\nAggregation by validator completed:")
            print(f"  Found {len(validator_lanes)} distinct validators")
            # for i, (validator_pk, events) in enumerate(validator_lanes.items()):
                # if i < 3:
                #     print(f"  Validator {i+1}: {validator_pk[:16]}... has {len(events)} events")
                # elif i == 3:
                #     print(f"  ... and {len(validator_lanes) - 3} more validators")
                #     break
            
            # Compute per-validator lane growth rate based on proposal height change
            validator_growth_rates = {}  # {validator_pk: [growth_rate1, growth_rate2, ...]}
            time_window = 10.0  # 10-second time window
            print(f"\nStart computing per-validator lane growth rate (time window: {time_window} seconds):")
            
            for validator_pk, lane_events in validator_lanes.items():
                if not lane_events:
                    continue
                
                # Sort events by time
                lane_events.sort(key=lambda x: x[2])
                
                # print(f"\nAnalyzing validator {validator_pk[:16]}...:")
                # print(f"  Total events: {len(lane_events)}")
                # print(f"  Height range: {min(event[1] for event in lane_events)} to {max(event[1] for event in lane_events)}")
                
                # Compute growth rate within the time window
                current_time = lane_events[0][2]
                window_events = []
                window_count = 0
                validator_growth_rates[validator_pk] = []
                
                for slot, height, timestamp in lane_events:
                    if timestamp - current_time <= time_window:
                        window_events.append((slot, height, timestamp))
                    else:
                        # Compute current window's growth rate (based on height change)
                        if len(window_events) > 1:
                            # Sort events within the window by time
                            window_events.sort(key=lambda x: x[2])
                            start_height = window_events[0][1]
                            end_height = window_events[-1][1]
                            start_time = window_events[0][2]
                            end_time = window_events[-1][2]
                            
                            # Compute height growth and time span
                            height_growth = end_height - start_height
                            time_span = end_time - start_time
                            
                            if time_span > 0:
                                growth_rate = height_growth / time_span  # height per second
                                validator_growth_rates[validator_pk].append(growth_rate)
                                window_count += 1
                                
                                # Print first few windows' details
                                # if window_count <= 3:
                                #     print(f"    Window {window_count}: height {start_height} -> {end_height} ({height_growth}), time {time_span:.2f}s, growth rate {growth_rate:.2f} height/s")
                        
                        # Start a new time window
                        current_time = timestamp
                        window_events = [(slot, height, timestamp)]
                
                # Process the last window
                if len(window_events) > 1:
                    window_events.sort(key=lambda x: x[2])
                    start_height = window_events[0][1]
                    end_height = window_events[-1][1]
                    start_time = window_events[0][2]
                    end_time = window_events[-1][2]
                    
                    height_growth = end_height - start_height
                    time_span = end_time - start_time
                    
                    if time_span > 0:
                        growth_rate = height_growth / time_span
                        validator_growth_rates[validator_pk].append(growth_rate)
                        window_count += 1
                        if window_count <= 3:
                            print(f"    Window {window_count}: height {start_height} -> {end_height} ({height_growth}), time {time_span:.2f}s, growth rate {growth_rate:.2f} height/s")
            
            # Print per-validator growth rates
            print(f"\nPer-validator growth rates:")
            for validator_pk, growth_rates in validator_growth_rates.items():
                if growth_rates:
                    avg_rate = np.mean(growth_rates)
                    std_rate = np.std(growth_rates)
                    print(f"  Validator {validator_pk[:16]}... avg growth: {avg_rate:.2f} height/s, std: {std_rate:.2f}, windows: {len(growth_rates)}")
            
            # Compute overall metrics (based on all validators' growth rates)
            all_growth_rates = []
            for growth_rates in validator_growth_rates.values():
                all_growth_rates.extend(growth_rates)
            
            # print(f"\nOverall growth rate statistics:")
            # print(f"  Total windows: {len(all_growth_rates)}")
            # print(f"  Growth rates: {all_growth_rates[:10]}{'...' if len(all_growth_rates) > 10 else ''}")
            
            # Compute indicators
            avg_lane_growth_rate = np.mean(all_growth_rates) if all_growth_rates else 0
            lane_growth_variance = np.var(all_growth_rates) if all_growth_rates else 0
            
            print(f"\nFinal indicators:")
            print(f"  Average lane growth rate: {avg_lane_growth_rate:.2f} height/s")
            print(f"  Lane growth variance: {lane_growth_variance:.2f}")
            
            # Compute DAG width and depth
            dag_width = len(validator_lanes) if len(validator_lanes) > 0 else 1

            # DAG depth: take the longest by max proposal height per lane
            lane_max_heights = []
            for events in validator_lanes.values():
                if events:
                    lane_max_heights.append(max(h for _, h, _ in events))
            dag_depth = max(lane_max_heights) if lane_max_heights else 1

            print(f"  DAG width: {dag_width}")
            print(f"  DAG depth: {dag_depth}")
            
            result = DAGGrowthPattern(
                avg_lane_growth_rate=float(avg_lane_growth_rate),
                lane_growth_variance=float(lane_growth_variance),
                dag_width=float(dag_width),
                dag_depth=float(dag_depth)
            )
            
            print(f"\n=== DAG growth pattern metrics collection completed ===")
            print(f"Final result: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to collect DAG growth metrics: {e}")
            print(f"Error: {e}")
            return DAGGrowthPattern(0, 0, 0, 0)
    
    def collect_consensus_metrics(self) -> ConsensusMetrics:
        """Collect consensus metrics"""
        try:
            # Extract consensus info from primary logs
            log_dir = Path("/home/ccclr0302/autobahn-test/benchmark/logs/")
            primary_logs = list(log_dir.glob("primary-*.log"))
            if not primary_logs:
                return ConsensusMetrics(0, 0, 0, 0)
            
            fast_path_count = 0
            slow_path_count = 0
            commit_success_count = 0
            commit_total = 0
            timeout_events = 0
            retry_events = 0
            consensus_latencies = []
            
            # Record first prepare and first commit timestamps by slot
            first_prepare_ts: Dict[int, float] = {}
            first_commit_ts: Dict[int, float] = {}
            
            for log_file in primary_logs:
                with open(log_file, 'r') as f:
                    for line in f:
                        # Count fast/slow path
                        if "taking fast path" in line.lower():
                            fast_path_count += 1
                        elif "taking slow path" in line.lower():
                            slow_path_count += 1
                        
                        # Count commit success
                        if "committed" in line.lower() and "B" in line:
                            commit_total += 1
                            if "success" in line.lower():
                                commit_success_count += 1
                        
                        # Count timeouts and retries
                        if "timeout" in line.lower():
                            timeout_events += 1
                        if "retry" in line.lower():
                            retry_events += 1
                        
                        # Extract latency
                        latency_match = re.search(r"latency.*?(\d+\.?\d*)\s*ms", line)
                        if latency_match:
                            consensus_latencies.append(float(latency_match.group(1)))
                    
                        timestamp = None
                        timestamp_match = re.search(r'\[(.*?Z)', line)
                        if timestamp_match:
                            ts = timestamp_match.group(1)  # time string in brackets
                            try:
                                from datetime import datetime, timezone
                                if ts.endswith('Z'):
                                    ts = ts[:-1] + '+00:00'  # normalize Z to +00:00
                                dt = datetime.fromisoformat(ts)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                timestamp = dt.timestamp()
                            except Exception as e:
                                logger.debug(f"Failed to parse timestamp: {e}")

                        # Only store when timestamp is available
                        prep_m = re.search(r"prepare slot\s+(\d+)", line)
                        if prep_m and timestamp is not None:
                            s = int(prep_m.group(1))
                            first_prepare_ts.setdefault(s, timestamp)

                        commit_m = re.search(r"processing commit in slot\s+(\d+)", line)
                        if commit_m and timestamp is not None:
                            commit_slot = int(commit_m.group(1))
                            first_commit_ts.setdefault(commit_slot, timestamp)
            
            # Compute per-slot commit latency (ms) as inter-commit interval: time between previous and current commit
            commit_latency_by_slot_ms: Dict[int, float] = {}
            # Build sorted list of (slot, first_commit_ts)
            commit_items = sorted(first_commit_ts.items(), key=lambda kv: kv[1])
            for i in range(1, len(commit_items)):
                slot_curr, t_commit_curr = commit_items[i]
                _, t_commit_prev = commit_items[i - 1]
                inter_ms = float(max(0.0, (t_commit_curr - t_commit_prev) * 1000.0))
                commit_latency_by_slot_ms[slot_curr] = inter_ms

            # Build distribution summary over inter-commit intervals
            lat_vals = list(commit_latency_by_slot_ms.values())
            summary: Dict[str, float] = {}
            if lat_vals:
                summary = {
                    "count": float(len(lat_vals)),
                    "mean": float(np.mean(lat_vals)),
                    "p50": float(np.percentile(lat_vals, 50)),
                    "p90": float(np.percentile(lat_vals, 90)),
                    "p99": float(np.percentile(lat_vals, 99)),
                    "min": float(np.min(lat_vals)),
                    "max": float(np.max(lat_vals)),
                }
                
            total_paths = fast_path_count + slow_path_count
            fast_path_ratio = fast_path_count / total_paths if total_paths > 0 else 0
            slow_path_ratio = slow_path_count / total_paths if total_paths > 0 else 0
            # commit_success_rate = commit_success_count / commit_total if commit_total > 0 else 0
            # consensus_latency = np.mean(consensus_latencies) if consensus_latencies else 0
            
            # # Estimate throughput (simplified)
            # consensus_throughput = commit_total / 60.0 if commit_total > 0 else 0  # assume within one minute
            
            return ConsensusMetrics(
                fast_path_ratio=float(fast_path_ratio),
                slow_path_ratio=float(slow_path_ratio),
                # proposal_commit_success_rate=float(commit_success_rate),
                # consensus_latency_ms=float(consensus_latency),
                # consensus_throughput_tps=float(consensus_throughput),
                # timeout_events=timeout_events,
                retry_events=retry_events,
                # commit_latency_by_slot_ms=commit_latency_by_slot_ms,
                commit_latency_summary_ms=summary,
            )
        except Exception as e:
            logger.error(f"Failed to collect consensus metrics: {e}")
            return ConsensusMetrics(0, 0, 0, 0)
    
    def load_current_node_params(self) -> Dict[str, Any]:
        """Load current node parameters"""
        try:
            if self.parameters_file.exists():
                with open(self.parameters_file, 'r') as f:
                    return json.load(f)
            else:
                # Return default parameters
                return {
                    'timeout_delay': 1500,
                    'header_size': 32,
                    'max_header_delay': 4000,
                    'gc_depth': 5,
                    'sync_retry_delay': 1000,
                    'sync_retry_nodes': 4,
                    'batch_size': 500000,
                    'max_batch_delay': 4000,
                    'use_optimistic_tips': False,
                    'use_parallel_proposals': True,
                    'k': 1,
                    'use_fast_path': False,
                    'fast_path_timeout': 200,
                    'use_ride_share': False,
                    'car_timeout': 2000,
                    'cut_condition_type': 3
                }
        except Exception as e:
            logger.error(f"Failed to load node parameters: {e}")
            return {}
    
    def collect_system_state(self) -> SystemState:
        """Collect complete system state"""
        logger.info("Start collecting system state...")
        
        hardware = self.collect_hardware_capacity()
        network = self.collect_network_baseline()
        dag_growth = self.collect_dag_growth_pattern()
        consensus = self.collect_consensus_metrics()
        node_params = self.load_current_node_params()
        workload = self.collect_workload()
        
        return SystemState(
            timestamp=time.time(),
            hardware=hardware,
            network=network,
            dag_growth=dag_growth,
            consensus=consensus,
            workload=workload,
            node_params=node_params
        )
    
    def save_state(self, state: SystemState, filename: Optional[str] = None) -> str:
        """Save system state to file"""
        if filename is None:
            timestamp = int(time.time())
            filename = f"system_state_{timestamp}.json"
        
        filepath = self.log_dir / filename
        
        # Convert to serializable dict
        state_dict = {
            'timestamp': state.timestamp,
            'hardware': asdict(state.hardware),
            'network': asdict(state.network),
            'dag_growth': asdict(state.dag_growth),
            'consensus': asdict(state.consensus),
            'workload': asdict(state.workload),
            'node_params': state.node_params
        }
        
        with open(filepath, 'w') as f:
            json.dump(state_dict, f, indent=2)
        
        logger.info(f"System state saved to: {filepath}")
        return str(filepath)
    
    def get_state_vector(self, state: SystemState) -> np.ndarray:
        """Convert system state to RL state vector"""
        # Layer 1: hardware and network metrics
        hardware_vec = [
            state.hardware.cpu_cores,
            state.hardware.memory_gb,
            state.hardware.network_bandwidth_mbps,
            state.hardware.workers_per_node
        ]
        
        network_vec = [
            state.network.intra_region_latency_ms,
            state.network.cross_region_latency_ms,
            state.network.latency_variance,
            state.network.network_asymmetry
        ]
        
        # Layer 2: DAG growth pattern
        dag_vec = [
            state.dag_growth.avg_lane_growth_rate,
            state.dag_growth.lane_growth_variance,
            state.dag_growth.dag_width,
            state.dag_growth.dag_depth
        ]
        
        # Layer 2: consensus metrics
        consensus_vec = [
            state.consensus.fast_path_ratio,
            state.consensus.slow_path_ratio,
            # state.consensus.proposal_commit_success_rate,
            # state.consensus.consensus_latency_ms,
            # state.consensus.consensus_throughput_tps,
            # state.consensus.timeout_events,
            state.consensus.retry_events,
            # state.consensus.commit_latency_by_slot_ms,
            # state.consensus.commit_latency_summary_ms,
        ]

        # Layer 3: workload metrics
        workload_vec = [
            float(state.workload.tx_size_bytes),
            float(state.workload.tx_arrival_rate_tps),
            float(state.workload.ingress_bps),
        ]
        
        # Concatenate all vectors
        state_vector = np.concatenate([hardware_vec, network_vec, dag_vec, consensus_vec, workload_vec])
        
        # Normalization (optional)
        state_vector = np.nan_to_num(state_vector, nan=0.0, posinf=1.0, neginf=0.0)
        
        return state_vector

def main():
    """Main function for testing the metrics collector"""
    collector = MetricsCollector()
    
    # Collect system state
    state = collector.collect_system_state()
    
    # Print state info
    print("=== System state collection result ===")
    print(f"Timestamp: {state.timestamp}\n")
    print(f"Hardware capacity: {asdict(state.hardware)}\n")
    print(f"Network baseline: {asdict(state.network)}\n")
    print(f"DAG growth pattern: {asdict(state.dag_growth)}\n")
    print(f"Consensus metrics: {asdict(state.consensus)}\n")
    print(f"Workload: {asdict(state.workload)}\n")
    print(f"Node params: {state.node_params}\n")
    
    # Save state
    filename = collector.save_state(state)
    print(f"State saved to: {filename}\n")
    
    # Get state vector
    state_vector = collector.get_state_vector(state)
    print(f"State vector dimension: {len(state_vector)}\n")
    print(f"State vector: {state_vector}\n")

if __name__ == "__main__":
    main()
