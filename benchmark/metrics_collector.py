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
class HardwareMetrics:
    """Layer 1: Hardware Metrics"""
    cpu_cores: int
    memory_gb: float
    network_bandwidth_mbps: float
    workers_per_node: int

@dataclass
class NetworkBaseline:
    """Layer 1: 网络基线指标"""
    intra_region_latency_ms: float
    cross_region_latency_ms: float
    latency_variance: float
    node_geographic_distribution: List[str]
    network_asymmetry: float

@dataclass
class DAGGrowthPattern:
    """Layer 2 Part I: DAG增长模式"""
    avg_lane_growth_rate: float
    lane_growth_variance: float
    total_lanes: int
    active_tips_count: int
    orphaned_tips_count: int
    dag_width: float
    dag_depth: float

@dataclass
class ConsensusMetrics:
    """Layer 2 Part II: 共识指标"""
    fast_path_ratio: float
    slow_path_ratio: float
    proposal_commit_success_rate: float
    consensus_latency_ms: float
    consensus_throughput_tps: float
    timeout_events: int
    retry_events: int
    commit_latency_by_slot_ms: Dict[int, float]
    commit_latency_summary_ms: Dict[str, float]
    

@dataclass
class SystemState:
    """完整的系统状态"""
    timestamp: float
    hardware: HardwareMetrics
    network: NetworkBaseline
    dag_growth: DAGGrowthPattern
    consensus: ConsensusMetrics
    node_params: Dict[str, Any]  # 当前节点参数

class MetricsCollector:
    """参数采集器主类"""
    
    def __init__(self, log_dir: str = "/home/ccclr0302/autobahn-test/benchmark/logs"):
        self.log_dir = Path(log_dir)
        self.committee_file = self.log_dir.parent / ".committee.json"
        self.parameters_file = self.log_dir.parent / ".parameters.json"
        
    def collect_hardware_capacity(self) -> HardwareMetrics:
        """收集硬件容量指标"""
        try:
            # CPU核心数
            cpu_cores = psutil.cpu_count(logical=False) or psutil.cpu_count()
            
            # 内存容量
            memory = psutil.virtual_memory()
            memory_gb = memory.total / (1024**3)
            
            # 磁盘I/O
            disk_io = psutil.disk_io_counters()
            disk_read_mbps = (disk_io.read_bytes / (1024**2)) if disk_io else 0
            disk_write_mbps = (disk_io.write_bytes / (1024**2)) if disk_io else 0
            
            # 网络带宽（简化估算）
            net_io = psutil.net_io_counters()
            network_bandwidth_mbps = 10000  # 假设1Gbps，实际应该动态测量
            
            # 每个节点的worker数量（从参数文件读取）
            workers_per_node = 1
            if self.parameters_file.exists():
                with open(self.parameters_file, 'r') as f:
                    params = json.load(f)
                    workers_per_node = params.get('workers', 1)
            
            return HardwareMetrics(
                cpu_cores=cpu_cores,
                memory_gb=memory_gb,
                disk_io_read_mbps=disk_read_mbps,
                disk_io_write_mbps=disk_write_mbps,
                network_bandwidth_mbps=network_bandwidth_mbps,
                workers_per_node=workers_per_node
            )
        except Exception as e:
            logger.error(f"收集硬件指标失败: {e}")
            return HardwareMetrics(0, 0, 0, 0, 0, 1)
    
    def collect_network_baseline(self) -> NetworkBaseline:
        """收集网络基线指标 - 集成fabfile版本"""
        try:
            # 方法1: 直接调用fabfile中的latency函数
            latency_matrix = self._call_fabfile_latency()
            
            if latency_matrix is not None:
                # 计算网络指标
                n = len(latency_matrix)
                intra_latencies = [latency_matrix[i][i] for i in range(n) if not np.isnan(latency_matrix[i][i])]
                cross_latencies = [latency_matrix[i][j] for i in range(n) for j in range(n) 
                                if i != j and not np.isnan(latency_matrix[i][j])]
                
                intra_region_latency = np.mean(intra_latencies) if intra_latencies else 1.0
                cross_region_latency = np.mean(cross_latencies) if cross_latencies else 50.0
                latency_variance = np.var(cross_latencies) if cross_latencies else 5.0
                
                # 计算网络不对称性
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
                    node_geographic_distribution=[f"region_{i}" for i in range(n)],
                    network_asymmetry=float(network_asymmetry)
                )
            else:
                logger.error(f"收集网络指标失败")
                return NetworkBaseline(1.0, 50.0, 5.0, ["local"], 0.1)
        except Exception as e:
            logger.error(f"收集网络指标失败: {e}")
            return NetworkBaseline(1.0, 50.0, 5.0, ["local"], 0.1)
            
    def _call_fabfile_latency(self) -> Optional[np.ndarray]:
        """直接调用fabfile中的latency函数"""
        try:
            import subprocess
            import os
            
            # 切换到项目目录
            project_dir = self.log_dir.parent.parent  # 回到autobahn-test目录
            original_cwd = os.getcwd()
            
            try:
                os.chdir(project_dir)
                
                # 调用fab latency命令
                result = subprocess.run(
                    ['fab', 'latency', '--intra-region=True'],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5分钟超时
                )
                
                if result.returncode == 0:
                    # 从输出文件读取结果
                    latency_files = list(self.log_dir.glob("latency_*_matrix_*.npy"))
                    if latency_files:
                        latest_file = max(latency_files, key=os.path.getctime)
                        return np.load(latest_file)
                
                return None
                
            finally:
                os.chdir(original_cwd)
                
        except Exception as e:
            logger.error(f"调用fab latency命令失败: {e}")
            return None
    
    def collect_dag_growth_pattern(self) -> DAGGrowthPattern:
        """收集DAG增长模式指标 - 基于validator pk聚合lane，保留每个lane的独立增长速度"""
        try:
            print("=== 开始收集DAG增长模式指标 ===")
            
            # 从primary日志中提取DAG信息
            log_dir = Path("/home/ccclr0302/autobahn-test/benchmark/logs/")
            primary_logs = list(log_dir.glob("primary-0.log"))
            print(f"找到 {len(primary_logs)} 个primary日志文件: {[f.name for f in primary_logs]}")
            
            if not primary_logs:
                print("没有找到primary日志文件，返回默认值")
                return DAGGrowthPattern(0, 0, 0, 0, 0, 0, 0)
            
            # 收集所有prepare事件，按validator pk聚合
            prepare_events = []  # [(slot, validator_pk, proposal_height, timestamp)]
            total_lines_processed = 0
            prepare_lines_found = 0
        
            for log_file in primary_logs:
                print(f"\n处理日志文件: {log_file.name}")
                with open(log_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        total_lines_processed += 1
                        
                        # 提取prepare事件
                        slot_match = re.search(r'prepare slot (\d+)', line)
                        if slot_match:
                            prepare_lines_found += 1
                            slot = int(slot_match.group(1))
                            
                            # 提取validator pk
                            validator_match = re.search(r'validator: ([A-Za-z0-9+/=]+)', line)
                            if validator_match:
                                validator_pk = validator_match.group(1)
                                
                                # 提取时间戳
                                timestamp_match = re.search(r'\[(.*?Z)', line)
                                if timestamp_match:
                                    timestamp_str = timestamp_match.group(1)
                                    try:
                                        from datetime import datetime
                                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                        timestamp = dt.timestamp()
                                        
                                        # 提取proposal height
                                        height_match = re.search(r'proposal height (\d+)', line)
                                        if height_match:
                                            proposal_height = int(height_match.group(1))
                                            prepare_events.append((slot, validator_pk, proposal_height, timestamp))
                                            
                                            # 打印前几个事件作为示例
                                            if len(prepare_events) <= 5:
                                                print(f"  事件 {len(prepare_events)}: slot={slot}, validator={validator_pk[:16]}..., height={proposal_height}, time={timestamp_str}")
                                            
                                    except Exception as e:
                                        logger.debug(f"解析时间戳失败: {e}")
                                        continue
                            #     else:
                            #         # print(f"  警告: 第{line_num}行没有找到时间戳")
                            # else:
                            #     # print(f"  警告: 第{line_num}行没有找到validator信息")
                        # else:
                            # 每处理1000行打印一次进度
                            # if line_num % 10000 == 0:
                            #     print(f"  已处理 {line_num} 行...")
            
            print(f"\n日志处理完成:")
            print(f"  总处理行数: {total_lines_processed}")
            print(f"  找到prepare事件行数: {prepare_lines_found}")
            print(f"  成功解析的事件数: {len(prepare_events)}")
            
            if not prepare_events:
                print("没有找到有效的prepare事件，返回默认值")
                return DAGGrowthPattern(0, 0, 0, 0, 0, 0, 0)
            
            # 按时间排序
            prepare_events.sort(key=lambda x: x[3])
            print(f"\n事件按时间排序完成，时间范围: {prepare_events[0][3]} 到 {prepare_events[-1][3]}")
            
            # 按validator pk聚合，计算每个validator的lane growth
            validator_lanes = {}  # {validator_pk: [(slot, height, timestamp), ...]}
            
            for slot, validator_pk, proposal_height, timestamp in prepare_events:
                if validator_pk not in validator_lanes:
                    validator_lanes[validator_pk] = []
                validator_lanes[validator_pk].append((slot, proposal_height, timestamp))
            
            print(f"\n按validator聚合完成:")
            print(f"  发现 {len(validator_lanes)} 个不同的validator")
            for i, (validator_pk, events) in enumerate(validator_lanes.items()):
                if i < 3:  # 只打印前3个validator的详细信息
                    print(f"  Validator {i+1}: {validator_pk[:16]}... 有 {len(events)} 个事件")
                elif i == 3:
                    print(f"  ... 还有 {len(validator_lanes) - 3} 个validator")
                    break
            
            # 计算每个validator的lane growth rate，基于proposal height的变化
            validator_growth_rates = {}  # {validator_pk: [growth_rate1, growth_rate2, ...]}
            time_window = 10.0  # 10秒时间窗口
            print(f"\n开始计算每个validator的lane growth rate (时间窗口: {time_window}秒):")
            
            for validator_pk, lane_events in validator_lanes.items():
                if not lane_events:
                    continue
                
                # 按时间排序该validator的事件
                lane_events.sort(key=lambda x: x[2])
                
                print(f"\n分析Validator {validator_pk[:16]}...:")
                print(f"  总事件数: {len(lane_events)}")
                print(f"  Height范围: {min(event[1] for event in lane_events)} 到 {max(event[1] for event in lane_events)}")
                
                # 计算该validator在时间窗口内的增长率
                current_time = lane_events[0][2]
                window_events = []
                window_count = 0
                validator_growth_rates[validator_pk] = []
                
                for slot, height, timestamp in lane_events:
                    if timestamp - current_time <= time_window:
                        window_events.append((slot, height, timestamp))
                    else:
                        # 计算当前窗口的增长率（基于height变化）
                        if len(window_events) > 1:
                            # 按时间排序窗口内的事件
                            window_events.sort(key=lambda x: x[2])
                            start_height = window_events[0][1]
                            end_height = window_events[-1][1]
                            start_time = window_events[0][2]
                            end_time = window_events[-1][2]
                            
                            # 计算height增长率和时间跨度
                            height_growth = end_height - start_height
                            time_span = end_time - start_time
                            
                            if time_span > 0:
                                growth_rate = height_growth / time_span  # height per second
                                validator_growth_rates[validator_pk].append(growth_rate)
                                window_count += 1
                                
                                # 打印前几个窗口的详细信息
                                if window_count <= 3:
                                    print(f"    窗口 {window_count}: height {start_height} -> {end_height} ({height_growth}), 时间 {time_span:.2f}s, 增长率 {growth_rate:.2f} height/s")
                        
                        # 开始新的时间窗口
                        current_time = timestamp
                        window_events = [(slot, height, timestamp)]
                
                # 处理最后一个窗口
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
                            print(f"    窗口 {window_count}: height {start_height} -> {end_height} ({height_growth}), 时间 {time_span:.2f}s, 增长率 {growth_rate:.2f} height/s")
            
            # 打印每个validator的独立增长速度
            print(f"\n每个validator的独立增长速度:")
            for validator_pk, growth_rates in validator_growth_rates.items():
                if growth_rates:
                    avg_rate = np.mean(growth_rates)
                    std_rate = np.std(growth_rates)
                    print(f"  Validator {validator_pk[:16]}... 平均增长速度: {avg_rate:.2f} height/s, 标准差: {std_rate:.2f}, 窗口数: {len(growth_rates)}")
            
            # 计算整体指标（基于所有validator的增长率）
            all_growth_rates = []
            for growth_rates in validator_growth_rates.values():
                all_growth_rates.extend(growth_rates)
            
            print(f"\n整体Growth rate统计:")
            print(f"  总窗口数: {len(all_growth_rates)}")
            print(f"  Growth rates: {all_growth_rates[:10]}{'...' if len(all_growth_rates) > 10 else ''}")
            
            # 计算指标
            avg_lane_growth_rate = np.mean(all_growth_rates) if all_growth_rates else 0
            lane_growth_variance = np.var(all_growth_rates) if all_growth_rates else 0
            
            print(f"\n计算最终指标:")
            print(f"  平均lane growth rate: {avg_lane_growth_rate:.2f} height/s")
            print(f"  Lane growth variance: {lane_growth_variance:.2f}")
            
            # 计算其他指标
            total_lanes = len(validator_lanes)  # 每个validator一个lane
            active_tips_count = len(validator_lanes)  # 所有validator都是active的
            orphaned_tips_count = 0  # 暂时设为0，可以根据实际需求调整
            
            # 计算DAG宽度和深度
            dag_width = len(validator_lanes) if len(validator_lanes) > 0 else 1

            # DAG depth 取4条lane中最长的那条的长度（以每条lane的最大proposal height为准）
            lane_max_heights = []
            for events in validator_lanes.values():
                if events:
                    lane_max_heights.append(max(h for _, h, _ in events))
            dag_depth = max(lane_max_heights) if lane_max_heights else 1
            
            print(f"  总lanes数: {total_lanes}")
            print(f"  Active tips数: {active_tips_count}")
            print(f"  Orphaned tips数: {orphaned_tips_count}")
            print(f"  DAG宽度: {dag_width}")
            print(f"  DAG深度: {dag_depth}")
            
            result = DAGGrowthPattern(
                avg_lane_growth_rate=float(avg_lane_growth_rate),
                lane_growth_variance=float(lane_growth_variance),
                total_lanes=total_lanes,
                active_tips_count=active_tips_count,
                orphaned_tips_count=orphaned_tips_count,
                dag_width=float(dag_width),
                dag_depth=float(dag_depth)
            )
            
            print(f"\n=== DAG增长模式指标收集完成 ===")
            print(f"最终结果: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"收集DAG增长指标失败: {e}")
            print(f"错误: {e}")
            return DAGGrowthPattern(0, 0, 0, 0, 0, 0, 0)
    
    def collect_consensus_metrics(self) -> ConsensusMetrics:
        """收集共识指标"""
        try:
            # 从primary日志中提取共识信息
            log_dir = Path("/home/ccclr0302/autobahn-test/benchmark/logs/")
            primary_logs = list(log_dir.glob("primary-*.log"))
            if not primary_logs:
                return ConsensusMetrics(0, 0, 0, 0, 0, 0, 0)
            
            fast_path_count = 0
            slow_path_count = 0
            commit_success_count = 0
            commit_total = 0
            timeout_events = 0
            retry_events = 0
            consensus_latencies = []
            
            # 记录每个slot的首次prepare与首次commit时间
            first_prepare_ts: Dict[int, float] = {}
            first_commit_ts: Dict[int, float] = {}
            
            for log_file in primary_logs:
                with open(log_file, 'r') as f:
                    for line in f:
                        # 统计快慢路径
                        if "taking fast path" in line.lower():
                            fast_path_count += 1
                        elif "taking slow path" in line.lower():
                            slow_path_count += 1
                        
                        # 统计提交成功率
                        if "committed" in line.lower() and "B" in line:
                            commit_total += 1
                            if "success" in line.lower():
                                commit_success_count += 1
                        
                        # 统计超时和重试事件
                        if "timeout" in line.lower():
                            timeout_events += 1
                        if "retry" in line.lower():
                            retry_events += 1
                        
                        # 提取延迟信息
                        latency_match = re.search(r"latency.*?(\d+\.?\d*)\s*ms", line)
                        if latency_match:
                            consensus_latencies.append(float(latency_match.group(1)))
                    
                        timestamp = None
                        timestamp_match = re.search(r'\[(.*?Z)', line)
                        if timestamp_match:
                            ts = timestamp_match.group(1)         # 取方括号内的时间串
                            try:
                                from datetime import datetime, timezone
                                if ts.endswith('Z'):
                                    ts = ts[:-1] + '+00:00'       # Z 标准化为 +00:00
                                dt = datetime.fromisoformat(ts)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                timestamp = dt.timestamp()
                            except Exception as e:
                                logger.debug(f"解析时间戳失败: {e}")

                        # 示例：仅当 timestamp 可用时再写入
                        prep_m = re.search(r"prepare slot\s+(\d+)", line)
                        if prep_m and timestamp is not None:
                            s = int(prep_m.group(1))
                            first_prepare_ts.setdefault(s, timestamp)

                        commit_m = re.search(r"processing commit in slot\s+(\d+)", line)
                        if commit_m and timestamp is not None:
                            commit_slot = int(commit_m.group(1))
                            first_commit_ts.setdefault(commit_slot, timestamp)
                        
            # 计算每个slot的commit延迟（ms）：首次commit - 首次prepare
            commit_latency_by_slot_ms: Dict[int, float] = {}
            for s, t_prep in first_prepare_ts.items():
                if s in first_commit_ts:
                    commit_latency_by_slot_ms[s] = float(max(0.0, (first_commit_ts[s] - t_prep) * 1000.0))

            # 生成分布摘要
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
            commit_success_rate = commit_success_count / commit_total if commit_total > 0 else 0
            consensus_latency = np.mean(consensus_latencies) if consensus_latencies else 0
            
            # 估算吞吐量（简化）
            consensus_throughput = commit_total / 60.0 if commit_total > 0 else 0  # 假设1分钟内的TPS
            
            return ConsensusMetrics(
                fast_path_ratio=float(fast_path_ratio),
                slow_path_ratio=float(slow_path_ratio),
                proposal_commit_success_rate=float(commit_success_rate),
                consensus_latency_ms=float(consensus_latency),
                consensus_throughput_tps=float(consensus_throughput),
                timeout_events=timeout_events,
                retry_events=retry_events,
                commit_latency_by_slot_ms=commit_latency_by_slot_ms,
                commit_latency_summary_ms=summary,
            )
        except Exception as e:
            logger.error(f"收集共识指标失败: {e}")
            return ConsensusMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)
    
    def load_current_node_params(self) -> Dict[str, Any]:
        """加载当前节点参数"""
        try:
            if self.parameters_file.exists():
                with open(self.parameters_file, 'r') as f:
                    return json.load(f)
            else:
                # 返回默认参数
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
            logger.error(f"加载节点参数失败: {e}")
            return {}
    
    def collect_system_state(self) -> SystemState:
        """收集完整的系统状态"""
        logger.info("开始收集系统状态...")
        
        hardware = self.collect_hardware_capacity()
        network = self.collect_network_baseline()
        dag_growth = self.collect_dag_growth_pattern()
        consensus = self.collect_consensus_metrics()
        node_params = self.load_current_node_params()
        
        return SystemState(
            timestamp=time.time(),
            hardware=hardware,
            network=network,
            dag_growth=dag_growth,
            consensus=consensus,
            node_params=node_params
        )
    
    def save_state(self, state: SystemState, filename: Optional[str] = None) -> str:
        """保存系统状态到文件"""
        if filename is None:
            timestamp = int(time.time())
            filename = f"system_state_{timestamp}.json"
        
        filepath = self.log_dir / filename
        
        # 转换为可序列化的字典
        state_dict = {
            'timestamp': state.timestamp,
            'hardware': asdict(state.hardware),
            'network': asdict(state.network),
            'dag_growth': asdict(state.dag_growth),
            'consensus': asdict(state.consensus),
            'node_params': state.node_params
        }
        
        with open(filepath, 'w') as f:
            json.dump(state_dict, f, indent=2)
        
        logger.info(f"系统状态已保存到: {filepath}")
        return str(filepath)
    
    def get_state_vector(self, state: SystemState) -> np.ndarray:
        """将系统状态转换为RL状态向量"""
        # Layer 1: 硬件和网络指标
        hardware_vec = [
            state.hardware.cpu_cores,
            state.hardware.memory_gb,
            state.hardware.disk_io_read_mbps,
            state.hardware.disk_io_write_mbps,
            state.hardware.network_bandwidth_mbps,
            state.hardware.workers_per_node
        ]
        
        network_vec = [
            state.network.intra_region_latency_ms,
            state.network.cross_region_latency_ms,
            state.network.latency_variance,
            state.network.network_asymmetry
        ]
        
        # Layer 2: DAG增长模式
        dag_vec = [
            state.dag_growth.avg_lane_growth_rate,
            state.dag_growth.lane_growth_variance,
            state.dag_growth.total_lanes,
            state.dag_growth.active_tips_count,
            state.dag_growth.orphaned_tips_count,
            state.dag_growth.dag_width,
            state.dag_growth.dag_depth
        ]
        
        # Layer 2: 共识指标
        consensus_vec = [
            state.consensus.fast_path_ratio,
            state.consensus.slow_path_ratio,
            state.consensus.proposal_commit_success_rate,
            state.consensus.consensus_latency_ms,
            state.consensus.consensus_throughput_tps,
            state.consensus.timeout_events,
            state.consensus.retry_events,
            # state.consensus.commit_latency_by_slot_ms,
            # state.consensus.commit_latency_summary_ms,
        ]
        
        # 合并所有向量
        state_vector = np.concatenate([hardware_vec, network_vec, dag_vec, consensus_vec])
        
        # 归一化（可选）
        state_vector = np.nan_to_num(state_vector, nan=0.0, posinf=1.0, neginf=0.0)
        
        return state_vector

def main():
    """主函数，用于测试参数采集器"""
    collector = MetricsCollector()
    
    # 收集系统状态
    state = collector.collect_system_state()
    
    # 打印状态信息
    print("=== 系统状态采集结果 ===")
    print(f"时间戳: {state.timestamp}\n")
    print(f"硬件容量: {asdict(state.hardware)}\n")
    print(f"网络基线: {asdict(state.network)}\n")
    print(f"DAG增长模式: {asdict(state.dag_growth)}\n")
    print(f"共识指标: {asdict(state.consensus)}\n")
    print(f"节点参数: {state.node_params}\n")
    
    # 保存状态
    filename = collector.save_state(state)
    print(f"状态已保存到: {filename}\n")
    
    # 获取状态向量
    state_vector = collector.get_state_vector(state)
    print(f"状态向量维度: {len(state_vector)}\n")
    print(f"状态向量: {state_vector}\n")

if __name__ == "__main__":
    main()
