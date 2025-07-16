# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
import json
import os

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0, parameters_path='.parameters.json'):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        self.parameters_json = {}
        if os.path.exists(parameters_path):
            with open(parameters_path, 'r') as f:
                self.parameters_json = json.load(f)

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples, self.hotspot_info \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        def parse_int(pattern):
            m = search(pattern, log)
            return int(m.group(1)) if m else None
        def parse_time(pattern):
            m = search(pattern, log)
            return self._to_posix(m.group(1)) if m else None

        size = parse_int(r'Transactions size: (\d+)')
        rate = parse_int(r'Transactions rate: (\d+)')
        start = parse_time(r'\[(.*Z) .* Start ')
        misses = len(findall(r'rate too high', log))
        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp} if tmp else {}

        # Parse hotspot information
        hotspot_info = {}
        node_id = parse_int(r'Node ID: (\d+)')
        if node_id is not None:
            hotspot_info['node_id'] = node_id
        
        total_nodes = parse_int(r'Total nodes: (\d+)')
        if total_nodes is not None:
            hotspot_info['total_nodes'] = total_nodes
        
        # Parse hotspot configuration if present
        hotspot_config_match = search(r'Hotspot configuration enabled:', log)
        if hotspot_config_match:
            hotspot_info['enabled'] = True
            # Extract hotspot windows information
            windows_matches = findall(r'Window \d+: (\d+)s-(\d+)s, (\d+) hotspot nodes, ([\d.]+)% rate increase', log)
            if windows_matches:
                windows = []
                for start_s, end_s, nodes, rate_pct in windows_matches:
                    windows.append({
                        'start': int(start_s),
                        'end': int(end_s),
                        'nodes': int(nodes),
                        'rate_increase': float(rate_pct) / 100.0
                    })
                hotspot_info['windows'] = windows
        else:
            hotspot_info['enabled'] = False

        # Parse dynamic rate information
        rate_changes = findall(r'Current transaction rate: ([\d.]+) tx/s at time (\d+)s', log)
        if rate_changes:
            hotspot_info['rate_changes'] = [(float(rate), int(time)) for rate, time in rate_changes]

        return size, rate, start, misses, samples, hotspot_info

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        def parse_int(pattern):
            m = search(pattern, log)
            return int(m.group(1)) if m else None
        def parse_bool(pattern):
            m = search(pattern, log)
            return m.group(1) == 'True' if m else None
        def parse_list(pattern):
            m = search(pattern, log)
            return eval(m.group(1)) if m else None

        configs = {
            'timeout_delay': parse_int(r'Timeout delay .* (\d+)'),
            'header_size': parse_int(r'Header size .* (\d+)'),
            'max_header_delay': parse_int(r'Max header delay .* (\d+)'),
            'gc_depth': parse_int(r'Garbage collection depth .* (\d+)'),
            'sync_retry_delay': parse_int(r'Sync retry delay .* (\d+)'),
            'sync_retry_nodes': parse_int(r'Sync retry nodes .* (\d+)'),
            'batch_size': parse_int(r'Batch size .* (\d+)'),
            'max_batch_delay': parse_int(r'Max batch delay .* (\d+)'),
            'use_optimistic_tips': parse_bool(r'Use optimistic tips: (True|False)'),
            'use_parallel_proposals': parse_bool(r'Use parallel proposals: (True|False)'),
            'k': parse_int(r'k: (\d+)'),
            'use_fast_path': parse_bool(r'Use fast path: (True|False)'),
            'fast_path_timeout': parse_int(r'Fast path timeout: (\d+)'),
            'use_ride_share': parse_bool(r'Use ride share: (True|False)'),
            'car_timeout': parse_int(r'Car timeout: (\d+)'),
            'simulate_asynchrony': parse_bool(r'Simulate asynchrony: (True|False)'),
            'asynchrony_type': parse_list(r'Asynchrony type: (\[.*?\])'),
            'asynchrony_start': parse_list(r'Asynchrony start: (\[.*?\])'),
            'asynchrony_duration': parse_list(r'Asynchrony duration: (\[.*?\])'),
            'affected_nodes': parse_list(r'Affected nodes: (\[.*?\])'),
            'egress_penalty': parse_int(r'Egress penalty: (\d+)'),
            'use_fast_sync': parse_bool(r'Use fast sync: (True|False)'),
            'use_exponential_timeouts': parse_bool(r'Use exponential timeouts: (True|False)'),
        }

        m = search(r'booted on (\d+.\d+.\d+.\d+)', log)
        ip = m.group(1) if m else None

        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        list_latencies = []
        first_start = 0
        set_first = True
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    if set_first:
                        first_start = start
                        first_end = end
                        set_first = False
                    latency += [end-start]
                    list_latencies += [(start-first_start, end-first_start, end-start)]

        list_latencies.sort(key=lambda tup: tup[0])
        with open('latencies.txt', 'w') as f:
            for line in list_latencies:
                f.write(str(line[0]) + ',' + str(line[1]) + ',' + str((line[2])) + '\n')
        return mean(latency) if latency else 0

    def _analyze_hotspot_performance(self):
        """Analyze hotspot performance metrics"""
        hotspot_analysis = {}
        
        # Check if hotspot was enabled
        hotspot_enabled = any(info.get('enabled', False) for info in self.hotspot_info)
        hotspot_analysis['enabled'] = hotspot_enabled
        
        if hotspot_enabled:
            # Analyze rate changes per node
            node_performances = {}
            for i, info in enumerate(self.hotspot_info):
                if 'rate_changes' in info:
                    node_id = info.get('node_id', i)
                    node_performances[node_id] = {
                        'rate_changes': info['rate_changes'],
                        'base_rate': self.rate[i] if i < len(self.rate) else 0
                    }
            
            hotspot_analysis['node_performances'] = node_performances
            
            # Extract hotspot windows configuration
            if self.hotspot_info and 'windows' in self.hotspot_info[0]:
                hotspot_analysis['windows'] = self.hotspot_info[0]['windows']
        
        return hotspot_analysis

    def result(self):
        # 合并 configs[0] 和 self.parameters_json，优先用 configs[0]
        cfg = self.parameters_json.copy()
        for k, v in self.configs[0].items():
            if v is not None:
                cfg[k] = v
        
        consensus_latency = (self._consensus_latency() or 0) * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        consensus_tps = consensus_tps or 0
        consensus_bps = consensus_bps or 0

        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_tps = end_to_end_tps or 0
        end_to_end_bps = end_to_end_bps or 0
        duration = duration or 0

        end_to_end_latency = (self._end_to_end_latency() or 0) * 1_000

        
        # Analyze hotspot performance
        hotspot_analysis = self._analyze_hotspot_performance()
        
        def show(key, unit=""):
            v = cfg.get(key, None)
            if v is None:
                return f'N/A{unit}'
            if isinstance(v, list):
                return f'{v}{unit}'
            return f'{v}{unit}'

        # Build hotspot summary
        hotspot_summary = ""
        print(hotspot_analysis)
        if hotspot_analysis['enabled']:
            hotspot_summary += f' Enable hotspot: {hotspot_analysis["enabled"]}\n'
            if 'windows' in hotspot_analysis:                
                # Show per-window analysis
                for i, window in enumerate(hotspot_analysis['windows']):
                    hotspot_summary += f' Window {i+1}: {window["start"]}s-{window["end"]}s, '
                    hotspot_summary += f'{window["nodes"]} nodes, {window["rate_increase"]*100:.1f}% increase\n'
        else:
            hotspot_summary += f' Enable hotspot: False\n'

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {", ".join(str(r) for r in self.rate if r is not None)} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Timeout delay: {show("timeout_delay", " ms")}\n'
            f' Header size: {show("header_size", " B")}\n'
            f' Max header delay: {show("max_header_delay", " ms")}\n'
            f' GC depth: {show("gc_depth", " round(s)")}\n'
            f' Sync retry delay: {show("sync_retry_delay", " ms")}\n'
            f' Sync retry nodes: {show("sync_retry_nodes", " node(s)")}\n'
            f' Batch size: {show("batch_size", " B")}\n'
            f' Max batch delay: {show("max_batch_delay", " ms")}\n'
            f' Use optimistic tips: {show("use_optimistic_tips")}\n'
            f' Use parallel proposals: {show("use_parallel_proposals")}\n'
            f' k: {show("k")}\n'
            f' Use fast path: {show("use_fast_path")}\n'
            f' Fast path timeout: {show("fast_path_timeout", " ms")}\n'
            f' Use ride share: {show("use_ride_share")}\n'
            f' Car timeout: {show("car_timeout", " ms")}\n'
            f' Simulate asynchrony: {show("simulate_asynchrony")}\n'
            f' Asynchrony type: {show("asynchrony_type")}\n'
            f' Asynchrony start: {show("asynchrony_start")}\n'
            f' Asynchrony duration: {show("asynchrony_duration")}\n'
            f' Affected nodes: {show("affected_nodes")}\n'
            f' Egress penalty: {show("egress_penalty", " ms")}\n'
            f' Use fast sync: {show("use_fast_sync")}\n'
            f' Use exponential timeouts: {show("use_exponential_timeouts")}\n'
            '\n'
            ' + HOTSPOT CONFIG:\n'
            f'{hotspot_summary}'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )
    
    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
