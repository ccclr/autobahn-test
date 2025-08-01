# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task, Connection
import time
import numpy as np

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.gcp_instance import InstanceManager
from benchmark.remote import Bench, BenchError
from fabric.transfer import Transfer
from paramiko import RSAKey, SSHException
from invoke.exceptions import UnexpectedExit
import os
from invoke import Responder
import sys

@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0, 
        'nodes': 4,
        'workers': 1,
        'rate': 150000,
        'tx_size': 512,
        'duration': 10,

        # Unused
        'simulate_partition': False,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 1,
        
        'enable_hotspot': True,
        'hotspot_windows':[[0, 10]],
        'hotspot_nodes': [2],
        'hotspot_rates': [0.8],
    }
    node_params = {
        'timeout_delay': 1_000,  # ms
        'header_size': 32,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 1_000,  # ms
        'sync_retry_nodes': 4,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'use_optimistic_tips': False,
        'use_parallel_proposals': True,
        'k': 1,
        'use_fast_path': True,
        'fast_path_timeout': 200,
        'use_ride_share': False,
        'car_timeout': 2000,
        'cut_condition_type': 1,

        'simulate_asynchrony': False,
        'asynchrony_type': [3],

        'asynchrony_start': [10_000], #ms
        'asynchrony_duration': [20_000], #ms
        'affected_nodes': [2],
        'egress_penalty': 50, #ms

        'use_fast_sync': True,
        'use_exponential_timeouts': True,
        
        
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=8):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=4):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)

from fabric import task


@task
def remote(ctx, debug=True):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 0,
        'nodes': [10],
        'workers': 1,
        'co-locate': True,
        # 'rate': [180_000, 190_000, 150_000, 100_000, 200_000],
        'rate': [180_000, 190_000],
        'tx_size': 512,
        'duration': 60,
        'runs': 2,

        # Unused
        'simulate_partition': False,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 2,
        
        'enable_hotspot': True,
        'hotspot_windows':[[0, 60]],
        'hotspot_nodes': [5],
        'hotspot_rates': [1],
    }
    node_params = {
        'timeout_delay': 1500,  # ms
        'header_size': 1000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 4,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'use_optimistic_tips': False,
        'use_parallel_proposals': True,
        'k': 1,
        'use_fast_path': False,
        'fast_path_timeout': 100,
        'use_ride_share': False,
        'car_timeout': 2000,
        'cut_condition_type': 2,

        'simulate_asynchrony': False,
        'asynchrony_type': [3],

        'asynchrony_start': [10_000], #ms
        'asynchrony_duration': [20_000], #ms
        'affected_nodes': [2],
        'egress_penalty': 100, #ms

        'use_fast_sync': True,
        'use_exponential_timeouts': True,
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [4],
        'workers': [1, 4, 7, 10],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [2_000, 2_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


@task
def latency(ctx):
    """
    Measure SSH latency (ms) between regions.
    Includes intra-region (i==j) and cross-region (i!=j) latency.
    """
    import time
    import numpy as np
    from fabric import Connection
    from paramiko import RSAKey, SSHException
    from invoke.exceptions import UnexpectedExit
    from benchmark.gcp_instance import InstanceManager
    from benchmark.utils import Print

    manager = InstanceManager.make()
    settings = manager.settings

    # 加载 SSH 私钥
    try:
        ctx.connect_kwargs.pkey = RSAKey.from_private_key_file("/home/ccclr0302/.ssh/google_compute_engine")
        connect_kwargs = ctx.connect_kwargs
    except (IOError, SSHException) as e:
        Print.error(f"Failed to load SSH key: {e}")
        return

    # 获取 hosts
    hosts_dict = manager.hosts()
    region_nodes = []

    for region, nodes in hosts_dict.items():
        if len(nodes) >= 2:
            region_nodes.append((region, nodes[0], nodes[1]))  # (region, node1, node2)
        else:
            Print.warn(f"[Skip] Region {region} has <2 nodes.")

    m = len(region_nodes)
    latency_matrix = np.zeros((m, m))
    region_names = [r[0] for r in region_nodes]

    # SSH latency 函数
    def ssh_latency(src, dst, repeat=3):
        if src == dst:
            return 0.0
        results = []
        for _ in range(repeat):
            try:
                conn = Connection(host=dst, user=settings.username, connect_kwargs=connect_kwargs)
                conn.run("echo warmup", hide=True, timeout=5)
                start = time.time()
                conn.run("echo hello", hide=True, timeout=5)
                end = time.time()
                conn.close()
                results.append((end - start) * 1000)  # ms
            except Exception as e:
                print(f"[Error] SSH {src} → {dst}: {e}")
                results.append(np.nan)
        valid = [x for x in results if not np.isnan(x)]
        return np.mean(valid) if valid else np.nan

    # 1. Intra-region latency
    for i in range(m):
        region, node1, node2 = region_nodes[i]
        latency = ssh_latency(node1, node2)
        latency_matrix[i][i] = latency / 2 if latency else np.nan
        print(f"[Intra] {region} ({node1} → {node2}): {latency_matrix[i][i]:.2f} ms")

    # 2. Cross-region latency
    for i in range(m):
        region_i, node_i, _ = region_nodes[i]
        for j in range(m):
            if i == j:
                continue
            region_j, node_j, _ = region_nodes[j]
            latency = ssh_latency(node_i, node_j)
            latency_matrix[i][j] = latency if latency else np.nan
            print(f"[Cross] {region_i} → {region_j}: {latency_matrix[i][j]:.2f} ms")
        print()

    print("\n=== SSH Latency Matrix (ms) ===")
    print("Regions:", region_names)
    print(latency_matrix)

    def average_latency(L):
        vals = [L[i][j] for i in range(m) for j in range(m) if i != j and not np.isnan(L[i][j])]
        return np.mean(vals)

    def asymmetry(L):
        vals = [abs(L[i][j] - L[j][i]) / (L[i][j] + L[j][i])
                for i in range(m) for j in range(m)
                if i != j and not np.isnan(L[i][j]) and not np.isnan(L[j][i]) and (L[i][j] + L[j][i]) > 0]
        return np.mean(vals)

    mu = average_latency(latency_matrix)
    asym = asymmetry(latency_matrix)

    print(f"\nAverage SSH Latency (Cross-region): {mu:.2f} ms")
    print(f"Asymmetry Degree: {asym:.4f}")
