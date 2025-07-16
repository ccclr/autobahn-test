# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(address, size, rate, nodes, node_id=None, hotspot_config=None):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        
        # Build base command
        nodes_str = f'--nodes {" ".join(nodes)}' if nodes else ''
        cmd = f'./benchmark_client {address} --size {size} --rate {rate} {nodes_str}'
        
        cmd += f' --node-id {node_id}'
        
        # Add hotspot configuration if provided
        if hotspot_config and hotspot_config.get('enable_hotspot', False):
            windows = hotspot_config.get('hotspot_windows', [])
            hotspot_nodes = hotspot_config.get('hotspot_nodes', [])
            hotspot_rates = hotspot_config.get('hotspot_rates', [])
            
            if windows and hotspot_nodes and hotspot_rates:
                # Convert windows to string format
                windows_str = ' '.join([f'{w[0]}:{w[1]}' for w in windows])
                nodes_str = ' '.join([str(n) for n in hotspot_nodes])
                rates_str = ' '.join([str(r) for r in hotspot_rates])
                
                cmd += f' --hotspot-windows {windows_str}'
                cmd += f' --hotspot-nodes {nodes_str}'
                cmd += f' --hotspot-rates {rates_str}'
        
        return cmd

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'