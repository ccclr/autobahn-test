# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.gcp_instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        EXCLUDED_ZONES = ['us-central1-c'] 
        manager = InstanceManager.make()
        settings = manager.settings
        hosts_dict = manager.hosts()

        filtered_hosts = {
            region: nodes for region, nodes in hosts_dict.items()
            if region.lower() not in EXCLUDED_ZONES
        }

        all_nodes = [ip for nodes in filtered_hosts.values() for ip in nodes]
        # Print(all_nodes)

        if not all_nodes:
            print("No hosts remaining after filtering.")
            return
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',
            'sudo apt-get -y install clang git curl',

            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            f'(git clone {self.settings.repo_url} {self.settings.repo_name} || (cd {self.settings.repo_name} && git pull --rebase))',
            f'(cd {self.settings.repo_name} && git checkout {self.settings.branch})',

            f'(cd {self.settings.repo_name} && source $HOME/.cargo/env && cargo build --release)',

            f'ln -sf {self.settings.repo_name}/target/release/node ~/node',
            f'ln -sf {self.settings.repo_name}/target/release/benchmark_client ~/benchmark_client',
        ]

        hosts = all_nodes
        print(hosts)
        try:
            g = Group(*hosts, user=self.settings.username, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user=self.settings.username, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)
            EXCLUDED_ZONES = ['us-central1-c']
            hosts_dict = self.manager.hosts()
            filtered_hosts = {
                region: nodes for region, nodes in hosts_dict.items()
                if region.lower() not in EXCLUDED_ZONES
            }
            all_nodes = [ip for nodes in filtered_hosts.values() for ip in nodes]
            if len(all_nodes) < nodes:
                Print.warn(f"Not enough hosts after excluding zones: {len(all_nodes)} < {nodes}")
                return []
            ordered = all_nodes
            return ordered[:nodes]
        else:
            primaries = max(bench_parameters.nodes)
            total_needed = primaries * (bench_parameters.workers + 1)
            hosts_dict = self.manager.hosts()
            all_nodes = [ip for nodes in hosts_dict.values() for ip in nodes]
            all_nodes = sorted(all_nodes, key=lambda ip: tuple(int(x) for x in ip.split('.')))
            if len(all_nodes) < total_needed:
                Print.warn(f"Not enough hosts: {len(all_nodes)} < {total_needed}")
                return []
            selected = []
            for i in range(primaries):
                group = all_nodes[i*(bench_parameters.workers+1):(i+1)*(bench_parameters.workers+1)]
                selected.append(group)
            return selected

    def _select_hosts_config(self, bench_parameters):
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)
            hosts = self.manager.internal_hosts()
            if sum(len(x) for x in hosts.values()) < nodes:
                return []
            ordered = [x for y in hosts.values() for x in y]
            assert len(ordered) >= nodes, f"Not enough hosts: got {len(ordered)}, need {nodes}"
            return ordered[:nodes]
        else:
            primaries = max(bench_parameters.nodes)
            total_needed = primaries * (bench_parameters.workers + 1)
            hosts = self.manager.internal_hosts()
            all_nodes = [ip for nodes in hosts.values() for ip in nodes]
            # 按IP地址从小到大排序
            all_nodes = sorted(all_nodes, key=lambda ip: tuple(int(x) for x in ip.split('.')))
            if len(all_nodes) < total_needed:
                return []
            selected = []
            for i in range(primaries):
                group = all_nodes[i*(bench_parameters.workers+1):(i+1)*(bench_parameters.workers+1)]
                selected.append(group)
            return selected



    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts, collocate):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        Print.info(
            f'Updating {len(ips)} machines (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]
        g = Group(*ips, user=self.settings.username, connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters, bench_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]

        if bench_parameters.collocate:
            workers = bench_parameters.workers
            addresses = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, y) for x, y in zip(names, hosts)
            )
        committee = Committee(addresses, self.settings.base_port)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files.
        names = names[:len(names)-bench_parameters.faults]
        progress = progress_bar(names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in committee.ips(name):
                c = Connection(ip, user=self.settings.username, connect_kwargs=self.connect)
                c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), '.')
                c.put(PathMaker.key_file(i), '.')
                c.put(PathMaker.parameters_file(), '.')

        return committee

    def _run_single(self, rate, committee, bench_parameters, debug=False):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and delete logs.
        hosts = committee.ips()
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        Print.info('Booting clients...')
        workers_addresses = committee.workers_addresses(faults)
        rate_share = ceil(rate / committee.workers())
        all_worker_nodes = [x for y in workers_addresses for _, x in y]
        
        hotspot_config = None
        if getattr(bench_parameters, 'enable_hotspot', False):
            hotspot_config = {
                'enable_hotspot': True,
                'hotspot_windows': getattr(bench_parameters, 'hotspot_windows', []),
                'hotspot_nodes': getattr(bench_parameters, 'hotspot_nodes', []),
                'hotspot_rates': getattr(bench_parameters, 'hotspot_rates', []),
            }
        print(hotspot_config)
        
        # for i, addresses in enumerate(workers_addresses):
        #     for (id, address) in addresses:
        #         host = Committee.ip(address)
        #         cmd = CommandMaker.run_client(
        #             address,
        #             bench_parameters.tx_size,
        #             rate_share,
        #             [x for y in workers_addresses for _, x in y]
        #         )
        #         # print(cmd)
        #         log_file = PathMaker.client_log_file(i, id)
        #         self._background_run(host, cmd, log_file)
        
        for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    host = Committee.ip(address)
                    cmd = CommandMaker.run_client(
                        address,
                        bench_parameters.tx_size,
                        rate_share,
                        all_worker_nodes,
                        node_id=i,
                        hotspot_config=hotspot_config
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(host, cmd, log_file)

        # Run the primaries (except the faulty ones).
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host = Committee.ip(address)
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host, cmd, log_file)

        # Run the workers (except the faulty ones).
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host, cmd, log_file)

         # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for i in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            #tick_size = ceil(duration / 20)
            #print(tick_size, i, bench_parameters.partition_start, bench_parameters.simulate_partition)
            #if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start:
            #    print('simulating partition')
            #    self._simulate_partition(bench_parameters, committee, faults)
            
            #if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start + bench_parameters.partition_duration:
            #    print('deleting partition')
            #    self._delete_partition(bench_parameters, committee, faults)

            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _simulate_partition(self, bench_parameters, committee, faults):
        partition_ips = []
        for i, address in enumerate(committee.primary_addresses(faults)):
            if i < bench_parameters.partition_nodes:
                print(i, address)
                cmd = []
                #cmd = ['sudo tc qdisc del dev ens4 root']
                cmd.append('sudo tc qdisc add dev ens4 root handle 1: htb')
                cmd.append('sudo tc class add dev ens4 parent 1: classid 1:1 htb rate 10gibps')
                idx = 2
                for j, addr in enumerate(committee.primary_addresses(faults)):
                    if i == j:
                        continue
                    cmd.append('sudo tc class add dev ens4 parent 1:1 classid 1:' + str(idx) + ' htb rate 10gibps')
                    cmd.append('sudo tc qdisc add dev ens4 handle ' + str(idx) + ': parent 1:' 
                            + str(idx) + ' netem delay 5000ms')
                    cmd.append('sudo tc filter add dev ens4 pref ' + str(idx) + ' protocol ip u32 match ip dst ' + 
                            Committee.ip(addr) + ' flowid 1:' + str(idx))
                    idx = idx + 1
                ip = [Committee.ip(address)]
                g = Group(*ip, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True) 
        

         
        #hosts = committee.ips()
        #cmd = ['sudo iptables -A OUTPUT -d ' + ip + ' -j DROP' for ip in partition_ips]
        #cmd = ['sudo tc qdisc add dev ens4 root netem delay 5000ms']
        
        #g = Group(*partition_ips, user='neilgiridharan', connect_kwargs=self.connect)
        #g.run(' && '.join(cmd), hide=True) 
        
        #for i, address in enumerate(committee.primary_addresses(faults)):
        
        #host = Committee.ip(address)
        #for partition_ip in partition_ips:
        #cmd = 'sudo iptables -A OUTPUT -d ' + partition_ip + '-j DROP'
        
        ##log_file = PathMaker.primary_log_file(i)
        #self._background_run(host, cmd, log_file)
    
    def _delete_partition(self, bench_parameters, committee, faults):
        partition_ips = []
        for i, address in enumerate(committee.primary_addresses(faults)):
            if i < bench_parameters.partition_nodes:
                partition_ips = [Committee.ip(address)]
                cmd = ['sudo tc qdisc del dev ens4 root']
                g = Group(*partition_ips, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True) 

       
        #hosts = committee.ips()
        #cmd = ['sudo iptables -F']
        #cmd = ['sudo tc qdisc del dev ens4 root']
        #g = Group(*partition_ips, user='neilgiridharan', connect_kwargs=self.connect)
        #g.run(' && '.join(cmd), hide=True) 
        
        #for i, address in enumerate(committee.primary_addresses(faults)):
        #    host = Committee.ip(address)
        #    cmd = 'sudo iptables -F'
        #    log_file = PathMaker.primary_log_file(i)
        #    self._background_run(host, cmd, log_file)

    def _logs(self, committee, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        workers_addresses = committee.workers_addresses(faults)
        progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in enumerate(progress):
            for id, address in addresses:
                host = Committee.ip(address)
                c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
                c.get(
                    PathMaker.client_log_file(i, id), 
                    local=PathMaker.client_log_file(i, id)
                )
                c.get(
                    PathMaker.worker_log_file(i, id), 
                    local=PathMaker.worker_log_file(i, id)
                )

        primary_addresses = committee.primary_addresses(faults)
        progress = progress_bar(primary_addresses, prefix='Downloading primaries logs:')
        for i, address in enumerate(progress):
            host = Committee.ip(address)
            c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
            c.get(
                PathMaker.primary_log_file(i), 
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        print(selected_hosts)
        try:
            self._update(selected_hosts, bench_parameters.collocate)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            r, committee_copy, bench_parameters, debug
                        )

                        faults = bench_parameters.faults
                        logger = self._logs(committee_copy, faults)
                        result_file = PathMaker.result_file(
                            faults,
                            n, 
                            bench_parameters.workers,
                            bench_parameters.collocate,
                            r, 
                            bench_parameters.tx_size, 
                        )
                        with open(result_file, 'a') as f:
                            f.write(logger.result())
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
