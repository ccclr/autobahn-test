# Copyright(C) Facebook, Inc. and its affiliates.
from json import dump, load
from collections import OrderedDict


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    ''' The committee looks as follows:
        "authorities: {
            "name": {
                "stake": 1,
                "consensus: {
                    "consensus_to_consensus": x.x.x.x:x,
                },
                "primary: {
                    "primary_to_primary": x.x.x.x:x,
                    "worker_to_primary": x.x.x.x:x,
                },
                "workers": {
                    "0": {
                        "primary_to_worker": x.x.x.x:x,
                        "worker_to_worker": x.x.x.x:x,
                        "transactions": x.x.x.x:x
                    },
                    ...
                }
            },
            ...
        }
    '''

    def __init__(self, addresses, base_port):
        ''' The `addresses` field looks as follows:
            { 
                "name": ["host", "host", ...],
                ...
            }
        '''
        assert isinstance(addresses, OrderedDict)
        assert all(isinstance(x, str) for x in addresses.keys())
        assert all(
            isinstance(x, list) and len(x) > 1 for x in addresses.values()
        )
        assert all(
            isinstance(x, str) for y in addresses.values() for x in y
        )
        assert len({len(x) for x in addresses.values()}) == 1
        assert isinstance(base_port, int) and base_port > 1024

        port = base_port
        self.json = {'authorities': OrderedDict()}

        for name, hosts in addresses.items():
            host = hosts.pop(0)
            consensus_addr = {
                'consensus_to_consensus': f'{host}:{port}',
            }
            port += 1

            primary_addr = {
                'primary_to_primary': f'{host}:{port}',
                'worker_to_primary': f'{host}:{port + 1}'
            }
            port += 2

            workers_addr = OrderedDict()
            for j, host in enumerate(hosts):
                workers_addr[j] = {
                    'primary_to_worker': f'{host}:{port}',
                    'transactions': f'{host}:{port + 1}',
                    'worker_to_worker': f'{host}:{port + 2}',
                }
                port += 3

            self.json['authorities'][name] = {
                'stake': 1,
                'consensus': consensus_addr,
                'primary': primary_addr,
                'workers': workers_addr
            }

    def primary_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['primary_to_primary']]
        return addresses

    def workers_addresses(self, faults=0):
        ''' Returns an ordered list of list of workers' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, worker in authority['workers'].items():
                authority_addresses += [(id, worker['transactions'])]
            addresses.append(authority_addresses)
        return addresses

    def ips(self, name=None):
        ''' Returns all the ips associated with an authority (in any order). '''
        if name is None:
            names = list(self.json['authorities'].keys())
        else:
            names = [name]

        ips = set()
        for name in names:
            addresses = self.json['authorities'][name]['consensus']
            ips.add(self.ip(addresses['consensus_to_consensus']))

            addresses = self.json['authorities'][name]['primary']
            ips.add(self.ip(addresses['primary_to_primary']))
            ips.add(self.ip(addresses['worker_to_primary']))

            for worker in self.json['authorities'][name]['workers'].values():
                ips.add(self.ip(worker['primary_to_worker']))
                ips.add(self.ip(worker['worker_to_worker']))
                ips.add(self.ip(worker['transactions']))

        return list(ips)

    def remove_nodes(self, nodes):
        ''' remove the `nodes` last nodes from the committee. '''
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['authorities'].popitem()

    def size(self):
        ''' Returns the number of authorities. '''
        return len(self.json['authorities'])

    def workers(self):
        ''' Returns the total number of workers (all authorities altogether). '''
        return sum(len(x['workers']) for x in self.json['authorities'].values())

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @staticmethod
    def ip(address):
        assert isinstance(address, str)
        return address.split(':')[0]


class LocalCommittee(Committee):
    def __init__(self, names, port, workers):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        addresses = OrderedDict((x, ['127.0.0.1']*(1+workers)) for x in names)
        super().__init__(addresses, port)


class NodeParameters:
    def __init__(self, json):
        required_ints = [
            'timeout_delay', 'header_size', 'max_header_delay', 'gc_depth',
            'sync_retry_delay', 'sync_retry_nodes', 'batch_size', 'max_batch_delay'
        ]
        optional_bools = [
            'use_optimistic_tips', 'use_parallel_proposals', 'use_fast_path',
            'use_ride_share', 'simulate_asynchrony', 'use_fast_sync', 'use_exponential_timeouts'
        ]
        optional_ints = [
            'k', 'fast_path_timeout', 'car_timeout', 'egress_penalty'
        ]
        optional_lists = [
            'asynchrony_type', 'asynchrony_start', 'asynchrony_duration', 'affected_nodes'
        ]
        hotspot_info =[
            'node_id', 'hotspot-windows', 'hotspot-nodes', 'hotspot-rates'
        ]
        for key in required_ints:
            if key not in json or not isinstance(json[key], int):
                raise ConfigError(f'Malformed parameters: missing or invalid key {key}')
        for key in optional_bools:
            if key in json and not isinstance(json[key], bool):
                raise ConfigError(f'Invalid type for {key}, should be bool')
        for key in optional_ints:
            if key in json and not isinstance(json[key], int):
                raise ConfigError(f'Invalid type for {key}, should be int')
        for key in optional_lists:
            if key in json and not isinstance(json[key], list):
                raise ConfigError(f'Invalid type for {key}, should be list')
        for key in hotspot_info:
            if key in json and not isinstance(json[key], list):
                raise ConfigError(f'Invalid type for {key}, should be list')
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            print(json)
            self.faults = int(json['faults'])

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')
            self.nodes = [int(x) for x in nodes]

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')
            self.rate = [int(x) for x in rate]

            self.workers = int(json['workers'])

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1
            self.simulate_partition = bool(json['simulate_partition'])

            self.partition_nodes = int(json['partition_nodes'])
            self.partition_start = int(json['partition_start'])
            self.partition_duration = int(json['partition_duration'])
            
            # New hotspot parameters
            self.enable_hotspot = bool(json.get('enable_hotspot'))
            
            if self.enable_hotspot:
                # Hotspot time windows in format [[start1, end1], [start2, end2], ...]
                self.hotspot_windows = json.get('hotspot_windows')
                if not isinstance(self.hotspot_windows, list):
                    raise ConfigError('hotspot_windows must be a list of [start, end] pairs')
                
                # Validate window format
                for window in self.hotspot_windows:
                    if not isinstance(window, list) or len(window) != 2:
                        raise ConfigError('Each hotspot window must be [start, end] pair')
                    if not all(isinstance(x, int) and x >= 0 for x in window):
                        raise ConfigError('Hotspot window times must be non-negative integers')
                    if window[0] >= window[1]:
                        raise ConfigError('Hotspot window start must be less than end')
                
                # Number of hotspot nodes for each window
                self.hotspot_nodes = json.get('hotspot_nodes')
                if not isinstance(self.hotspot_nodes, list):
                    raise ConfigError('hotspot_nodes must be a list')
                if len(self.hotspot_nodes) != len(self.hotspot_windows):
                    raise ConfigError('hotspot_nodes length must match hotspot_windows length')
                if not all(isinstance(x, int) and x > 0 for x in self.hotspot_nodes):
                    raise ConfigError('hotspot_nodes must be positive integers')
                
                # Rate multipliers for each window
                self.hotspot_rates = json.get('hotspot_rates')
                if not isinstance(self.hotspot_rates, list):
                    raise ConfigError('hotspot_rates must be a list')
                if len(self.hotspot_rates) != len(self.hotspot_windows):
                    raise ConfigError('hotspot_rates length must match hotspot_windows length')
                if not all(isinstance(x, (int, float)) and x >= 0 for x in self.hotspot_rates):
                    raise ConfigError('hotspot_rates must be non-negative numbers')
            else:
                self.hotspot_windows = []
                self.hotspot_nodes = []
                self.hotspot_rates = []
            
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')

        # Validate hotspot parameters against total nodes
        if self.enable_hotspot:
            max_hotspot_nodes = max(self.hotspot_nodes) if self.hotspot_nodes else 0
            total_client_nodes = sum(self.nodes)  # Total number of client nodes
            if max_hotspot_nodes > total_client_nodes:
                raise ConfigError(f'Maximum hotspot nodes ({max_hotspot_nodes}) exceeds total client nodes ({total_client_nodes})')
            


class PlotParameters:
    def __init__(self, json):
        try:
            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            workers = json['workers']
            workers = workers if isinstance(workers, list) else [workers]
            if not workers:
                raise ConfigError('Missing number of workers')
            self.workers = [int(x) for x in workers]

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if len(self.nodes) > 1 and len(self.workers) > 1:
            raise ConfigError(
                'Either the "nodes" or the "workers can be a list (not both)'
            )

    def scalability(self):
        return len(self.workers) > 1
