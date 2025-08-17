# Copyright(C) Facebook, Inc. and its affiliates.

# This script produces the plot used in the paper [Narwhal and Tusk: A DAG-based
# Mempool and Efficient BFT Consensus](https://arxiv.org/abs/2105.11827). Its
# only dependency is [matplotlib](https://matplotlib.org/) v3.4.3.

from glob import glob
from os.path import join
import os
from itertools import cycle
from re import search
from copy import deepcopy
from statistics import mean, stdev
from collections import defaultdict
from re import findall, search, split
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from itertools import cycle
from os.path import join, abspath
from itertools import cycle

DATA_ROOT = '/home/ccclr0302/autobahn-test/benchmark/results/Scenario-2-v3'

# --- PARSE DATA ---


class Setup:
    def __init__(self, faults, nodes, workers, collocate, rate, tx_size):
        self.nodes = nodes
        self.workers = workers
        self.collocate = collocate
        self.rate = rate
        self.tx_size = tx_size
        self.faults = faults
        self.max_latency = 'any'

    def __str__(self):
        return (
            f' Faults: {self.faults}\n'
            f' Committee size: {self.nodes}\n'
            f' Workers per node: {self.workers}\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {self.rate} tx/s\n'
            f' Transaction size: {self.tx_size} B\n'
            f' Max latency: {self.max_latency} ms\n'
        )

    def __eq__(self, other):
        return isinstance(other, Setup) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @classmethod
    def from_str(cls, raw):
        faults = int(search(r'Faults: (\d+)', raw).group(1))
        nodes = int(search(r'Committee size: (\d+)', raw).group(1))

        tmp = search(r'Worker\(s\) per node: (\d+)', raw)
        workers = int(tmp.group(1)) if tmp is not None else 1

        tmp = search(r'Collocate primary and workers: (True|False)', raw)
        if tmp is not None:
            collocate = 'True' == tmp.group(1)
        else:
            collocate = 'True'

        rate = int(search(r'Input rate: (\d+)', raw).group(1))
        tx_size = int(search(r'Transaction size: (\d+)', raw).group(1))
        return cls(faults, nodes, workers, collocate, rate, tx_size)


class Result:
    def __init__(self, mean_tps, mean_latency, std_tps=0, std_latency=0):
        self.mean_tps = mean_tps
        self.mean_latency = mean_latency
        self.std_tps = std_tps
        self.std_latency = std_latency

    def __str__(self):
        return(
            f' TPS: {self.mean_tps} +/- {self.std_tps} tx/s\n'
            f' Latency: {self.mean_latency} +/- {self.std_latency} ms\n'
        )

    @classmethod
    def from_str(cls, raw):
        tps = int(search(r'.* End-to-end TPS: (\d+)', raw).group(1))
        latency = int(search(r'.* End-to-end latency: (\d+)', raw).group(1))
        if tps == 0:
            return None
        return cls(tps, latency)

    @classmethod
    def aggregate(cls, results):
        if len(results) == 1:
            return results[0]

        mean_tps = round(mean([x.mean_tps for x in results]))
        mean_latency = round(mean([x.mean_latency for x in results]))
        std_tps = round(stdev([x.mean_tps for x in results]))
        std_latency = round(stdev([x.mean_latency for x in results]))
        return cls(mean_tps, mean_latency, std_tps, std_latency)


class LogAggregator:
    def __init__(self, system, files, max_latencies):
        assert isinstance(system, str)
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)
        
        print(f"[debug] parsing {len(files)} files for system: {system}")

        self.system = system
        self.max_latencies = max_latencies

        data = ''
        for filename in files:
            with open(filename, 'r') as f:
                data += f.read()

        records = defaultdict(list)
        for chunk in data.replace(',', '').split('SUMMARY')[1:]:
            if chunk:
                result = Result.from_str(chunk)
                if result is not None:
                    records[Setup.from_str(chunk)] += [result]

        self.records = {k: Result.aggregate(v) for k, v in records.items()}

    def print(self):
        results = [
            self._print_latency(),
            self._print_tps(scalability=False),
            self._print_tps(scalability=True),
        ]
        for graph_type, records in results:
            for setup, values in records.items():
                data = '\n'.join(
                    f' Variable value: X={x}\n{y}' for x, y in values
                )
                string = (
                    '\n'
                    '-----------------------------------------\n'
                    ' RESULTS:\n'
                    '-----------------------------------------\n'
                    f'{setup}'
                    '\n'
                    f'{data}'
                    '-----------------------------------------\n'
                )

                filename = (
                    f'{self.system}.'
                    f'{graph_type}-'
                    f'{setup.faults}-'
                    f'{setup.nodes}-'
                    f'{setup.workers}-'
                    f'{setup.collocate}-'
                    f'{setup.rate}-'
                    f'{setup.tx_size}-'
                    f'{setup.max_latency}.txt'
                )
                with open(filename, 'w') as f:
                    f.write(string)

    def _print_latency(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'any'
            organized[setup] += [(result.mean_tps, result, rate)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[2])
            organized[setup] = [(x, y) for x, y, _ in results]

        return 'latency', organized

    def _print_tps(self, scalability):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for max_latency in self.max_latencies:
            for setup, result in records.items():
                setup = deepcopy(setup)
                if result.mean_latency <= max_latency:
                    setup.rate = 'any'
                    setup.max_latency = max_latency
                    if scalability:
                        variable = setup.workers
                        setup.workers = 'x'
                    else:
                        variable = setup.nodes
                        setup.nodes = 'x'

                    new_point = all(variable != x[0] for x in organized[setup])
                    highest_tps = False
                    for v, r in organized[setup]:
                        if result.mean_tps > r.mean_tps and variable == v:
                            organized[setup].remove((v, r))
                            highest_tps = True
                    if new_point or highest_tps:
                        organized[setup] += [(variable, result)]

        [v.sort(key=lambda x: x[0]) for v in organized.values()]
        return 'tps', organized


# --- MAKE THE PLOTS ---


@ticker.FuncFormatter
def default_major_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    else:
        return f'{x:.0f}'


def sec_major_formatter(x, pos):
    return f'{float(x)/1000:.1f}'


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self, width=6.4, height=4.8):
        self.fig, (self.ax1, self.ax2, self.ax3) = plt.subplots(1, 3, figsize=(width, height))
        self.reset_markers()
        self.reset_linestyles()
        
        self.colors = {
            'S1': {
                'A1': '#0d47a1',
                'A2': '#004d40',  
                'A3': '#ff7f0e', 
                'A4': 'tab:red'  
            },
            'S2': {
                'A1': '#0d47a1',
                'A2': '#004d40',
                'A3': '#ff7f0e',
                'A4': 'tab:red'
            },
            'S3': {
                'A1': '#0d47a1', 
                'A2': '#004d40', 
                'A3': '#ff7f0e', 
                'A4': 'tab:red' 
            }
        }
        
        self.style_cycle = cycle(['solid', 'dashed', 'dotted', 'dashdot'])
        
        self.marker_cycle = cycle(['o', 'v', 's', 'd'])
        
        self.marker_map = {}
        self.style_map = {}
        self.current_ax = None

    def reset_markers(self):
        self.markers = cycle(['o', 'v', 's', 'd'])

    def reset_linestyles(self):
        self.styles = cycle(['solid', 'dashed', 'dotted'])

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split('(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _variable(self, data):
        return [int(x) for x in findall(r'Variable value: X=(\d+)', data)]

    def _tps2bps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * size / 10**6

    def _bps2tps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * 10**6 / size

    def _plot(self, x_label, y_label, y_axis, z_axis, type, marker, color):
        self.results.sort(key=self._natural_keys, reverse=(type == 'tps'))
        seen_labels = set()
        
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)
            
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            label = z_axis(result)
            base = self.system.split('-')[0]  # S1/S2/S3
            variant = self.system.split('-')[1]  # A1, A2, A3
            
            if base == 'S1':
                ax = self.ax1
            elif base == 'S2':
                ax = self.ax2
            else:  # S3
                ax = self.ax3
            
            color = self.colors[base][variant]
            
            if self.system not in self.marker_map:
                self.marker_map[self.system] = next(self.marker_cycle)
            if self.system not in self.style_map:
                self.style_map[self.system] = next(self.style_cycle)

            marker = self.marker_map[self.system]
            style = self.style_map[self.system]

            if label and label not in seen_labels:
                ax.errorbar(
                    x_values, y_values, yerr=y_err, label=label,
                    linestyle=style, marker=marker, color=color, 
                    capsize=4,           
                    linewidth=2.5,       
                    markersize=8,        
                    markeredgewidth=1.5, 
                    markeredgecolor='white',  
                    alpha=0.9          
                )
                seen_labels.add(label)
            else:
                ax.errorbar(
                    x_values, y_values, yerr=y_err,
                    linestyle=style, marker=marker, color=color, 
                    capsize=4, linewidth=2.5, markersize=8,
                    markeredgewidth=1.5, markeredgecolor='white',
                    alpha=0.9
                )

    def _nodes(self, data):
        x_match = search(r'Committee size: (\d+|x)', data)
        x = x_match.group(1) if x_match else "Unknown"
        
        f_match = search(r'Faults: (\d+)', data)
        f = f_match.group(1) if f_match else "0"
        
        system_name = self.legend_name(self.system)
        if f != '0':
            return f'{system_name} ({f}F)'
        else:
            return f'{system_name}'

    def _workers(self, data):
        x_match = search(r'Workers per node: (\d+|x)', data)
        if x_match is None:
            print(f"[Error] Could not find Workers per node in data: {data[:200]}...")
            return "Unknown"
        x = x_match.group(1)
        
        f_match = search(r'Faults: (\d+)', data)
        if f_match is None:
            print(f"[Error] Could not find Faults in data: {data[:200]}...")
            return "Unknown"
        f = f_match.group(1)
        
        faults = f'({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        return f'{name}, {x} workers {faults}'

    def _max_latency(self, data):
        x_match = search(r'Max latency: (\d+|any)', data)
        if x_match is None:
            print(f"[Error] Could not find Max latency in data: {data[:200]}...")
            return "Unknown"
        x = x_match.group(1)
        
        f_match = search(r'Faults: (\d+)', data)
        if f_match is None:
            print(f"[Error] Could not find Faults in data: {data[:200]}...")
            return "Unknown"
        f = f_match.group(1)
        
        faults = f' ({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        
        if x == 'any':
            return f'{name}{faults}, Max latency: any'
        else:
            return f'{name}{faults}, Max latency: {float(x)/1000:,.1f}s'

    def _input_rate(self, data):
        x_match = search(r'Input rate: (\d+|any)', data)
        if x_match is None:
            print(f"[Error] Could not find Input rate in data: {data[:200]}...")
            return "Unknown"
        x = x_match.group(1)
        
        f_match = search(r'Faults: (\d+)', data)
        if f_match is None:
            print(f"[Error] Could not find Faults in data: {data[:200]}...")
            return "Unknown"
        f = f_match.group(1)
        
        faults = f' ({f} faulty)' if f != '0' else ''
        name = self.legend_name(self.system)
        
        if x == 'any':
            return f'{name}{faults}, Input rate: any'
        else:
            return f'{name}{faults}, Input rate: {float(x)/1000:,.0f}k'

    @staticmethod
    def legend_name(system):
        system_mapping = {
            "S1-A1": "S1-A1, Cut Condition = f+1, k = 1",
            "S1-A2": "S1-A2, Cut Condition = f+1, k = 3",
            "S1-A3": "S1-A3, Cut Condition = 2f+1, k = 1",
            "S1-A4": "S1-A4, Cut Condition = 2f+1, k = 3",
            "S2-A1": "S2-A1, Cut Condition = f+1, k = 1",
            "S2-A2": "S2-A2, Cut Condition = f+1, k = 3",
            "S2-A3": "S2-A3, Cut Condition = 2f+1, k = 1",
            "S2-A4": "S2-A4, Cut Condition = 2f+1, k = 3",
            "S3-A1": "S3-A1, Cut Condition = f+1, k = 1",
            "S3-A2": "S3-A2, Cut Condition = f+1, k = 3",
            "S3-A3": "S3-A3, Cut Condition = 2f+1, k = 1",
            "S3-A4": "S3-A4, Cut Condition = 2f+1, k = 3",
        }
        return system_mapping[system]

    def plot_latency(self, system, faults, nodes, workers, tx_size):
        assert isinstance(system, str)
        assert isinstance(nodes, list)
        assert all(isinstance(x, int) for x in nodes)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(tx_size, int)

        scalability = len(workers) > 1
        collocate = not scalability
        iterator = workers if scalability else nodes

        self.reset_markers()
        self.reset_linestyles()

        self.results = []
        for file in glob(join(DATA_ROOT, f'{system}.*.txt')):
            with open(file, 'r') as f:
                self.results.append(f.read().replace(',', ''))

        self.system = system
        z_axis = self._workers if scalability else self._nodes
        x_label = 'Throughput (tx /s)'
        y_label = ['Latency (ms)']
        marker = next(self.markers)
        system_group = self.system.split('-')[0]
        color = self.colors.get(system_group, 'tab:gray')
        self._plot(
            x_label, y_label, self._latency, z_axis, 'latency', marker, color
        )

    def plot_tps(self, system, faults, nodes, workers, tx_size, max_latencies):
        assert isinstance(system, str)
        assert isinstance(faults, list)
        assert all(isinstance(x, int) for x in faults)
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)
        assert isinstance(tx_size, int)

        scalability = len(workers) > 1
        collocate = not scalability

        self.reset_markers()
        self.reset_linestyles()

        self.results = []
        for f in faults:
            for l in max_latencies:
                filename = (
                    f'{system}.'
                    f'tps-'
                    f'{f}-'
                    f'{"x" if not scalability else nodes[0]}-'
                    f'{"x" if scalability else workers[0]}-'
                    f'{collocate}-'
                    f'any-'
                    f'{tx_size}-'
                    f'{l}.txt'
                )
                if os.path.isfile(filename):
                    with open(filename, 'r') as file:
                        self.results += [file.read().replace(',', '')]

        self.system = system
        z_axis = self._max_latency
        x_label = 'Workers per validator' if scalability else 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        marker = next(self.markers)
        color = next(self.colors)
        self._plot(x_label, y_label, self._tps, z_axis, 'tps', marker, color)

    def finalize(self, name, legend_cols, top_lim=None, legend_loc=None, legend_anchor=None):
        assert isinstance(name, str)

        self.ax1.set_title('S1 Series', fontsize=14, fontweight='bold', pad=20)
        self.ax2.set_title('S2 Series', fontsize=14, fontweight='bold', pad=20)
        self.ax3.set_title('S3 Series', fontsize=14, fontweight='bold', pad=20)
        
        self.ax1.set_xlabel('Throughput (tx/s)', fontsize=12, fontweight='bold')
        self.ax1.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
        self.ax2.set_xlabel('Throughput (tx/s)', fontsize=12, fontweight='bold')
        self.ax2.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
        self.ax3.set_xlabel('Throughput (tx/s)', fontsize=12, fontweight='bold')
        self.ax3.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
        
        self.ax1.set_xlim(left=70000)
        self.ax1.set_ylim(bottom=0, top=top_lim)
        self.ax2.set_xlim(left=70000)
        self.ax2.set_ylim(bottom=0, top=top_lim)
        self.ax3.set_xlim(left=70000)
        self.ax3.set_ylim(bottom=0, top=top_lim)
        
        self.ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        self.ax2.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        self.ax3.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        self.ax1.tick_params(axis='both', which='major', labelsize=10)
        self.ax2.tick_params(axis='both', which='major', labelsize=10)
        self.ax3.tick_params(axis='both', which='major', labelsize=10)
        
        self.ax1.set_facecolor('#f8f9fa')
        self.ax2.set_facecolor('#f8f9fa')
        self.ax3.set_facecolor('#f8f9fa')
        
        legend1 = self.ax1.legend(
            loc='upper left', 
            frameon=True, fancybox=True, shadow=True,
            framealpha=0.95, edgecolor='gray', facecolor='white',
            fontsize=9
        )
        legend2 = self.ax2.legend(
            loc='upper left',
            frameon=True, fancybox=True, shadow=True,
            framealpha=0.95, edgecolor='gray', facecolor='white',
            fontsize=9
        )
        legend3 = self.ax3.legend(
            loc='upper left',
            frameon=True, fancybox=True, shadow=True,
            framealpha=0.95, edgecolor='gray', facecolor='white',
            fontsize=9
        )
        
        plt.tight_layout()
        plt.subplots_adjust(wspace=0.3)

        for x in ['pdf', 'png']:
            plt.savefig(f'{name}.{x}', bbox_inches='tight', dpi=300)


if __name__ == '__main__':
    max_latencies = [3_000, 5_000]  # For TPS graphs.
    all_systems = [
        'S1-A1', 'S1-A2', 'S1-A3', 'S1-A4',
        'S2-A1', 'S2-A2', 'S2-A3', 'S2-A4',
        'S3-A1', 'S3-A2', 'S3-A3', 'S3-A4'
    ]

    # Parse the results.
    for system in all_systems:
        system_dir = join(DATA_ROOT, system)
        files = glob(join(system_dir, '*.txt'))
        if not files:
            print(f"[Warning] No input files found for system '{system}' in {system_dir}")
            continue
        print(f"[Info] Found {len(files)} files for system '{system}': {files}")
        LogAggregator(system, files, max_latencies).print()

    # Plot 'Happy path' graph.
    ploter = Ploter(width=24, height=6)
    for system in all_systems:
        print(f"[Info] Plotting latency for system: {system}")
        ploter.plot_latency(system, [0], [10, 50], [1], 512)
    ploter.finalize(
        'committee-latency',
        legend_cols=3,
        top_lim=15_000,
        legend_loc='lower left',
        legend_anchor=(0, 0)
    )

    # # Plot 'Dead nodes' graph.
    # ploter = Ploter()
    # for system in all_systems:
    #     ploter.plot_latency(system, [1, 3], [10], [1], 512)
    # ploter.finalize(
    #     'committee-latency-faults',
    #     legend_cols=1,
    #     top_lim=40_000,
    #     legend_loc='upper right',
    #     legend_anchor=(1, 1)
    # )

    # # Plot 'Scalability latency' graph.
    # ploter = Ploter(height=3.6)
    # for system in ['tusk', 'narwhal-hs']:
    #     ploter.plot_latency(system, [0], [4], [4, 7, 10], 512)
    # ploter.finalize('scalability-latency', legend_cols=2)

    # # Plot 'Scalability tps' graph.
    # ploter = Ploter(height=3.6)
    # for system in ['tusk', 'narwhal-hs']:
    #     ploter.plot_tps(system, [0], [4], [1, 4, 7, 10], 512, max_latencies)
    # ploter.finalize('scalability-tps', legend_cols=1)

    # Remove aggregated log files.
    for system in all_systems:
        for f in glob(join(DATA_ROOT, f'{system}.*.txt')):
            os.remove(f)