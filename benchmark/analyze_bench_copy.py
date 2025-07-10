import re
import matplotlib.pyplot as plt
import numpy as np

def parse_bench_file(filename):
    results = []
    with open(filename, 'r') as f:
        content = f.read()
    # 按 SUMMARY 分段
    blocks = content.split('-----------------------------------------')
    for block in blocks:
        # Max header delay
        # m = re.search(r'Max header delay:\s*([0-9]+) ms', block)
        # if not m:
        #     continue
        # max_header_delay = int(m.group(1))
        # Consensus TPS
        m = re.search(r'End-to-end TPS:\s*([0-9,]+)', block)
        if not m:
            continue
        tps = int(m.group(1).replace(',', ''))
        # Consensus latency
        m = re.search(r'End-to-end latency:\s*([0-9,]+) ms', block)
        if not m:
            continue
        latency = int(m.group(1).replace(',', ''))
        results.append({
            # 'max_header_delay': max_header_delay,
            'tps': tps,
            'latency': latency
        })
    return results

def compute_pareto_frontier(points):
    """
    给定一组点 (latency, tps)，返回 Pareto 最优子集。
    Pareto frontier = 没有其它点 latency 更低且 tps 更高
    """
    sorted_points = sorted(points, key=lambda x: (x[0], -x[1]))  # sort by latency asc, tps desc
    frontier = []
    max_tps = -1
    for latency, tps in sorted_points:
        if tps > max_tps:
            frontier.append((latency, tps))
            max_tps = tps
    return frontier

def plot_pareto(grouped, label):
    # 提取 (latency_mean, tps_mean) 对
    points = [(grouped[d]['latency_mean'], grouped[d]['tps_mean']) for d in grouped]
    frontier = compute_pareto_frontier(points)

    xs, ys = zip(*points)
    plt.scatter(xs, ys, label=label, alpha=0.6)

    # if frontier:
    #     f_xs, f_ys = zip(*frontier)
    #     plt.plot(f_xs, f_ys, linestyle='--', color='black', linewidth=1.5, label=f'{label} Pareto Frontier')

    # 可选：在点旁边标注 max_header_delay
    for d in grouped:
        plt.text(grouped[d]['latency_mean'], grouped[d]['tps_mean'], str(d), fontsize=8, color='gray')

def plot_compare(grouped1, grouped2, grouped3, grouped4, label1, label2, label3, label4):
    delays = sorted(set(grouped1.keys()) | set(grouped2.keys()) | set(grouped3.keys()) | set(grouped4.keys()))
    tps1 = [grouped1[d]['tps_mean'] if d in grouped1 else None for d in delays]
    tps2 = [grouped2[d]['tps_mean'] if d in grouped2 else None for d in delays]
    # tps3 = [grouped3[d]['tps_mean'] if d in grouped3 else None for d in delays]
    tps4 = [grouped4[d]['tps_mean'] if d in grouped4 else None for d in delays]
    lat1 = [grouped1[d]['latency_mean'] if d in grouped1 else None for d in delays]
    lat2 = [grouped2[d]['latency_mean'] if d in grouped2 else None for d in delays]
    # lat3 = [grouped3[d]['latency_mean'] if d in grouped3 else None for d in delays]
    lat4 = [grouped4[d]['latency_mean'] if d in grouped4 else None for d in delays]

    fig, ax1 = plt.subplots(figsize=(10,5))
    ax1.set_xlabel('Max header delay (ms)')
    ax1.set_ylabel('End-to-end TPS')
    ax1.plot(delays, tps1, 'o-', color='tab:red', label=f'TPS {label1}')
    ax1.plot(delays, tps2, 'o-', color='tab:blue', label=f'TPS {label2}')
    # ax1.plot(delays, tps3, 'o-', color='tab:green', label=f'TPS {label3}')
    ax1.plot(delays, tps4, 'o-', color='C4', label=f'TPS {label4}')
    ax1.legend(loc='upper left')
    ax1.grid(True)

    ax2 = ax1.twinx()
    ax2.set_ylabel('End-to-end Latency (ms)')
    ax2.plot(delays, lat1, 's--', color='tab:red', label=f'Latency {label1}')
    ax2.plot(delays, lat2, 's--', color='tab:blue', label=f'Latency {label2}')
    # ax2.plot(delays, lat3, 's--', color='tab:green', label=f'Latency {label3}')
    ax2.plot(delays, lat4, 's--', color='C4', label=f'Latency {label4}')
    ax2.legend(loc='upper right')

    plt.title('Performance under different Max header delay')
    plt.tight_layout()
    plt.savefig('results.png')
    

def plot_compare_raw(results_list, labels, title, filename):
    plt.figure(figsize=(10, 5))
    for results, label in zip(results_list, labels):
        xs = list(range(1, len(results) + 1))
        tps = [r['tps'] for r in results]
        latency = [r['latency'] for r in results]
        plt.plot(xs, tps, 'o-', label=f'TPS {label}')
        plt.plot(xs, latency, 's--', label=f'Latency {label}')
    plt.xlabel('Sample Index')
    plt.ylabel('Value')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_pareto_raw(results_list, labels, title, filename):
    plt.figure(figsize=(10, 6))
    for results, label in zip(results_list, labels):
        points = [(r['latency'], r['tps']) for r in results]
        if points:
            xs, ys = zip(*points)
            plt.scatter(xs, ys, label=label, alpha=0.6)
            for r in results:
                plt.text(r['latency'], r['tps'], str(r['max_header_delay']), fontsize=8, color='gray')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Throughput (TPS)')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_all_compare_mean(results_s1, results_s2, labels_s1, labels_s2, filename):
    plt.figure(figsize=(8, 6))
    x_s1 = np.arange(len(labels_s1))
    x_s2 = np.arange(len(labels_s2)) + len(labels_s1) + 1
    # S1
    tps_means_s1 = [np.mean([r['tps'] for r in results]) for results in results_s1]
    lat_means_s1 = [np.mean([r['latency'] for r in results]) for results in results_s1]
    # S2
    tps_means_s2 = [np.mean([r['tps'] for r in results]) for results in results_s2]
    lat_means_s2 = [np.mean([r['latency'] for r in results]) for results in results_s2]
    # 连线+点
    plt.plot(x_s1, tps_means_s1, '-o', color='tab:red', label='S1 TPS')
    plt.plot(x_s1, lat_means_s1, '-s', color='tab:orange', label='S1 Latency')
    plt.plot(x_s2, tps_means_s2, '-o', color='tab:blue', label='S2 TPS')
    plt.plot(x_s2, lat_means_s2, '-s', color='tab:green', label='S2 Latency')
    # 标注
    for i, label in enumerate(labels_s1):
        plt.text(x_s1[i], tps_means_s1[i], f'{label}', ha='center', va='bottom', fontsize=10, color='tab:red')
        plt.text(x_s1[i], lat_means_s1[i], f'{label}', ha='center', va='top', fontsize=10, color='tab:orange')
    for i, label in enumerate(labels_s2):
        plt.text(x_s2[i], tps_means_s2[i], f'{label}', ha='center', va='bottom', fontsize=10, color='tab:blue')
        plt.text(x_s2[i], lat_means_s2[i], f'{label}', ha='center', va='top', fontsize=10, color='tab:green')
    plt.xticks(list(x_s1) + list(x_s2), labels_s1 + labels_s2)
    plt.ylabel('Value')
    plt.title('Mean TPS & Latency')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_all_pareto_raw(results_s1, results_s2, labels_s1, labels_s2, filename):
    plt.figure(figsize=(10, 6))
    colors_s1 = ['tab:red', 'tab:orange', 'tab:pink']
    colors_s2 = ['tab:blue', 'tab:green', 'tab:purple']
    for results, label, color in zip(results_s1, labels_s1, colors_s1):
        points = [(r['latency'], r['tps']) for r in results]
        if points:
            xs, ys = zip(*points)
            plt.scatter(xs, ys, label=label, color=color, alpha=0.7)
            for r in results:
                plt.text(r['latency'], r['tps'], label, fontsize=8, color=color)
    for results, label, color in zip(results_s2, labels_s2, colors_s2):
        points = [(r['latency'], r['tps']) for r in results]
        if points:
            xs, ys = zip(*points)
            plt.scatter(xs, ys, label=label, color=color, alpha=0.7)
            for r in results:
                plt.text(r['latency'], r['tps'], label, fontsize=8, color=color)
    plt.xlabel('Latency (ms)')
    plt.ylabel('Throughput (TPS)')
    plt.title('S1/S2: Latency vs Throughput (Raw)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_mean_tps_latency(results_s1, results_s2, labels_s1, labels_s2, filename):
    plt.figure(figsize=(8, 6))
    # S1
    tps_means_s1 = [np.mean([r['tps'] for r in results]) for results in results_s1]
    lat_means_s1 = [np.mean([r['latency'] for r in results]) for results in results_s1]
    # S2
    tps_means_s2 = [np.mean([r['tps'] for r in results]) for results in results_s2]
    lat_means_s2 = [np.mean([r['latency'] for r in results]) for results in results_s2]
    # S1排序
    s1_points = sorted(zip(lat_means_s1, tps_means_s1, labels_s1), key=lambda x: x[0])
    s2_points = sorted(zip(lat_means_s2, tps_means_s2, labels_s2), key=lambda x: x[0])
    # 连线+点
    xs1, ys1, labs1 = zip(*s1_points)
    xs2, ys2, labs2 = zip(*s2_points)
    plt.plot(xs1, ys1, '-o', color='tab:red', label='S1')
    plt.plot(xs2, ys2, '-o', color='tab:blue', label='S2')
    # 标注
    for x, y, label in s1_points:
        plt.text(x, y, label, fontsize=10, color='tab:red', ha='right', va='bottom')
    for x, y, label in s2_points:
        plt.text(x, y, label, fontsize=10, color='tab:blue', ha='left', va='top')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Throughput (TPS)')
    plt.title('Mean Throughput vs Latency')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

if __name__ == '__main__':
    # 文件名
    file_s1_a1 = '/home/ccclr0302/autobahn-test/benchmark/S1_A1.txt'
    file_s1_a2 = '/home/ccclr0302/autobahn-test/benchmark/S1_A2.txt'
    file_s1_a3 = '/home/ccclr0302/autobahn-test/benchmark/S1_A3.txt'
    file_s2_a1 = '/home/ccclr0302/autobahn-test/benchmark/S2_A1.txt'
    file_s2_a2 = '/home/ccclr0302/autobahn-test/benchmark/S2_A2.txt'
    file_s2_a3 = '/home/ccclr0302/autobahn-test/benchmark/S2_A3.txt'

    # 解析
    results_s1_a1 = parse_bench_file(file_s1_a1)
    results_s1_a2 = parse_bench_file(file_s1_a2)
    results_s1_a3 = parse_bench_file(file_s1_a3)
    results_s2_a1 = parse_bench_file(file_s2_a1)
    results_s2_a2 = parse_bench_file(file_s2_a2)
    results_s2_a3 = parse_bench_file(file_s2_a3)

    # 均值对比图
    plot_all_compare_mean(
        [results_s1_a1, results_s1_a2, results_s1_a3],
        [results_s2_a1, results_s2_a2, results_s2_a3],
        ['S1_A1', 'S1_A2', 'S1_A3'],
        ['S2_A1', 'S2_A2', 'S2_A3'],
        'all_performance_mean.png'
    )
    # Throughput-Latency 均值连线图
    plot_mean_tps_latency(
        [results_s1_a1, results_s1_a2, results_s1_a3],
        [results_s2_a1, results_s2_a2, results_s2_a3],
        ['S1_A1', 'S1_A2', 'S1_A3'],
        ['S2_A1', 'S2_A2', 'S2_A3'],
        'all_mean_tps_latency.png'
    )
    # 其它画图函数可按需保留或注释