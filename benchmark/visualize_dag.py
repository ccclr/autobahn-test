import json
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

# 读取 committee，建立 author->validator 的映射
with open('/home/ccclr0302/autobahn-test/benchmark/.committee.json', 'r') as f:
    committee = json.load(f)

# 取出所有 authorities 的 key（完整 base64 公钥）
authority_keys = list(committee['authorities'].keys())

# 读取 DAG 快照
with open('/home/ccclr0302/dag_snapshot_slot21_view1.json', 'r') as f:
    data = json.load(f)

nodes = data['nodes']

# 建立 author（短名）到 validator（完整key）的映射
author_map = {}
for short in set(n['author'] for n in nodes):
    for full in authority_keys:
        if short in full:
            author_map[short] = full
            break
    else:
        author_map[short] = short  # fallback

# 统计所有 validator（按 committee 顺序）
validators = [k for k in authority_keys]
validator_pos = {v: i for i, v in enumerate(validators)}

# 统计所有 round（height）
rounds = sorted(set(n['height'] for n in nodes))
round_pos = {r: i for i, r in enumerate(rounds)}

G = nx.DiGraph()
for node in nodes:
    digest = node['digest']
    author = node['author']
    height = node['height']
    validator = author_map.get(author, author)
    # 确保每个节点都带属性
    G.add_node(digest, validator=validator, round=height)
    for parent in node['parents']:
        G.add_edge(digest, parent)

# 计算节点坐标：x=round序号, y=validator序号
pos = {}
for node, attr in G.nodes(data=True):
    v = attr['validator']
    r = attr['round']
    x = round_pos[r]
    y = -validator_pos.get(v, 0)
    pos[node] = (x, y)

plt.figure(figsize=(max(8, len(rounds)), max(6, len(validators))))

# 画节点（圆点）
nx.draw_networkx_nodes(G, pos, node_size=300, node_color='orange', edgecolors='black', linewidths=1)

# 画有箭头的边
nx.draw_networkx_edges(
    G, pos,
    arrows=True,
    arrowstyle='-|>',
    arrowsize=18,
    edge_color='gray',
    width=1.5,
    connectionstyle='arc3,rad=0.08',  # 让箭头更明显
    min_source_margin=10,
    min_target_margin=10,
    style='-'
)

# 可选：画节点label
# nx.draw_networkx_labels(G, pos, font_size=8)

# 标注 round（高度）
for r, x in round_pos.items():
    plt.text(x, 1, f"r = {r}", ha='center', va='bottom', fontsize=10)
# 标注 validator
for v, y in validator_pos.items():
    plt.text(-1, -y, v[:8], ha='right', va='center', fontsize=10)

plt.axis('off')
plt.title(f"DAG Snapshot (slot {data['committed_slot']}, view {data['view']})")
plt.tight_layout()
plt.savefig('dag.png')