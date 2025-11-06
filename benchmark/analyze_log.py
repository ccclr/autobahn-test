import re
import numpy as np

log_path = "/home/ccclr0302/autobahn-test/benchmark/logs/primary-0.log"

lane_growth = {}
fast_path_count = 0
slow_path_count = 0
commit_success_count = 0
commit_total = 0

with open(log_path, "r") as f:
    for line in f:
        # 统计lane增长
        lane_match = re.search(r"Lane (\d+): block added", line)
        if lane_match:
            lane_id = int(lane_match.group(1))
            lane_growth[lane_id] = lane_growth.get(lane_id, 0) + 1

        # 统计共识路径
        if "Consensus: fast path" in line:
            fast_path_count += 1
        elif "Consensus: slow path" in line:
            slow_path_count += 1

        # 统计提案提交成功率
        if "Proposal committed:" in line:
            commit_total += 1
            if "success" in line:
                commit_success_count += 1

# Layer 2 Part I: DAG增长
growth_rates = list(lane_growth.values())
avg_growth = np.mean(growth_rates) if growth_rates else 0
variance_growth = np.var(growth_rates) if growth_rates else 0

# Layer 2 Part II: 共识
path_ratio = fast_path_count / (fast_path_count + slow_path_count) if (fast_path_count + slow_path_count) > 0 else 0
commit_success_rate = commit_success_count / commit_total if commit_total > 0 else 0

print("平均lane增长率:", avg_growth)
print("lane增长方差:", variance_growth)
print("快慢路径比例:", path_ratio)
print("提案提交成功率:", commit_success_rate)