import sys
from benchmark.logs import LogParser

if __name__ == "__main__":
    logs_path = "./logs"
    faults = 0
    if len(sys.argv) > 1:
        logs_path = sys.argv[1]
    if len(sys.argv) > 2:
        faults = int(sys.argv[2])
    log_parser = LogParser.process(logs_path, faults=faults)
    print(log_parser.result())
