import sys
import glob
import re
import json

total_num_items = 0
total_num_active_after = 0
total_time = 0

prefix = sys.argv[1]

srcs = glob.glob(f'{prefix}/*/CC-MAIN-*.log')
for src in srcs:
    m = re.match(f'{prefix}/([^/]+)/', src)
    year = m.group(1)
    with open(src) as fi:
        for line in fi:
            p = line.find('Result: {')
            if p != -1:
                result = json.loads(line[p+8:])
                print(f'| {year} | {result["num_items"]:,} | {result["num_active_after"]:,} | {result["active_ratio_after"]} | {result["time"] / 3600:.3f} |')
                total_num_items += result['num_items']
                total_num_active_after += result['num_active_after']
                total_time += result['time']

print(f'| TOTAL | {total_num_items:,} | {total_num_active_after:,} | {total_num_active_after / total_num_items:.5f} | {total_time / 3600:.3f} |')
