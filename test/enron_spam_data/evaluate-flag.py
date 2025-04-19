#!/usr/bin/env python

import argparse
import collections
import sys
import re
import math
import itertools
from sklearn.metrics import classification_report

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Evaluate the accuracy, precision, recall, f1-score of flags')
    parser.add_argument('-r', '--reverse', action='store_true', help='reverse mode')
    args = parser.parse_args()

    S = collections.defaultdict(list)
    with open('enron_spam_data-sim.txt') as fi:
        for line in fi:
            fields = line.strip('\n').split(' ')
            sim = math.floor(float(fields[0]) * 100) / 100.
            if args.reverse:
                S[sim].append((int(fields[2]), int(fields[1])))
            else:                
                S[sim].append((int(fields[1]), int(fields[2])))
    S = sorted(S.items(), key=lambda x: x[0], reverse=True)

    output = [1 if c == 'D' else 0 for c in sys.stdin.read()]
    reference = [0 for i in range(len(output))]

    R = {}
    for sim, pairs in S:
        for pair in pairs:
            reference[pair[1]] = 1

        result = classification_report(
            reference,
            output,
            output_dict=True)
        result['sim'] = sim
        R[sim] = result

    print(f'# reverse={args.reverse}')
    for sim, stat in R.items():
        print(f"{sim:.2f} {stat['accuracy']} {stat['1']['precision']} {stat['1']['recall']} {stat['1']['f1-score']}")

    """for pairs in S[0][1]:
        if output[pairs[1]] == 0:
            print(pairs)
"""