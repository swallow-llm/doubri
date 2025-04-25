#!/usr/bin/env python

import collections
import sys
import re
import math
import itertools

def parse_id(s):
    values = s.split(':')
    return int(values[0]) * 8429 + int(values[1])

dupkey = 'Duplicate(s): '

D = set()
for line in sys.stdin:
    line = line.strip('\n')
    p = line.find(dupkey)
    if p != -1:
        values = line[p+len(dupkey):].split(' ')
        ids = [parse_id(v) for v in values]
        for x, y in itertools.combinations(ids, 2):
            D.add((x, y))

S = collections.defaultdict(list)
with open('enron_spam_data-sim.txt') as fi:
    for line in fi:
        fields = line.strip('\n').split(' ')
        S[round(float(fields[0]), 2)].append((int(fields[1]), int(fields[2])))
S = sorted(S.items(), key=lambda x: x[0], reverse=True)

R = {}
for sim, pairs in S:
    n = 0
    m = 0
    for pair in pairs:
        n += 1
        if pair in D:
            m += 1
    R[sim] = dict(recall = m / n)

P = set()
for sim, pairs in S:
    m = 0
    P |= set(pairs)
    for pair in D:
        if pair in P:
            m += 1
    R[sim]['precision'] = m / len(D)

for sim, stat in R.items():
    print(f"{sim:.2f} {stat['recall']} {stat['precision']}")

#P = set()
#for sim, pairs in S:
#    P |= set(pairs)
#print(D - P)
