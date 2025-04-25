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
mergekey = 'Merge: '

D = set()
M = set()
for line in sys.stdin:
    line = line.strip('\n')
    p = line.find(dupkey)
    if p != -1:
        values = line[p+len(dupkey):].split(' ')
        ids = [parse_id(v) for v in values]
        D |= set(ids[1:])

    p = line.find(mergekey)
    if p != -1:
        values = line[p+len(mergekey):].split(' ')
        ids = [parse_id(v) for v in values]
        assert len(ids) == 2
        M.add((ids[0], ids[1]))

S = collections.defaultdict(list)
with open('enron_spam_data-sim.txt') as fi:
    for line in fi:
        fields = line.strip('\n').split(' ')
        x, y = int(fields[1]), int(fields[2])
        if x not in D and y not in D:
            S[round(float(fields[0]), 2)].append((x, y))
S = sorted(S.items(), key=lambda x: x[0], reverse=True)

R = {}
for sim, pairs in S:
    n = 0
    m = 0
    for pair in pairs:
        n += 1
        if pair in M:
            m += 1
    R[sim] = dict(recall = m / n)

P = set()
for sim, pairs in S:
    m = 0
    P |= set(pairs)
    for pair in M:
        if pair in P:
            m += 1
    R[sim]['precision'] = m / len(D)

for sim, stat in R.items():
    print(f"{sim:.2f} {stat['recall']} {stat['precision']}")


for sim, pairs in S:
    for pair in pairs:
        if sim == 1.00 and pair not in M:
            print(pair)
#P = set()
#for sim, pairs in S:
#    P |= set(pairs)
#print(D - P)
