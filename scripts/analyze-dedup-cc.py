#!/usr/bin/env python

import sys
import os
import collections

target = sys.argv[1]
year = target[-4:]

D = []
begin = 0
with open(f'{target}/CC-MAIN-{year}.src') as fi:
    for line in fi:
        fields = line.strip('\n').split('\t')
        size = int(fields[0])
        src = fields[1]
        D.append(dict(begin=begin, size=size, src=src, flags=''))
        begin += size

with open(f'{target}/CC-MAIN-{year}.dup') as fi:
    for d in D:
        assert fi.tell() == d['begin']
        d['flags'] = fi.read(d['size'])
        assert len(d['flags']) == d['size']
    assert fi.readline() == ''

data = collections.defaultdict(lambda: dict(total=0, active=0))
for d in D:
    date = os.path.basename(d['src'])[8:16]
    data[date]['total'] += d['size']
    data[date]['active'] += sum(1 for c in d['flags'] if c == ' ')

for date, stat in data.items():
    print(f'{date} {stat["active"]} {stat["total"]} {stat["active"] / stat["total"]}')
