#!/usr/bin/env python

import os
import sys

SRC = 's3://swallow-corpus-cc/dedup'
WORK = '/data/dedup'
R = 40

SRCS = [
    'NINJAL/NINJAL',
    '2013/CC-MAIN-2013',
    '2014/CC-MAIN-2014',
    '2015/CC-MAIN-2015',
    '2016/CC-MAIN-2016',
    '2017/CC-MAIN-2017',
    '2018/CC-MAIN-2018',
    '2019/CC-MAIN-2019',
    '2020/CC-MAIN-2020',
    '2021/CC-MAIN-2021',
    '2022/CC-MAIN-2022',
    '2023/CC-MAIN-2023',
    '2024/CC-MAIN-2024',
    '2025/CC-MAIN-2025',
    ]

def exec(cmd):
    print(cmd)
    os.system(cmd)

# Copy duplication flags to the local storage.
for src in SRCS:
    dst = f'{WORK}/{src}.dup'
    dstdir = os.path.dirname(dst)

    cmd = f'mkdir -p {dstdir}'
    exec(cmd)

    cmd = f'aws s3 cp {SRC}/{src}.dup {dst}'
    exec(cmd)

    cmd = f'cp {dst} {dst}.-----'
    exec(cmd)

# Merge indices for each bucket index
for bn in range(R):
    bstr = f'{bn:05d}'

    for src in SRCS:
        srcuri = f'{SRC}/{os.path.dirname(src)}/'
        srcbase = os.path.basename(src)
        dst = f'{WORK}/{src}.dup'
        dstdir = os.path.dirname(dst)
        cmd = f'aws s3 sync {srcuri} {dstdir}/ --exclude "*" --include "{srcbase}.idx.{bstr}.*" '
        exec(cmd)

    args = ' '.join([WORK + '/' + src for src in SRCS])
    cmd = f'./build/doubri-merge -s {bn} -e {bn+1} -o {WORK}/merge.{bstr} -l info -L info {args}'
    exec(cmd)

    for src in SRCS:
        dst = f'{WORK}/{src}.dup'
        cmd = f'cp {dst}.merge {dst}'
        exec(cmd)

        cmd = f'mv {dst}.merge {dst}.{bstr}'
        exec(cmd)

        srcbase = os.path.basename(src)
        dstdir = os.path.dirname(dst)
        cmd = f'rm {dstdir}/{srcbase}.idx.{bstr}.*'
        exec(cmd)
