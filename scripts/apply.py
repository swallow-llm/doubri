"""
    Filter active items.

Copyright (c) 2023-2025, Naoaki Okazaki

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
import gzip
import json
import os
import sys
import re

def read_flag(fname):
    with open(fname) as fi:
        return fi.read()

def read_src(fname):
    with open(fname) as fi:
        S = []
        for line in fi:
            fields = line.strip('\n').split('\t')
            S.append((fields[1], int(fields[0])))
        return S

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Apply deduplication flags to filter out duplicate documents',
        )
    parser.add_argument('-s', '--src', type=str,
        help='specify a source (.src) file generated by doubri-dedup'
        )
    parser.add_argument('-d', '--dup', type=str,
        help='specify a duplication flag file (.dup) generated by doubri-dedup'
        )
    parser.add_argument('-r', '--replace', action='append', metavar='PATTERN:REPL', default=[],
        help='replace a source path using re.sub(PATTERN, REPL, path)'
        )
    parser.add_argument('-t', '--test', action='store_true',
        help='test the existance of source files'
        )

    args = parser.parse_args()

    # Read sources.
    S = read_src(args.src)
    num_total_items = sum(n for _, n in S)

    # Read flags.
    F = read_flag(args.dup)

    # Check the total number of items.
    if num_total_items != len(F):
        print(f'ERROR: Inconsistent number of items: {num_total_items} ({args.src}) != {len(F)} ({args.dup})')
        sys.exit(1)

    i = 0
    ret = 0
    for s, n in S:
        # Replace the source path by using regex patterns.
        for r in args.replace:
            pattern, repl = r.split(':')
            s = re.sub(pattern, repl, s)
        
        if args.test:
            # Test the existence of the source JSONL file.
            ok = os.path.exists(s)
            print(f'{"OK" if ok else "FAIL"} {s}', file=sys.stderr)
            if not ok:
                ret = 1
        else:
            # Extract non-duplicate documents.
            with gzip.open(s, 'rt') as fi:
                m = 0
                for line in fi:
                    if F[i] != 'D':
                        sys.stdout.write(line)
                    i += 1
                    m += 1
                if n != m:
                    print(f'ERROR: The source {s} is expected to have {n} lines but {m} lines actually.', file=sys.stderr)
                    os.exit(1)

    # Report an error if any.
    if ret != 0:
        print(f'Exit with the error code ({ret})', file=sys.stderr)

    # Exit.
    os.exit(ret)
