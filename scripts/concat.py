import gzip
import json
import os
import sys

def ccpath2name(ccpath):
    # See: "swallow-corpus-private/commoncrawl/generate-tasks.py"

    fields = ccpath.split('/')
    assert fields[0] == 'crawl-data'
    archive = fields[1]
    assert archive.startswith('CC-MAIN-2')
    assert fields[2] == 'segments'
    dirname = fields[3]
    assert fields[4] == 'warc'
    basename = fields[5]
    assert basename.endswith('.warc.gz')
    basename = basename.replace('.warc.gz', '')

    if basename.endswith('.internal'):
        # Old format.
        values = basename.split('-')
        assert values[0] == 'CC'
        assert values[1] == 'MAIN'
        assert len(values[2]) == 14
        assert values[2].isdigit()
        assert len(values[3]) == 5
        assert values[3].isdigit()
        assert values[4] == 'ip'
        assert values[5].isdigit()
        assert values[6].isdigit()
        assert values[7].isdigit()
        dst = '-'.join(values[:3] + [dirname,] + values[3:])
    else:
        # New format.
        dst = basename
    
    return dst

# Create a mapping from filename to path.
P = {}
with gzip.open(sys.argv[1], 'rt') as fi:
    for line in fi:
        ccpath = line.strip('\n')
        #print(ccpath, ccpath2name(ccpath))
        P[ccpath2name(ccpath) + '.warc.gz'] = ccpath

for line in sys.stdin:
    src = line.strip('\n')
    name = os.path.basename(src).replace('.jsonl.gz', '.warc.gz')
    path = P.get(name)
    if path is None:
        print(f'ERROR: Failed to retrieve a path from {name}', file=sys.stderr)
        sys.exit(1)

    with gzip.open(src, 'rt') as fi:
        for line in fi:
            d = dict(path=path)
            d |= json.loads(line)
            print(json.dumps(d, ensure_ascii=False))
