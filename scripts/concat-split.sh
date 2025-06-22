#!/bin/bash

for D; do
    echo "Target: $D"
    mkdir -p /data/concat/$D
    find /data/deduped/$D/ -name *.jsonl.gz | sort |  python concat.py ~/swallow-corpus-private/commoncrawl/paths/$D.warc.paths.gz | split -a 5 -d -l 500000 --filter 'gzip -9c > $FILE.jsonl.gz' - /data/concat/$D/$D.
    retval=$?
    if [ $retval -ne 0 ]; then
        echo "FAILED"
        exit 1
    fi
done
