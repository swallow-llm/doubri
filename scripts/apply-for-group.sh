#!/bin/bash
YEAR=$1

# Create destination directories.
find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:s3:data/deduped:g" | xargs mkdir -p

# Compute MinHash buckets.
find /s3/CC-MAIN-$YEAR-* -name '*.jsonl.gz' | parallel --progress "zcat {} | ./build/doubri-apply -f /s3/dedup/$YEAR/CC-MAIN-$YEAR.dup -s /s3/dedup/$YEAR/CC-MAIN-$YEAR.src {= s:s3:data/minhash:; s:.jsonl.gz:.mh: =} | gzip -9c > {= s:s3:data/deduped: =}"
