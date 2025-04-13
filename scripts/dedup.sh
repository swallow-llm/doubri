#!/bin/bash
YEAR=$1

# Create destination directories.
find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:s3:data/minhash:g" | xargs mkdir -p

# Compute MinHash buckets.
find /s3/CC-MAIN-$YEAR-* -name '*.jsonl.gz' | parallel --progress 'zcat {} | ./build/doubri-minhash -q {= s:s3:data/minhash:; s:.jsonl.gz:.mh: =}'

# Deduplicate.
mkdir -p /data/dedup
find /data/minhash/CC-MAIN-$YEAR-* -name '*.mh' | sort | ./build/doubri-dedup -g $YEAR -l info -L info /data/dedup/CC-MAIN-$YEAR

# Upload the deduplication result to S3
mkdir -p /s3/dedup
cp -r /data/dedup/CC-MAIN-$YEAR /s3/dedup/
