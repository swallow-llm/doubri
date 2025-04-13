#!/bin/bash
YEAR=$1

# Create destination directories.
find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:s3:data/minhash:g" | xargs mkdir -p

# Compute MinHash buckets.
find /s3/CC-MAIN-$YEAR-* -name '*.jsonl.gz' | parallel --progress 'zcat {} | ./build/doubri-minhash -q {= s:s3:data/minhash:; s:.jsonl.gz:.mh: =}'

# Deduplicate.
mkdir -p /data/dedup/$YEAR
find /data/minhash/CC-MAIN-$YEAR-* -name '*.mh' | sort | ./build/doubri-dedup -g $YEAR -l info -L info /data/dedup/$YEAR/CC-MAIN-$YEAR

# Upload the deduplication result to S3
aws s3 cp --recursive /data/dedup/$YEAR s3://swallow-corpus-cc/dedup/$YEAR/
