#!/bin/bash
PREFIX="NINJAL"

# Create destination directories.
find /s3/$PREFIX-* -maxdepth 1 -type d -name "$PREFIX-*" | sed "s:s3:data/minhash:g" | xargs mkdir -p

# Compute MinHash buckets.
find /s3/$PREFIX-* -name '*.jsonl.gz' | parallel --progress 'zcat {} | ./build/doubri-minhash -q {= s:s3:data/minhash:; s:.jsonl.gz:.mh: =}'

# Deduplicate.
mkdir -p /data/dedup/$PREFIX
find /data/minhash/$PREFIX-* -name '*.mh' | sort | ./build/doubri-dedup -r -l info -L info /data/dedup/$PREFIX/$PREFIX

# Upload the deduplication result to S3
aws s3 cp --recursive /data/dedup/$PREFIX s3://swallow-corpus-cc/dedup/$PREFIX/

