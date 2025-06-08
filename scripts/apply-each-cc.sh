#!/bin/bash
YEAR=$1

echo "[ $1 ]"

# Create destination directories.
find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:s3:data/raw:g" | xargs mkdir -p
find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | parallel -j1 aws s3 sync s3://swallow-corpus-cc/{/}/ /data/raw/{/}/

mkdir -p /data/dedup-fwd-NINJAL-CC/$YEAR
aws s3 cp s3://swallow-corpus-cc/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.src /data/dedup-fwd-NINJAL-CC/$YEAR/
aws s3 cp s3://swallow-corpus-cc/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.dup /data/dedup-fwd-NINJAL-CC/$YEAR/

find /data/raw/ -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:/raw:/deduped:g" | xargs mkdir -p
find /data/raw/CC-MAIN-$YEAR-* -name '*.jsonl.gz' | parallel --progress "zcat {} | ./build/doubri-apply-each -f /data/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.dup -s /data/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.src {= s:data/raw:data/minhash:; s:.jsonl.gz:.mh: =} | gzip -9c > {= s:/raw:/deduped: =}"

find /s3 -maxdepth 1 -type d -name "CC-MAIN-$YEAR-*" | sed "s:s3:data/raw:g" | xargs rm -rf
