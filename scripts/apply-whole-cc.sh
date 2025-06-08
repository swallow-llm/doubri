#!/bin/bash
YEAR=$1

mkdir -p /data/deduped/$YEAR
cat /s3/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.src | cut -f2 | sed 's/.mh/.jsonl.gz/g' | sed 's:/data/minhash:/s3:g' | xargs zcat | ./build/doubri-apply-whole -f /s3/dedup-fwd-NINJAL-CC/$YEAR/CC-MAIN-$YEAR.dup | split -l 500000 -d -a5 --filter 'gzip > $FILE.json.gz' - /data/deduped/$YEAR/CC-MAIN-$YEAR.
