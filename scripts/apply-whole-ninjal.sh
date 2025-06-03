#!/bin/bash

mkdir -p /data/deduped/NINJAL
cat /s3/dedup-fwd-NINJAL-CC/NINJAL/NINJAL.src | cut -f2 | sed 's/.mh/.jsonl.gz/g' | sed 's:/data/minhash:/s3:g' | xargs zcat | ./build/doubri-apply-whole -f /s3/dedup-fwd-NINJAL-CC/NINJAL/NINJAL.dup | split -l 1000000 -d -a5 --filter 'gzip > $FILE.json.gz' - /data/deduped/NINJAL/NINJAL.
