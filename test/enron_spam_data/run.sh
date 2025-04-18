#!/bin/bash

N=$1
WORK=/data/test/enron_spam/$N
DIRS=($(seq -f "%02g" 0 $((N-1))))

# Create working directories.
#rm -rf $WORK/data $WORK/minhash $WORK/dedup
mkdir -p $WORK/data $WORK/minhash $WORK/dedup

# Build data files.
./build-dataset.py $N $WORK/data

# Compute the similarity of all pairs of text exactly.
#find $WORK/data/ -name '*.jsonl' | sort | xargs cat | ../../build/doubri-similarity > enron_spam_data-sim.txt

# Generate MinHash values.
for DIR in "${DIRS[@]}"; do
    mkdir -p $WORK/minhash/$DIR
    find $WORK/data/$DIR -name '*.jsonl' | parallel --progress "cat {} | ../../build/doubri-minhash -q $WORK/minhash/$DIR/{/.}.mh"
done

# Deduplicate all text at once.
mkdir -p $WORK/dedup/all
find $WORK/minhash/ -name '*.mh' | sort | ../../build/doubri-dedup -g 0 -l info -L info $WORK/dedup/all/dedup
cat $WORK/dedup/all/dedup.dup | ./evaluate-flag.py enron_spam_data-sim.txt > result-all-flag.txt

# Deduplicate text for each group.
for DIR in "${DIRS[@]}"; do
    mkdir -p $WORK/dedup/$DIR
    find $WORK/minhash/$DIR -name '*.mh' | sort | ../../build/doubri-dedup -g $DIR -l info -L info $WORK/dedup/$DIR/dedup
done

# Merge indices
ARGS=()
for DIR in "${DIRS[@]}"; do
    ARGS+=($WORK/dedup/$DIR/dedup)
done
../../build/doubri-merge -o $WORK/dedup/merge -l info -L info ${ARGS[*]}
find $WORK/dedup/ -name 'dedup.dup.merge' | sort | xargs cat | ./evaluate-flag.py enron_spam_data-sim.txt > result-merge-flag-$N.txt
