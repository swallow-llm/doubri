#!/bin/bash
NS=($(seq 2 9))
for N in "${NS[@]}"; do
    ./run.sh $N
    ./run.sh $N reverse
done
