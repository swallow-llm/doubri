import pandas as pd
import json
import os
import math

G = 4
S = 100

df = pd.read_csv('enron_spam_data.csv')
df.fillna('')

D = []
for index, row in df.iterrows():
    D.append(dict(
        id=str(len(D)),
        text=row['Message'] if not pd.isna(row['Message']) else '',
    ))

datasets = [[] for g in range(G)]
m = math.ceil(len(D) / G)
for i in range(0, len(D), m):
    g = i // m
    datasets[g] = D[i:min(i+m, len(D))]
    print(i, i+m, len(datasets[g]))

for g, dataset in enumerate(datasets):
    for i in range(0, len(dataset), S):
        os.makedirs(f'data/{g:02d}', exist_ok=True)
        with open(f'data/{g:02d}/{g:02d}-{i//S:05d}.jsonl', 'w') as fo:
            print(i, i+S)
            for d in dataset[i:min(i+S, len(dataset))]:
                print(json.dumps(d), file=fo)
