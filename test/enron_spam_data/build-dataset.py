import pandas as pd
import json
import os

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
for i, d in enumerate(D):
    g = int(i / (len(D) / G))
    datasets[g].append(d)

for g, dataset in enumerate(datasets):
    for i in range(len(dataset) // S):
        os.makedirs(f'data/{g:02d}', exist_ok=True)
        with open(f'data/{g:02d}/{g:02d}-{i:05d}.jsonl', 'w') as fo:
            for d in dataset[i*S:min((i+1)*S, len(dataset))]:
                print(json.dumps(d), file=fo)
