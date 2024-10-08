# Doubri

## Introduction

Doubri is a C++ implementation of deduplication (e.g., removing similar documents).

+ Doubri does not require a special infrastructure for data parallelism (e.g., Spark) or a database. It can run on the standard server(s) with (many) CPU cores, (large) main memory, and a file system.
+ Doubri assumes that source documents are stored in the following format:
    + Each source document is encoded in a JSON object where the text is stored in the value of `"text"` key.
    + Source documents are represented in JSON Lines format (`.jsonl`), in other words, one JSON document per line.
+ Doubri converts a source file (`.jsonl`) to a MinHash file (`.hash`) where each document is represented by MinHash values. It then performs deduplication on MinHash files. Once MinHash files are created, Doubri does not have to access source documents during deduplication (i.e., you don't have to keep source documents in the storage).
+ Doubri maintains a 'flag' file (`.f`) to indicate whether a document in a source file is non-duplicate (`1`) or duplicate (`0`). Doubri initializes the flag of a document as `1` initially and changes it to `0` when the document is recognized as a duplicate.
+ After all deduplication processes are completed, we can extract non-duplicate documents by extracting the lines in the source file (`.jsonl`) such that the corresponding values of the flag file (`.f`) are `1`.
+ When the number of source documents is extremely large, it is impossible to store MinHash values of all documents on the main memory. Doubri assumes that the source documents are split into smaller groups such that deduplication on each group can be completed on the main memory.
    + `doubri-self` performs deduplication within a group and generates index files (`.index`) that stores sorted MinHash values of the documents.
    + Given index files of a group, `doubri-other` finds documents in other groups that are duplicates of the documents in the given group.
+ Doubri does not implement parallelization code for simplicity when the job is easily parallelized by mere data parallelism with an external command (e.g., `xargs`, GNU `parallel`). Doubri implements a multi-thread pool for recognizing duplicated documents in multiple groups (`doubri-other`), storing the index in the shared memory.
+ Doubri computes MinHash values on character n-grams *in Unicode* (not in UTF-8 byte sequence). This is a preferable treatment for computing text similarity in languages that use non-ASCII characters.

Origin of the name: in Japanese, we say *daburu* (ダブる) when something is a duplicate

## MinHash deduplication

### Theory

First, let us review the *Jaccard* coefficient (Jaccard, 1912) and *MinHash* (Broder, 1997).
The Jaccard coefficient is a similarity measure defined between two sets.
When using Jaccard coefficient as a similarity measure for text, we represent a text with a set of *features*.
The definition of features is arbitrary, but we use character $n$-grams in this explanation.
When two texts are represented by sets of features $X$ and $Y$, the Jaccard coefficient is given by,

$$
\mathrm{Jaccard}(X, Y) = \frac{|X \cap Y|}{|X \cup Y|} .
$$

The Jaccard coefficient ranges from 0 to 1. The more elements $X$ and $Y$ have in common, i.e., the more similar the two texts are in the feature space, the closer to $1$ it is. In contrast, when few elements are commonly included in $X$ and $Y$, the Jaccard coefficient approaches $0$.

Next, we explain MinHash, which approximately computes a Jaccard coefficient without explicitly finding $X \cup Y$ or $X \cap Y$.
First, we prepare multiple hash functions $H = \{h_1, \dots, h_{|H|}\}$ where each hash function randomly maps an $n$-gram to an integer value.
MinHash of a set $X$ on the hash function $h$ is defined as the minimum of hash values computed for all elements in $X$.

$$
\mathrm{MinHash}_h(X) = \min_{x \in X} h(x)
$$

Assuming that a hash function $h \in H$ randomly maps an $n$-gram to a hash value and that no collision of hash values occurs, we can find a Jaccard coefficient from the empirical probability that the MinHash values of $X$ and $Y$, computed by $|H|$ hash functions, match.

$$
  \mathrm{Jaccard}(X, Y) \approx \frac{1}{|H|}\sum_{h \in H}\mathbf{1}(\mathrm{MinHash}_h(X) = \mathrm{MinHash}_h(Y))
$$

Here, $\mathbf{1}(\mathrm{MinHash}_h(X) = \mathrm{MinHash}_h(Y))$ presents $1$ when $\mathrm{MinHash}_h(X) = \mathrm{MinHash}_h(Y)$ and $0$ otherwise.
The right-hand side of the above approximation gives the empirical probability of the matches of MinHash values.

Here is a brief explanation of why the above approximation holds. Calculating $\mathrm{MinHash}_h(X)$ and $\mathrm{MinHash}_h(Y)$ is equivalent to reordering the elements of $X$ and $Y$ by the criteria defined by the hash function $h$ and extracting the first element.
There are $|X \cup Y|$ candidate elements to be extracted, and if the MinHash values of $X$ and $Y$ match, then the element is extracted from $X \cap Y$. Because there are $|X \cap Y|$ elements that match the condition, the above approximation holds.

When using MinHash for deduplication, we often create $r$ buckets of $b$ concatenated MinHash values, compare the bucket pairs of the two documents $r$ times, and regard the two documents to as a duplicate if any of the bucket pairs exactly matches. If the Jackard coefficient between two documents is $s$, the probability that all $b$ pairs of hash values in both buckets match is $s^b$. Therefore, the probability that at least one bucket matches exactly when a pair of buckets is compared $r$ times is expressed as follows.

$$
1 - (1 - s^b)^r
$$

Lee et al. (2022) adopted a setting where $b=20, r=450$ and $|H|=br=9000$ hash values were used so that document pairs with a Jackard coefficient of $0.8$ could be detected with high probability.
However, this setting generates 36 kB of MinHash values per documents (when each MinHash value is represented in 32 bits), so approximately 36 TB of storage space is required to store the MinHash values for 1,000,000,000 pages.
Therefore, the current implementation uses a lenient setting where $b=20, r=40$ so that document pairs with a Jackard coefficient of $0.9$ could be detected with a probability of about 92.5 percent.

### Psuedo code

It may be easy for you to understand MinHash deduplication from Python codes. This section illustrates a simplified implemantation of MinHash deduplication in Python. First, we import necessary libraries. `mmh3` is a Python implementation of MurmurHash3.

```python
import mmh3
import json
import sys
```

This code reads JSON documents from STDIN, computes MinHash values to build buckets (roughly corresponding to the task of `doubri-minhash`), and initializes flags to `1` (corresponding to `doubri-init`).
Although the actual implementations writes buckets to a file (`.hash`) and flags to a file (`.f`), this code stores them in the `doc` object for simplicity.
In addition, the actual implementation represents a hash value in a byte sequence (DWORD) whereas this code represents one in a hex string.

```python
def ngram(text, n=5):
    return (text[i:i+n] for i in range(len(text)-n+1))

def minhash(text, seed):
    return min(mmh3.hash(x, seed) for x in ngram(text))

def build_bucket(text, i, b=20):
    return ''.join(hex(minhash(text, i*b+j)) for j in range(b))

r = 40
docs = []
for line in sys.stdin:
    doc = json.loads(line)
    doc['buckets'] = [build_bucket(doc['text'], i) for i in range(r)]
    doc['flag'] = '1'
```

This code explains the core algorithm of MinHash deduplication. If any bucket of the current document is included the index `I[i]`, this code recognizes the document as a duplicate and sets the flag to `0`. It then adds the buckets of the current documents to the indices `I`. The time complexity of this implementation is $O(n \log n)$ when `bucket in I[i]` is completed in $O(\log n)$ time ($n$ is the number of documents).

```python
I = [set() for i in range(r)]
for doc in docs:
    for i, bucket in enumerate(doc.buckets):
        if bucket in I[i]:
            doc.flag = '0'
            break
    for i, bucket in enumerate(doc.buckets):
        I[i].add(bucket)
```

```python
for doc in other_docs:
    for i, bucket in enumerate(doc.buckets):
        if bucket in I[i]:
            doc.flag = '0'
            break
```

## Implementation of doubri 

### Computing MinHash buckets from text

![bucket](./assets/bucket.svg)

For the text stored in `"text"` field of each JSON object from STDIN, `doubri-minhash` computes $r$ buckets of MinHash values, where each bucket consists of a concatenation of $b$ MinHash values.
Each MinHash value is 32 bits long and computed by [MurmurHash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) with a different seed.
This tool stores MinHash values in a binary file (`.hash`) that amounts to $4brN$ bytes (plus 24 bytes for the header).
Currently, the values of $b$ and $r$ are hard coded as $b=20, r=40$.


### Deduplication on a single `.jsonl` file

![self](./assets/self.svg)

This figure explains the process of deduplication when the MinHash values of all documents fit into the main memory.
Firstly, we compute MinHash values of all documents and store them into a file (`.hash`) by using `doubri-minhash` command. The figure illustrates an example where each document is represented by four buckets of MinHash values (we represent each bucket in 16 bits for simplicity).
For example, the text of the first document, "Madam, I'm Adam." is represented by four buckets of MinHash values, `6b7c`, `11d7`, `d2c1`, and `7414`.
We then use `double-init` command to initialize flags for all documents with `1` (non-duplicate).
In this example, the content of the flag file is `"1111"` (four consecutive `1`s) because the number of documents is four.

The command `doubri-self` performs deduplication for given hash and flag files. The deduplication algorithm is as simple as checking whether any bucket value was seen before for each document. If any bucket value of a document is seen before, it sets the corresponding flag to `0`; in this example, the flag for the third document is changed to `0` because the third document is similar (identical in this example) to the first one.
It also sorts values of each bucket and writes them to files as an index that will be used by the command `double-other`.

The command `doubri-apply` extracts non-duplicate documents in the source file. This is as simple as extracting JSON lines whose flags remain `1` after the command `doubri-self`.

### Deduplication on multiple `.jsonl` files

![other](./assets/other.svg)

When the number of source documents is extremely large, it may be impossible to store all buckets of source documents in the main memory.
You can roughly estimate the memory requirement by $\text{(size of buckets per document)} \times \text{(number of documents)}$.

In order to handle such a large number of documents, doubri uses the strategy to split the source documents into groups:

+ `doubri-self` deduplicates documents within each group.
+ `doubri-other` deduplicates documents that are similar to the ones in other groups by using the index generated by `doubri-self`.

The above figure illustrates deduplication with three document groups.
All documents in each group are converted to MinHash buckets by using `doubri-minhash` and initialized with the non-duplicate flag `1` by using `doubri-init`.
The figure first applies `doubri-self` to deduplicate documents in group #1, marks the third document as a duplicate, and stores non-duplicate MinHash buckets in the index.

In order to mark all duplicate documents in groups #2 and #3 that are similar to any document in group #1, `doubri-other` searches for buckets of each document in groups #2 and #3 that are identical to the ones in the index file from group #1.
In this example, the second document of group #2 and the second document of group #3 are marked as duplicates.
This also means that the number of non-duplicate documents is reduced from four to three on these groups.

We continue to deduplicate documents in group #2 by running `doubri-self`.
Here, we skip the second document, which has already been marked as a duplicate.
In this example, `doubri-self` recognizes the third document as a duplicate (of the first document) and yields the index with two non-duplicate documents.
`double-other` then marks the fourth document of group #3 as a duplicate by taking the index file.
We omit an explanation of the process of `double-self` on group #3 (but the flag file will be unchanged).
By extracting documents with the flag `1`, we complete the whole deduplication process.

## How to build

You can use `cmake` to compile the source code.

```
cmake -S . -B build
cmake --build build
```

This will build tools `doubri-init`, `doubri-apply`, `doubri-minhash`, `doubri-self`, `doubri-self` in `build` directory.

## How to use

### doubri-minhash

```
doubri-minhash MINHASH_FILE
```

This tool reads source documents in JSONL format from STDIN and stores MinHash buckets into `MINHASH_FILE`.

### doubri-init

```
doubri-init MINHASH_FILE
```

This tool reads MinHash buckets from `MINHASH_FILE` and output the character `1` $N$ times to STDOUT, where $N$ is the number of documents stored in `MINHASH_FILE`. Please redirect the output to create a flag file `.f`. The file name shuold be `MINHASH_FILE.f` (An extension `.f` appended to the MinHash file).

### doubri-self

```
doubri-self INDEX_FILE
```

This tool reads a group (list) of MinHash files from STDIN (one MinHash file per line), apply deduplication, and store an index file to `INDEX_FILE`. In other words, the input stream should be:

```
MINHASH_FILE-1
MINHASH_FILE-2
...
MINHASH_FILE-M
```

This tool assumes that the flag file for each MinHash file exists, i.e., `MINHASH_FILE-1.f`, `MINHASH_FILE-2.f`, ..., `MINHASH_FILE-M.f`, and updates the flag files for identified duplicates. The reason why this tools accepts a list of hash files is because we want to adjust the number of documents to fit them into the main memory.

This tool stores index files with the prefix `INDEX_FILE`, which will be used by `doubri-other`.

### doubri-other

```
doubri-other INDEX_FILE GROUP-1 GROUP-2 ... GROUP-K
```

This tool reads index files from the files with the prefix `INDEX_FILE`, find duplicate entries in groups (lists) of MinHash files specified by `GROUP-1`, `GROUP-2`, ..., `GROUP-K`. The file format of group files is the same to the one used in `doubri-self`, i.e., one MinHash file per line.

### doubri-other

```
doubri-apply FLAG_FILE
```

This tool reads source documents in JSONL format from STDIN and output non-duplicate documents to STDOUT, i.e., lines whose corresponding flags are `1`.

## Copyright and licensing information

This program is distributed under the MIT license. Refer to
[LICENSE](LICENSE) file for the precise description of the license.

This program uses these open-source implementations:

+ nlohmann/json: Copyright 2013-2022 Niels Lohmann (MIT license)
+ utf8: Copyright 2006 Nemanja Trifunovic (no license name found; probably Boost Software License 1.0?)
+ BS_thread_pool: Copyright (c) 2023 Barak Shoshany, licensed under the MIT license.

## Citing this software

Plain text:

```
Naoaki Okazaki, Kakeru Hattori, Hirai Shota, Hiroki Iida, Masanari Ohi, Kazuki Fujii, Taishi Nakamura, Mengsay Loem, Rio Yokota, and Sakae Mizuki. Building a Large Japanese Web Corpus for Large Language Models. In Proceedings of the First Conference on Language Modeling (COLM), October 2024.
```

Bibtex for the COLM paper:

```
@inproceedings{Okazaki:COLM2024,
  title={Building a Large Japanese Web Corpus for Large Language Models},
  author={Naoaki Okazaki and Kakeru Hattori and Hirai Shota and Hiroki Iida and Masanari Ohi and Kazuki Fujii and Taishi Nakamura and Mengsay Loem and Rio Yokota and Sakae Mizuki},
  booktitle="Proceedings of the First Conference on Language Modeling",
  series={COLM},
  pages="(to appear)",
  year="2024",
  month=oct,
  address={University of Pennsylvania, USA},
}
```

Bibtex for the Github page:

```
@misc{Okazaki:COLM2024,
  title={Doubri: C++ implementation of deduplication},
  author={Naoaki Okazaki},
  howpublished={\url{https://github.com/swallow-llm/doubri}},
  year="2024",
}
```
