"""A small MapReduce engine for word counting on one machine.

It mimics the Hadoop pipeline so the same ideas can be demonstrated
without installing a cluster:

  1. SPLIT   - every file is cut into byte-range chunks (like HDFS blocks)
  2. MAP     - each chunk is tokenized and counted in parallel; the counts
               are partitioned by hash(word) % num_reducers
               (in-mapper combining: the same job a Hadoop combiner does)
  3. SHUFFLE - partition i of every mapper is routed to reducer i
  4. REDUCE  - every reducer merges its partitions into final totals
"""
import os
import zlib
from collections import Counter
from multiprocessing import Pool

from tokenizer import tokenize


def list_input_files(path):
    """Return all text files below `path` (a file or a directory)."""
    if os.path.isfile(path):
        return [path]
    files = []
    for root, _, names in os.walk(path):
        for name in sorted(names):
            if name.endswith((".txt", ".text", ".log", ".csv", ".md")):
                files.append(os.path.join(root, name))
    return files


def make_splits(files, chunk_size):
    """Create (path, start, end) byte ranges, like input splits in Hadoop."""
    splits = []
    for path in files:
        size = os.path.getsize(path)
        if size == 0:
            continue
        for start in range(0, size, chunk_size):
            splits.append((path, start, min(start + chunk_size, size)))
    return splits


def partition_of(word, num_reducers):
    # crc32 is stable across processes (Python's hash() is randomized)
    return zlib.crc32(word.encode("utf-8")) % num_reducers


def map_task(args):
    """Count the words of one split and partition the result."""
    (path, start, end), num_reducers, stopwords, min_len = args
    counts = Counter()
    lines = 0
    with open(path, "rb") as f:
        if start > 0:
            f.seek(start - 1)
            f.readline()  # skip the line that belongs to the previous split
        while f.tell() < end:
            raw = f.readline()
            if not raw:
                break
            lines += 1
            counts.update(tokenize(raw.decode("utf-8", "ignore"), stopwords, min_len))
    partitions = [dict() for _ in range(num_reducers)]
    for word, n in counts.items():
        partitions[partition_of(word, num_reducers)][word] = n
    return partitions, lines


def reduce_task(partition_list):
    """Merge the partial counts that belong to one reducer."""
    total = Counter()
    for part in partition_list:
        total.update(part)
    return total


def run_wordcount(input_path, workers=None, num_reducers=4, chunk_mb=16,
                  stopwords=frozenset(), min_len=2):
    """Run the full pipeline. Returns (Counter, stats dict)."""
    workers = workers or os.cpu_count() or 1
    files = list_input_files(input_path)
    if not files:
        raise FileNotFoundError(f"No text files found in: {input_path}")
    splits = make_splits(files, max(1, int(chunk_mb * 1024 * 1024)))
    jobs = [(s, num_reducers, stopwords, min_len) for s in splits]

    with Pool(workers) as pool:
        mapped = pool.map(map_task, jobs, chunksize=1)                       # MAP
        shuffled = [[m[0][r] for m in mapped] for r in range(num_reducers)]  # SHUFFLE
        reduced = pool.map(reduce_task, shuffled)                            # REDUCE

    final = Counter()
    for r in reduced:
        final.update(r)  # reducers hold disjoint keys, so this is a cheap union

    stats = {
        "files": len(files),
        "splits": len(splits),
        "lines": sum(m[1] for m in mapped),
        "total_words": sum(final.values()),
        "unique_words": len(final),
        "workers": workers,
        "reducers": num_reducers,
    }
    return final, stats
