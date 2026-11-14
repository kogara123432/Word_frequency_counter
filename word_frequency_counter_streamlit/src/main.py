"""Large-Scale Word Frequency Counter - command line interface."""
import argparse
import csv
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mapreduce import run_wordcount          # noqa: E402
from tokenizer import load_stopwords         # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description="Large-Scale Word Frequency Counter")
    p.add_argument("-i", "--input", required=True, help="text file or directory of text files")
    p.add_argument("-o", "--output", default="results", help="output directory (default: results)")
    p.add_argument("-n", "--top", type=int, default=20, help="how many top words to show")
    p.add_argument("-w", "--workers", type=int, default=None, help="parallel mapper processes (default: all CPUs)")
    p.add_argument("-r", "--reducers", type=int, default=4, help="number of reducer partitions")
    p.add_argument("--chunk-mb", type=float, default=16, help="split size in MB")
    p.add_argument("--stopwords", default=None, help="path to stop word file (e.g. data/stopwords.txt)")
    p.add_argument("--min-len", type=int, default=2, help="ignore words shorter than this")
    p.add_argument("--plot", action="store_true", help="save a bar chart of the top words (needs matplotlib)")
    return p.parse_args()


def main():
    args = parse_args()
    stopwords = load_stopwords(args.stopwords)

    start = time.perf_counter()
    counts, stats = run_wordcount(
        args.input, workers=args.workers, num_reducers=args.reducers,
        chunk_mb=args.chunk_mb, stopwords=stopwords, min_len=args.min_len)
    elapsed = time.perf_counter() - start

    os.makedirs(args.output, exist_ok=True)
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

    with open(os.path.join(args.output, "word_counts.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "word", "count"])
        for rank, (word, n) in enumerate(ranked, 1):
            w.writerow([rank, word, n])

    print("\n=== Large-Scale Word Frequency Counter ===")
    print(f"Files: {stats['files']}   Splits: {stats['splits']}   Lines: {stats['lines']:,}")
    print(f"Total words: {stats['total_words']:,}   Unique words: {stats['unique_words']:,}")
    print(f"Workers: {stats['workers']}   Reducers: {stats['reducers']}   Time: {elapsed:.2f}s")
    print(f"\nTop {args.top} words:")
    print(f"{'Rank':<6}{'Word':<20}{'Count':>12}")
    print("-" * 38)
    for rank, (word, n) in enumerate(ranked[:args.top], 1):
        print(f"{rank:<6}{word:<20}{n:>12,}")
    print(f"\nFull results saved to {os.path.join(args.output, 'word_counts.csv')}")

    if args.plot:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            print("matplotlib is not installed; run: pip install matplotlib")
            return
        top = ranked[:args.top]
        plt.figure(figsize=(9, 6))
        plt.barh([w for w, _ in reversed(top)], [n for _, n in reversed(top)])
        plt.title(f"Top {args.top} most frequent words")
        plt.xlabel("Frequency")
        plt.tight_layout()
        path = os.path.join(args.output, "top_words.png")
        plt.savefig(path, dpi=150)
        print(f"Chart saved to {path}")


if __name__ == "__main__":
    main()
