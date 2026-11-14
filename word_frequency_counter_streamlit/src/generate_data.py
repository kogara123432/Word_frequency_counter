"""Generate a synthetic text corpus of a chosen size for scalability tests.

Word frequencies follow Zipf's law, like real natural language.
Example: python src/generate_data.py --size-mb 200 --output data/big/corpus.txt
"""
import argparse
import os
import random

COMMON = ("the of and to in is that it for as was with be by on not he this are or his "
          "from at which but have an had they you were their one all we can her has there "
          "been if more when will would who so no data big hadoop map reduce cluster node "
          "analysis text word count system large scale process file block memory network "
          "computer science model result value key pair task job time search index").split()


def build_vocab(size, rng):
    vocab = list(COMMON)
    letters = "abcdefghijklmnopqrstuvwxyz"
    while len(vocab) < size:
        vocab.append("".join(rng.choice(letters) for _ in range(rng.randint(3, 9))))
    return vocab


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--size-mb", type=float, default=50)
    p.add_argument("--vocab", type=int, default=50000)
    p.add_argument("--output", default="data/big/corpus.txt")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    rng = random.Random(a.seed)
    vocab = build_vocab(a.vocab, rng)
    weights = [1.0 / (i + 1) for i in range(len(vocab))]  # Zipf distribution
    os.makedirs(os.path.dirname(a.output) or ".", exist_ok=True)

    target = int(a.size_mb * 1024 * 1024)
    written = 0
    with open(a.output, "w", encoding="utf-8") as f:
        while written < target:
            words = rng.choices(vocab, weights, k=100000)
            for i in range(0, len(words), 12):
                line = " ".join(words[i:i + 12]).capitalize() + ".\n"
                f.write(line)
                written += len(line)
    print(f"Wrote {written / 1024 / 1024:.1f} MB to {a.output}")


if __name__ == "__main__":
    main()
