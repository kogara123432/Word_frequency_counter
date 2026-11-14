import os
import shutil
import sys
import tempfile
import unittest
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mapreduce import run_wordcount  # noqa: E402
from tokenizer import tokenize       # noqa: E402

TEXT = ("The quick brown fox jumps over the lazy dog.\n"
        "The dog barks; the fox runs away!\n"
        "Don't stop: big data, big ideas, BIG results.\n") * 200


class WordCountTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.file = os.path.join(self.dir, "t.txt")
        with open(self.file, "w", encoding="utf-8") as f:
            f.write(TEXT)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def expected(self, stop=frozenset()):
        c = Counter()
        for line in TEXT.splitlines():
            c.update(tokenize(line, stop))
        return c

    def test_matches_naive_counter(self):
        counts, _ = run_wordcount(self.file, workers=2, num_reducers=3)
        self.assertEqual(counts, self.expected())

    def test_many_tiny_splits_do_not_lose_or_duplicate_lines(self):
        # tiny chunks force splits to land in the middle of lines
        counts, stats = run_wordcount(self.file, workers=2, num_reducers=4, chunk_mb=0.0005)
        self.assertGreater(stats["splits"], 10)
        self.assertEqual(counts, self.expected())
        self.assertEqual(stats["lines"], 600)

    def test_stopwords(self):
        stop = frozenset({"the", "big"})
        counts, _ = run_wordcount(self.file, workers=1, stopwords=stop)
        self.assertNotIn("the", counts)
        self.assertEqual(counts, self.expected(stop))

    def test_case_and_punctuation(self):
        self.assertEqual(list(tokenize("Don't STOP, big-data!")), ["don't", "stop", "big", "data"])


if __name__ == "__main__":
    unittest.main()
