"""Text cleaning and tokenization shared by every implementation."""
import re

WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)?")


def load_stopwords(path):
    """Read one stop word per line. Returns an empty set if path is None."""
    if not path:
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


def tokenize(line, stopwords=frozenset(), min_len=2):
    """Lowercase a line and yield cleaned words."""
    for word in WORD_RE.findall(line.lower()):
        if len(word) >= min_len and word not in stopwords:
            yield word
