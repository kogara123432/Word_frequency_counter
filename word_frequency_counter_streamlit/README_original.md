# Large-Scale Word Frequency Counter

**Course:** Big Data Analytics  
**Student:** KOGARA MANMITHA  
**Roll No:** 24691832E1

A MapReduce-style program that counts how often every word occurs in very large text
collections and reports the Top-N most frequent words. It comes in three versions:

| Version | Where | Needs |
|---|---|---|
| **1. Local parallel MapReduce** (main project) | `src/` | Python 3.8+ only |
| **2. Hadoop Streaming** (real cluster) | `hadoop/` | Hadoop 3.x + Python 3 |
| **3. Apache Spark** (optional comparison) | `spark/` | PySpark |

## Project structure

```
word_frequency_counter/
├── README.md
├── requirements.txt
├── src/
│   ├── main.py            command line program (run this)
│   ├── mapreduce.py       split -> map -> shuffle -> reduce engine
│   ├── tokenizer.py       lowercasing, punctuation removal, stop words
│   └── generate_data.py   creates a big synthetic corpus for testing
├── hadoop/
│   ├── mapper.py          Hadoop Streaming mapper
│   ├── reducer.py         Hadoop Streaming reducer and combiner
│   ├── run_hadoop.sh      submit the job to a Hadoop cluster
│   └── run_local_streaming_test.sh   test mapper/reducer without Hadoop
├── spark/wordcount_spark.py
├── data/
│   ├── sample.txt         small demo text
│   └── stopwords.txt      common English stop words
├── tests/test_wordcount.py
└── results/               output files are written here
```

## How it works

1. **Split** – every input file is cut into byte ranges (like HDFS blocks).
   A range that starts in the middle of a line skips to the next line, so no line is
   counted twice or lost.
2. **Map** – each split is processed by a separate process. Words are lowercased,
   cleaned, optionally filtered, and counted locally (in-mapper combining).
3. **Shuffle** – each word goes to reducer `crc32(word) % reducers`, so every
   occurrence of the same word reaches the same reducer.
4. **Reduce** – reducers add up the partial counts. The final list is sorted by
   frequency and written to `results/word_counts.csv`.

---

# How to run

## Step 0 – Prerequisites

Check that Python 3.8 or newer is installed:

```bash
python3 --version        # Windows: python --version
```

Unzip the project and open a terminal **inside** the `word_frequency_counter` folder:

```bash
cd word_frequency_counter
```

(Optional) install matplotlib only if you want a bar chart:

```bash
pip install -r requirements.txt
```

## Step 1 – Run the demo (version 1)

```bash
python3 src/main.py -i data/sample.txt --stopwords data/stopwords.txt -n 10
```

You will see the statistics and the Top 10 words. The complete list is saved to
`results/word_counts.csv`. (On Windows use `python` instead of `python3`.)

## Step 2 – Run the unit tests

```bash
python3 -m unittest discover -s tests -v
```

All 4 tests should print `ok`.

## Step 3 – Test on a really large file

Generate a synthetic corpus (here 500 MB; use a smaller number if your disk is small):

```bash
python3 src/generate_data.py --size-mb 500 --output data/big/corpus.txt
```

Count the words using all CPU cores:

```bash
python3 src/main.py -i data/big --stopwords data/stopwords.txt -n 20 --plot
```

Compare the speed with different numbers of workers to study scalability:

```bash
python3 src/main.py -i data/big -w 1 -n 5
python3 src/main.py -i data/big -w 2 -n 5
python3 src/main.py -i data/big -w 4 -n 5
```

Note the "Time" line of each run and put the values in your report.

You can also use your own text: put `.txt` files in a folder (for example Project
Gutenberg books) and run `python3 src/main.py -i path/to/folder`.

### All options

| Option | Meaning | Default |
|---|---|---|
| `-i, --input` | file or folder of text files | required |
| `-o, --output` | output folder | `results` |
| `-n, --top` | number of top words to print | 20 |
| `-w, --workers` | parallel map processes | all CPU cores |
| `-r, --reducers` | number of reducer partitions | 4 |
| `--chunk-mb` | split size in MB | 16 |
| `--stopwords` | stop word file | none |
| `--min-len` | ignore shorter words | 2 |
| `--plot` | save `top_words.png` | off |

## Step 4 – Hadoop Streaming (version 2)

**4a. Without a cluster** (simulates map, sort, reduce on your computer – Linux/macOS/WSL/Git Bash):

```bash
bash hadoop/run_local_streaming_test.sh data/sample.txt
```

**4b. On a real Hadoop cluster** (Hadoop 3.x installed, `HADOOP_HOME` set):

```bash
start-dfs.sh
start-yarn.sh
bash hadoop/run_hadoop.sh data/sample.txt
```

The script uploads the file to HDFS, runs the streaming job with a combiner, and
prints the Top 20 words. Job progress is visible at `http://localhost:8088`.
Python 3 must be installed on every worker node.

## Step 5 – Spark (version 3, optional)

```bash
pip install pyspark
spark-submit spark/wordcount_spark.py data/sample.txt 20
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `python3: command not found` | On Windows use `python`; otherwise install Python 3 |
| `ModuleNotFoundError: matplotlib` | `pip install matplotlib` or run without `--plot` |
| `No text files found` | Input folder must contain `.txt` files (or pass a file directly) |
| Out of memory with a huge vocabulary | Lower `--workers`, or use a smaller `--chunk-mb` |
| `hadoop: command not found` | Install Hadoop and set `HADOOP_HOME` and `PATH` |
| `Output directory already exists` (Hadoop) | `hdfs dfs -rm -r /user/$USER/wordcount/output` |
| `Permission denied` on `.sh` files | `chmod +x hadoop/*.sh` or run with `bash` |

## Possible extensions

Bigram counting, stemming, TF-IDF on top of the counts, streaming input with Kafka.
