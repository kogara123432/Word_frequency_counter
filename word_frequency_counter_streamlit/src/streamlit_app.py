"""Streamlit interface for the Large-Scale Word Frequency Counter."""
from pathlib import Path
import io
import os
import tempfile
import time

import pandas as pd
import streamlit as st

from mapreduce import run_wordcount
from tokenizer import load_stopwords

st.set_page_config(page_title="Large-Scale Word Frequency Counter",
                   page_icon="📊", layout="wide")

st.title("📊 Large-Scale Word Frequency Counter")
st.write("Upload text files and count word frequencies using the project's "
         "Split → Map → Shuffle → Reduce pipeline.")

with st.sidebar:
    st.header("Processing settings")
    top_n = st.slider("Top words to display", 5, 50, 10)
    workers = st.number_input("Mapper workers", min_value=1,
                              max_value=max(1, os.cpu_count() or 1),
                              value=min(4, os.cpu_count() or 1), step=1)
    reducers = st.number_input("Number of reducers", min_value=1,
                               max_value=16, value=4, step=1)
    chunk_mb = st.number_input("Chunk size (MB)", min_value=0.01,
                               max_value=128.0, value=16.0, step=1.0)
    min_len = st.number_input("Minimum word length", min_value=1,
                              max_value=20, value=2, step=1)
    remove_stopwords = st.checkbox("Remove common stop words", value=True)

uploaded_files = st.file_uploader(
    "Upload one or more .txt, .text, .log, .csv, or .md files",
    type=["txt", "text", "log", "csv", "md"],
    accept_multiple_files=True
)

sample_choice = st.checkbox("Use the project's sample.txt instead", value=False)
run_button = st.button("🚀 Count word frequencies", type="primary",
                       use_container_width=True)

if run_button:
    if not uploaded_files and not sample_choice:
        st.warning("Please upload at least one text file or select the sample file.")
        st.stop()

    with tempfile.TemporaryDirectory() as temp_dir:
        input_dir = Path(temp_dir) / "input"
        input_dir.mkdir()

        if sample_choice:
            sample_path = Path(__file__).resolve().parent.parent / "data" / "sample.txt"
            shutil_copy = input_dir / "sample.txt"
            shutil_copy.write_bytes(sample_path.read_bytes())

        for index, uploaded in enumerate(uploaded_files or []):
            safe_name = Path(uploaded.name).name
            # Avoid overwriting when two uploaded files have the same name.
            target = input_dir / f"{index}_{safe_name}"
            target.write_bytes(uploaded.getvalue())

        stopwords = set()
        if remove_stopwords:
            stop_path = Path(__file__).resolve().parent.parent / "data" / "stopwords.txt"
            if stop_path.exists():
                stopwords = load_stopwords(str(stop_path))

        try:
            started = time.perf_counter()
            with st.spinner("Running Split → Map → Shuffle → Reduce..."):
                counts, stats = run_wordcount(
                    str(input_dir),
                    workers=int(workers),
                    num_reducers=int(reducers),
                    chunk_mb=float(chunk_mb),
                    stopwords=stopwords,
                    min_len=int(min_len),
                )
            elapsed = time.perf_counter() - started
        except Exception as exc:
            st.error(f"Processing failed: {exc}")
            st.stop()

    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    result_df = pd.DataFrame(
        [{"Rank": rank, "Word": word, "Count": count}
         for rank, (word, count) in enumerate(ranked, start=1)]
    )

    st.success("Word frequency analysis completed!")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Files", stats["files"])
    c2.metric("Total words", f'{stats["total_words"]:,}')
    c3.metric("Unique words", f'{stats["unique_words"]:,}')
    c4.metric("Execution time", f"{elapsed:.2f} sec")

    st.subheader("MapReduce execution details")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Input splits", stats["splits"])
    d2.metric("Mapper workers", stats["workers"])
    d3.metric("Reducers", stats["reducers"])
    d4.metric("Lines processed", f'{stats["lines"]:,}')

    st.subheader(f"Top {min(int(top_n), len(result_df))} frequent words")
    top_df = result_df.head(int(top_n))
    if not top_df.empty:
        st.bar_chart(top_df.set_index("Word")["Count"])
    st.dataframe(top_df, use_container_width=True, hide_index=True)

    csv_bytes = result_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download complete results (CSV)",
        data=csv_bytes,
        file_name="word_counts.csv",
        mime="text/csv",
        use_container_width=True,
    )

    with st.expander("View all word counts"):
        st.dataframe(result_df, use_container_width=True, hide_index=True)
else:
    st.info("Choose files or the sample text, adjust settings if needed, then click "
            "**Count word frequencies**.")
