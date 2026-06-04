"""
FinBot Vector Ingestion Pipeline
=================================
Ingests books (PDF), newspaper CSVs, and Wikipedia articles into ChromaDB with separate collections per source type.

Collections:
  - finbot_books
  - finbot_newspapers
  - finbot_wikipedia

Run:
  python vectorDB.py --source all
  python vectorDB.py --source books
  python vectorDB.py --source newspapers
  python vectorDB.py --source wikipedia
"""

import os
import time
import argparse
from pathlib import Path
import pandas as pd
from langchain_community.document_loaders import PDFPlumberLoader
from langchain_text_splitters import TokenTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document

# ── Config ────────────────────────────────────────────────────────────────────

BOOKS_DIR      = "./data/books"        
NEWSPAPERS_DIR = "./data/cleaned_articles"    
WIKIPEDIA_DIR  = "./data/cleaned_wikipedia"  

CHROMA_PERSIST_DIR = "./chroma_db"

COLLECTIONS = {
    "books":      "finbot_books",
    "newspapers": "finbot_newspapers",
    "wikipedia":  "finbot_wikipedia",
}

# Chunking
CHUNK_SIZE   = 400
OVERLAP_SIZE = 80
BATCH_SIZE   = 50

# Minimum word count to skip very short/noisy articles
MIN_WORDS = 100

# ── Embeddings ────────────────────────────────────────────────────────────────

print("Loading BGE embeddings model (one-time)...")
embeddings = HuggingFaceEmbeddings(
    model_name="BAAI/bge-large-en-v1.5",
    model_kwargs={"device": "cpu"},
    encode_kwargs={"normalize_embeddings": True},
)
print("✅ Embeddings model ready.\n")

text_splitter = TokenTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=OVERLAP_SIZE)

# ── Loaders ───────────────────────────────────────────────────────────────────

def load_books(directory: str) -> list[Document]:
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"  ⚠️  Directory not found: {directory} — skipping.")
        return []

    files = list(dir_path.rglob("*.pdf"))
    if not files:
        print(f"  ⚠️  No PDF files found in {directory} — skipping.")
        return []

    print(f"  Found {len(files)} PDF files.")
    all_docs = []
    for f in files:
        print(f"    Loading: {f.name}...")
        try:
            loader = PDFPlumberLoader(str(f), extract_images=False)
            docs = loader.load()
            for doc in docs:
                doc.metadata["source_type"] = "book"
                doc.metadata["filename"]    = f.name
            all_docs.extend(docs)
        except Exception as e:
            print(f"    ❌ Failed: {e}")
    return all_docs


def load_newspapers(directory: str) -> list[Document]:
    """
    Loads newspaper CSVs with columns: Year, Month, Day, Article_Text,
    word_count, date. Each row becomes one Document with full metadata.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"  ⚠️  Directory not found: {directory} — skipping.")
        return []

    csv_files = list(dir_path.rglob("*.csv"))
    if not csv_files:
        print(f"  ⚠️  No CSV files found in {directory} — skipping.")
        return []

    print(f"  Found {len(csv_files)} CSV file(s).")
    all_docs = []
    total_skipped = 0

    for f in csv_files:
        print(f"    Loading: {f.name}...")
        try:
            df = pd.read_csv(f)

            # Validate expected columns
            required = {"Article_Text", "date"}
            if not required.issubset(df.columns):
                print(f"    ⚠️  Missing columns in {f.name} — expected {required}, got {df.columns.tolist()}")
                continue

            # Drop rows with missing text
            df = df.dropna(subset=["Article_Text"])

            # Filter out very short articles
            if "word_count" in df.columns:
                before = len(df)
                df = df[df["word_count"] >= MIN_WORDS]
                skipped = before - len(df)
                total_skipped += skipped
                if skipped:
                    print(f"      Skipped {skipped} articles under {MIN_WORDS} words.")

            for _, row in df.iterrows():
                metadata = {
                    "source_type": "newspaper",
                    "filename":    f.name,
                    "date":        str(row.get("date", "")),
                    "year":        str(row.get("Year", "")),
                    "month":       str(row.get("Month", "")),
                    "day":         str(row.get("Day", "")),
                }
                all_docs.append(Document(
                    page_content=str(row["Article_Text"]).strip(),
                    metadata=metadata,
                ))

            print(f"      Loaded {len(df)} articles from {f.name}.")
        except Exception as e:
            print(f"    ❌ Failed to load {f.name}: {e}")

    if total_skipped:
        print(f"  Total articles skipped (too short): {total_skipped}")
    return all_docs


def load_wikipedia(directory: str) -> list[Document]:
    """
    Reads Wikipedia CSVs with columns: title, url, source, Article_Text, word_count.
    Also supports plain .txt files.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"  ⚠️  Directory not found: {directory} — skipping.")
        return []

    files = list(dir_path.rglob("*.csv"))
    if not files:
        print(f"  ⚠️  No .csv files found in {directory} — skipping.")
        return []

    print(f"  Found {len(files)} Wikipedia files.")
    all_docs = []
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="ignore").strip()
            if not text:
                continue
            all_docs.append(Document(
                page_content=text,
                metadata={
                    "source_type": "wikipedia",
                    "filename":    f.name,
                    "title":       f.stem.replace("_", " "),
                }
            ))
        except Exception as e:
            print(f"    ❌ Failed to load {f.name}: {e}")
    return all_docs


# ── Core ingestion ────────────────────────────────────────────────────────────

def ingest(source_type: str, raw_docs: list[Document], collection_name: str):
    if not raw_docs:
        print(f"  No documents to ingest for {source_type}.")
        return

    print(f"\n  Splitting into chunks (size={CHUNK_SIZE}, overlap={OVERLAP_SIZE})...")
    docs = text_splitter.split_documents(raw_docs)
    total_chunks  = len(docs)
    total_batches = (total_chunks + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Total chunks: {total_chunks} | Batches: {total_batches}")

    # Try to resume existing collection
    vector_store  = None
    existing_count = 0
    try:
        vector_store = Chroma(
            collection_name=collection_name,
            embedding_function=embeddings,
            persist_directory=CHROMA_PERSIST_DIR,
        )
        existing_count = vector_store._collection.count()
        if existing_count > 0:
            print(f"  ⚡ Resuming: {existing_count} chunks already stored.")
    except Exception:
        pass

    print(f"\n  🚀 Embedding...\n")
    for i in range(0, total_chunks, BATCH_SIZE):
        batch     = docs[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1

        print(f"  🔄 [{source_type}] Batch {batch_num}/{total_batches} | chunks {i}–{min(i+BATCH_SIZE, total_chunks)-1}...", end=" ")

        try:
            if vector_store is None or (existing_count == 0 and i == 0):
                vector_store = Chroma.from_documents(
                    documents=batch,
                    embedding=embeddings,
                    collection_name=collection_name,
                    persist_directory=CHROMA_PERSIST_DIR,
                )
            else:
                vector_store.add_documents(batch)
            print("✅")
        except Exception as e:
            print(f"\n  ❌ Batch {batch_num} error: {e}")
            print("  Pausing 15s...")
            time.sleep(15)

    final_count = vector_store._collection.count()
    print(f"\n  ✅ {source_type.upper()} done: {final_count} total chunks in '{collection_name}'")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FinBot vector ingestion pipeline")
    parser.add_argument(
        "--source",
        choices=["all", "books", "newspapers", "wikipedia"],
        default="all",
    )
    args = parser.parse_args()

    runs = {
        "books":      (load_books,      BOOKS_DIR,      COLLECTIONS["books"]),
        "newspapers": (load_newspapers, NEWSPAPERS_DIR, COLLECTIONS["newspapers"]),
        "wikipedia":  (load_wikipedia,  WIKIPEDIA_DIR,  COLLECTIONS["wikipedia"]),
    }

    to_run = list(runs.keys()) if args.source == "all" else [args.source]

    for source_type in to_run:
        loader_fn, directory, collection = runs[source_type]
        print(f"\n{'='*60}")
        print(f"📚 Ingesting: {source_type.upper()}  →  {collection}")
        print(f"{'='*60}")
        raw_docs = loader_fn(directory)
        ingest(source_type, raw_docs, collection)

    print("\n\n🎉 Vector ingestion complete!")


if __name__ == "__main__":
    main()
