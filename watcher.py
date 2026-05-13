"""
watcher.py
==========
Watches a folder for new scraped CSV files, runs them through a two-step
pipeline: (1) normalize text, (2) keep only business articles.
Saves the final cleaned CSV, then deletes the original.

Also processes any existing raw CSVs already in the folder on startup
(your 5 already-scraped files will be handled automatically).

Pipeline per file:
    raw CSV  →  normalize_scraped_news  →  filter_business  →  cleaned CSV
                (clean text/spacing)       (drop non-biz)

Requirements:
    pip install watchdog pandas

Usage:
    python watcher.py

Configuration — edit the three paths below before running.
"""

import os
import sys
import time
import logging
import threading
import tempfile
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Import pipeline steps (must be in the same folder as this script) ─────────
sys.path.insert(0, str(Path(__file__).parent))
from normalize_scraped_news import process as normalize
from filter_business import process as filter_business

# =============================================================================
# CONFIGURATION — edit these before running
# =============================================================================

# Folder where the scraper dumps raw CSV files
WATCH_FOLDER  = r"C:\news_pipeline\raw"

# Folder where cleaned CSVs will be saved
OUTPUT_FOLDER = r"C:\news_pipeline\cleaned"

# How long (seconds) to wait after a file appears before processing it.
# Gives the scraper script time to finish writing before we read the file.
SETTLE_DELAY  = 5

# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),                          # console
        logging.FileHandler("watcher.log", encoding="utf-8"),  # log file
    ],
)

# Track files currently being processed to avoid double-triggers
_in_progress: set = set()
_lock = threading.Lock()


def output_path_for(raw_path: Path) -> Path:
    """Build the cleaned output path from the raw input path."""
    stem = raw_path.stem
    if not stem.endswith("_cleaned"):
        stem = stem + "_cleaned"
    return Path(OUTPUT_FOLDER) / (stem + ".csv")


def handle_file(raw_path: Path):
    """Run two-step pipeline on one CSV, save final output, delete original."""
    with _lock:
        if str(raw_path) in _in_progress:
            return
        _in_progress.add(str(raw_path))

    try:
        # Wait for the file to stop being written to
        logging.info(f"Detected: {raw_path.name} — waiting {SETTLE_DELAY}s for write to finish...")
        time.sleep(SETTLE_DELAY)

        # Verify it still exists (scraper might have moved/deleted it)
        if not raw_path.exists():
            logging.warning(f"File disappeared before processing: {raw_path.name}")
            return

        final_out = output_path_for(raw_path)

        # Step 1: Normalize text (clean spacing, remove scraper errors, strip footers)
        # Write to a temp file so Step 2 can read it
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        logging.info(f"Step 1/2 — Normalizing: {raw_path.name}")
        normalize(str(raw_path), tmp_path)

        # Step 2: Business filter (drop non-business articles)
        logging.info(f"Step 2/2 — Filtering for business content: {raw_path.name}")
        filter_business(tmp_path, str(final_out))

        # Clean up temp file
        Path(tmp_path).unlink(missing_ok=True)

        # Delete the original raw file only after successful pipeline
        raw_path.unlink()
        logging.info(f"Done. Original deleted: {raw_path.name} → {final_out.name}")

    except Exception as e:
        logging.error(f"Failed to process {raw_path.name}: {e}", exc_info=True)
        # Do NOT delete the original if processing failed — leave it for inspection

    finally:
        with _lock:
            _in_progress.discard(str(raw_path))


class CSVHandler(FileSystemEventHandler):
    """Watchdog event handler — reacts to new CSV files."""

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() == ".csv":
            # Run in a background thread so the watcher loop stays responsive
            threading.Thread(target=handle_file, args=(path,), daemon=True).start()

    def on_moved(self, event):
        """Catches files moved/renamed into the watch folder."""
        if event.is_directory:
            return
        path = Path(event.dest_path)
        if path.suffix.lower() == ".csv":
            threading.Thread(target=handle_file, args=(path,), daemon=True).start()


def process_existing(watch_folder: Path):
    """Process any CSVs already sitting in the folder when the watcher starts."""
    existing = list(watch_folder.glob("*.csv"))
    if not existing:
        logging.info("No existing CSVs to process on startup.")
        return
    logging.info(f"Found {len(existing)} existing CSV(s) — processing now...")
    for csv_file in existing:
        threading.Thread(target=handle_file, args=(csv_file,), daemon=True).start()


def main():
    watch_folder  = Path(WATCH_FOLDER)
    output_folder = Path(OUTPUT_FOLDER)

    # Create folders if they don't exist
    watch_folder.mkdir(parents=True, exist_ok=True)
    output_folder.mkdir(parents=True, exist_ok=True)

    logging.info("=" * 60)
    logging.info("  News CSV Watcher started")
    logging.info(f"  Watching : {watch_folder}")
    logging.info(f"  Output   : {output_folder}")
    logging.info(f"  Settle   : {SETTLE_DELAY}s")
    logging.info("=" * 60)

    # Handle files already present (your 5 existing files)
    process_existing(watch_folder)

    # Start watching for new files
    handler  = CSVHandler()
    observer = Observer()
    observer.schedule(handler, str(watch_folder), recursive=False)
    observer.start()

    logging.info("Watcher running — press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        observer.stop()
    observer.join()
    logging.info("Watcher stopped.")


if __name__ == "__main__":
    main()
