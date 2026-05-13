"""
batch_pipeline.py
=================
Unified pipeline — processes CSVs through:

    raw CSV  ->  normalize_scraped_news  ->  filter_business  ->  cleaned CSV

How it works:
    1. If there are CSVs in `batches/`, process them all first
    2. Once `batches/` is empty, watch for new files dropped by the scraper
    3. Repeats steps 1-2 until `cleaned/` hits 50 files — then hard stops

Place this file in the same folder as normalize_scraped_news.py and filter_business.py.

Requirements:
    pip install pandas watchdog

Usage:
    python batch_pipeline.py
"""

import sys
import time
import logging
import tempfile
from pathlib import Path

import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

sys.path.insert(0, str(Path(__file__).parent))
from normalize_scraped_news import process as normalize
from filter_business import process as filter_business

# =============================================================================
# CONFIG
# =============================================================================

BATCHES_FOLDER = "batches"   # raw CSVs go here
OUTPUT_FOLDER  = "cleaned"   # cleaned CSVs saved here
TARGET_COUNT   = 50          # hard stop when cleaned/ hits this many files
SETTLE_DELAY   = 5           # seconds to wait after a new file appears
                             # (gives scraper time to finish writing)

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("batch_pipeline.log", encoding="utf-8"),
    ],
)

# =============================================================================
# HELPERS
# =============================================================================

def cleaned_count() -> int:
    return len(list(Path(OUTPUT_FOLDER).glob("*.csv")))

def target_reached() -> bool:
    return cleaned_count() >= TARGET_COUNT

def output_path_for(raw_path: Path) -> Path:
    stem = raw_path.stem
    if not stem.endswith("_cleaned"):
        stem = stem + "_cleaned"
    return Path(OUTPUT_FOLDER) / (stem + ".csv")

# =============================================================================
# CORE: process one file
# =============================================================================

def process_file(raw_path: Path):
    out_path = output_path_for(raw_path)

    if out_path.exists():
        logging.info(f"SKIP (already cleaned): {raw_path.name}")
        return

    logging.info(f"START: {raw_path.name}  |  cleaned so far: {cleaned_count()}/{TARGET_COUNT}")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        # Step 1: Normalize
        logging.info(f"  [1/2] Normalizing ...")
        normalize(str(raw_path), str(tmp_path))

        # Step 2: Filter business
        logging.info(f"  [2/2] Filtering for business content ...")
        filter_business(str(tmp_path), str(out_path))

        tmp_path.unlink(missing_ok=True)

        # Drop URL column — was needed for filtering, not needed after
        df = pd.read_csv(out_path, dtype=str)
        df.drop(columns=["URL"], errors="ignore").to_csv(out_path, index=False)

        # Delete raw file after success (same as watcher.py)
        raw_path.unlink()
        logging.info(f"  DONE: {raw_path.name} -> {out_path.name} (raw deleted)  |  cleaned so far: {cleaned_count()}/{TARGET_COUNT}\n")

    except Exception as e:
        logging.error(f"  FAILED: {raw_path.name} — {e} (raw file kept for inspection)\n")
        if tmp_path:
            tmp_path.unlink(missing_ok=True)

# =============================================================================
# WATCHER: fires when scraper drops a new file
# =============================================================================

_new_file_arrived: list = []

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            _new_file_arrived.append(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory and event.dest_path.endswith(".csv"):
            _new_file_arrived.append(Path(event.dest_path))

# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    batches_folder = Path(BATCHES_FOLDER)
    output_folder  = Path(OUTPUT_FOLDER)

    batches_folder.mkdir(parents=True, exist_ok=True)
    output_folder.mkdir(parents=True, exist_ok=True)

    logging.info("=" * 55)
    logging.info("  Pipeline started")
    logging.info(f"  Batches folder : {batches_folder.resolve()}")
    logging.info(f"  Output folder  : {output_folder.resolve()}")
    logging.info(f"  Target         : {TARGET_COUNT} cleaned files")
    logging.info("=" * 55 + "\n")

    # Start the file watcher in the background (always listening)
    handler  = CSVHandler()
    observer = Observer()
    observer.schedule(handler, str(batches_folder), recursive=False)
    observer.start()

    try:
        while not target_reached():

            # Phase 1: drain whatever is already in batches/
            existing = sorted(batches_folder.glob("*.csv"))

            if existing:
                logging.info(f"Found {len(existing)} file(s) in batches/ — processing now...")
                for csv_file in existing:
                    if target_reached():
                        break
                    process_file(csv_file)

            else:
                # Phase 2: batches/ is empty — wait for scraper to drop new files
                logging.info("batches/ is empty — waiting for new files from scraper...")

                while not target_reached():
                    if _new_file_arrived:
                        _new_file_arrived.clear()
                        logging.info("New file detected — checking batches/...")
                        time.sleep(SETTLE_DELAY)  # let scraper finish writing
                        break
                    time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Interrupted by user.")

    finally:
        observer.stop()
        observer.join()

    logging.info(f"\n{'=' * 55}")
    logging.info(f"  Target reached: {cleaned_count()} cleaned files in {output_folder}/")
    logging.info(f"  Pipeline complete. Exiting.")
    logging.info(f"{'=' * 55}")


if __name__ == "__main__":
    main()