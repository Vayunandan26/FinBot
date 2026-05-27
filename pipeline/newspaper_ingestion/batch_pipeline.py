"""
batch_pipeline.py
=================
Single entry point for the entire pipeline. Run this and everything happens:

    python batch_pipeline.py

What runs:
    1. url_scraping.py  runs in a background thread — scrapes URLs from
                        bigquery_article_results.csv and saves batches to ../batches/

    2. As soon as each batch CSV lands in ../batches/, the pipeline picks it up:
           normalize_scraped_news  ->  filter_business  ->  cleaned CSV

    3. When ..data/cleaned_articles/ hits 50 files, it automatically hard stops.

    4. Every cleaned CSV is immediately synced to shared storage so teammates
       receive files as they are produced, one by one.

Folder layout:
    pipeline/                       
        newpaper_ingestion/         <- all scripts live here
            batch_pipeline.py
            url_scraping.py
            normalize_scraped_news.py
            filter_business.py
    bigquery_article_results.csv

    batches/                        <- outside pipeline/, auto-created
    data/cleaned_articles/          <- outside pipeline/ but inside data/, auto-created

Requirements:
    pip install pandas watchdog requests trafilatura
    rclone  (system install — see README for setup, only needed if SYNC_MODE = "rclone")
"""

import sys
import time
import shutil
import logging
import tempfile
import subprocess
import threading
from pathlib import Path

import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

PIPELINE_DIR = Path(__file__).parent
ROOT_DIR     = PIPELINE_DIR.parent.parent

sys.path.insert(0, str(PIPELINE_DIR))

from normalize_scraped_news import process as normalize
from filter_business import process as filter_business
import url_scraping as url_scraping

BATCHES_FOLDER = ROOT_DIR / "batches"
OUTPUT_FOLDER  = ROOT_DIR / "data" / "cleaned_articles"
TARGET_COUNT   = 50
SETTLE_DELAY   = 5

SYNC_MODE     = "rclone"
RCLONE_REMOTE = "gdrive:large-files/cleaned"
SYNC_FOLDER   = Path("/mnt/team-share/cleaned")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PIPELINE_DIR / "batch_pipeline.log", encoding="utf-8"),
    ],
)


def cleaned_count() -> int:
    return len(list(OUTPUT_FOLDER.glob("*.csv")))

def target_reached() -> bool:
    return cleaned_count() >= TARGET_COUNT

def output_path_for(raw_path: Path) -> Path:
    stem = raw_path.stem
    if not stem.endswith("_cleaned"):
        stem = stem + "_cleaned"
    return OUTPUT_FOLDER / (stem + ".csv")


def _sync_worker(out_path: Path):
    if SYNC_MODE == "rclone":
        try:
            result = subprocess.run(
                [
                    "rclone", "copy",
                    str(out_path),
                    RCLONE_REMOTE,
                    "--retries", "3",
                    "--low-level-retries", "3",
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode == 0:
                logging.info(f"  SYNCED → {RCLONE_REMOTE}/{out_path.name}")
            else:
                logging.warning(f"  SYNC FAILED (rclone): {out_path.name}\n    stderr: {result.stderr.strip()}")
        except FileNotFoundError:
            logging.warning("  SYNC SKIPPED: rclone not found. Install rclone or set SYNC_MODE = 'off'.")
        except subprocess.TimeoutExpired:
            logging.warning(f"  SYNC TIMEOUT: {out_path.name} — skipped. File is safe locally.")
        except Exception as e:
            logging.warning(f"  SYNC ERROR (rclone): {out_path.name} — {e}")

    elif SYNC_MODE == "folder":
        try:
            SYNC_FOLDER.mkdir(parents=True, exist_ok=True)
            shutil.copy2(out_path, SYNC_FOLDER / out_path.name)
            logging.info(f"  SYNCED → {SYNC_FOLDER / out_path.name}")
        except Exception as e:
            logging.warning(f"  SYNC FAILED (folder): {out_path.name} — {e}")


def sync_to_shared(out_path: Path):
    if SYNC_MODE == "off":
        return
    threading.Thread(
        target=_sync_worker,
        args=(out_path,),
        daemon=True,
        name=f"sync-{out_path.stem}",
    ).start()


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

        logging.info("  [1/2] Normalizing ...")
        normalize(str(raw_path), str(tmp_path))

        logging.info("  [2/2] Filtering for business content ...")
        filter_business(str(tmp_path), str(out_path))

        tmp_path.unlink(missing_ok=True)

        df = pd.read_csv(out_path, dtype=str)
        df.drop(columns=["URL"], errors="ignore").to_csv(out_path, index=False)

        raw_path.unlink()
        logging.info(f"  DONE: {raw_path.name} -> {out_path.name} (raw deleted)  |  cleaned so far: {cleaned_count()}/{TARGET_COUNT}\n")

        sync_to_shared(out_path)

    except Exception as e:
        logging.error(f"  FAILED: {raw_path.name} — {e} (raw file kept for inspection)\n")
        if tmp_path:
            tmp_path.unlink(missing_ok=True)


_new_file_arrived: list = []

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and str(event.src_path).endswith(".csv"):
            _new_file_arrived.append(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory and str(event.dest_path).endswith(".csv"):
            _new_file_arrived.append(Path(event.dest_path))


def start_scraper():
    def _run():
        logging.info("Scraper thread started — running url_scraping.run_all()")
        try:
            url_scraping.run_all()
            logging.info("Scraper thread finished — all batches scraped.")
        except Exception as e:
            logging.error(f"Scraper thread crashed: {e}", exc_info=True)

    threading.Thread(target=_run, daemon=True, name="scraper").start()


def main():
    BATCHES_FOLDER.mkdir(parents=True, exist_ok=True)
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    if SYNC_MODE == "rclone":
        logging.info("Pushing existing backlog in cleaned_articles/ to shared storage...")
        subprocess.run(["rclone", "copy", str(OUTPUT_FOLDER), RCLONE_REMOTE])

    logging.info("=" * 55)
    logging.info("  Pipeline started")
    logging.info(f"  Batches folder : {BATCHES_FOLDER}")
    logging.info(f"  Output folder  : {OUTPUT_FOLDER}")
    logging.info(f"  Target         : {TARGET_COUNT} cleaned files")
    logging.info(f"  Sync mode      : {SYNC_MODE}")
    if SYNC_MODE == "rclone":
        logging.info(f"  Sync remote    : {RCLONE_REMOTE}")
    elif SYNC_MODE == "folder":
        logging.info(f"  Sync folder    : {SYNC_FOLDER}")
    logging.info("=" * 55 + "\n")

    start_scraper()

    handler  = CSVHandler()
    observer = Observer()
    observer.schedule(handler, str(BATCHES_FOLDER), recursive=False)
    observer.start()

    try:
        while not target_reached():
            existing = sorted(BATCHES_FOLDER.glob("*.csv"))

            if existing:
                logging.info(f"Found {len(existing)} file(s) in batches/ — processing now...")
                for csv_file in existing:
                    if target_reached():
                        break
                    process_file(csv_file)
            else:
                logging.info("Waiting for scraper to produce next batch...")
                while not target_reached():
                    if _new_file_arrived:
                        _new_file_arrived.clear()
                        logging.info("New batch detected — checking batches/...")
                        time.sleep(SETTLE_DELAY)
                        break
                    time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Interrupted by user.")

    finally:
        observer.stop()
        observer.join()

    logging.info(f"\n{'=' * 55}")
    logging.info(f"  Target reached: {cleaned_count()} cleaned files in {OUTPUT_FOLDER}/")
    logging.info(f"  Pipeline complete. Exiting.")
    logging.info(f"{'=' * 55}")


if __name__ == "__main__":
    main()