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

    3. When ../cleaned/ hits 50 files, calls merge_to_pdfs.py automatically
       then hard stops.

    4. Every cleaned CSV is immediately synced to shared storage so teammates
       receive files as they are produced, one by one.

    4. Every cleaned CSV is immediately synced to shared storage so teammates
       receive files as they are produced, one by one.

Folder layout:
    pipeline/                       <- all scripts live here
        batch_pipeline.py
        url_scraping.py
        normalize_scraped_news.py
        filter_business.py
        merge_to_pdfs.py
        bigquery_article_results.csv

    batches/                        <- outside pipeline/, auto-created
    cleaned/                        <- outside pipeline/, auto-created
    pdfs/                           <- outside pipeline/, auto-created by merge_to_pdfs.py

Requirements:
    pip install pandas watchdog requests trafilatura reportlab
    rclone  (system install — see README for setup, only needed if SYNC_MODE = "rclone")
    rclone  (system install — see README for setup, only needed if SYNC_MODE = "rclone")
"""

import sys
import time
import shutil
import shutil
import logging
import tempfile
import subprocess
import subprocess
import threading
from pathlib import Path

import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# All sibling scripts are in the same folder as this file
PIPELINE_DIR = Path(__file__).parent
sys.path.insert(0, str(PIPELINE_DIR))

from normalize_scraped_news import process as normalize
from filter_business import process as filter_business
import url_scraping as url_scraping
from normalize_scraped_news import process as normalize
from filter_business import process as filter_business
import url_scraping as url_scraping

# =============================================================================
# CONFIG  — paths are relative to pipeline/ (one level up for data folders)
# =============================================================================

BATCHES_FOLDER = PIPELINE_DIR.parent / "batches"
OUTPUT_FOLDER  = PIPELINE_DIR.parent / "cleaned"
TARGET_COUNT   = 50          # hard stop when cleaned/ hits this many files
SETTLE_DELAY   = 5           # seconds to wait after a new file appears

# =============================================================================
# SYNC CONFIG
#
# Three sync modes — set SYNC_MODE to whichever matches your setup:
#
#   "rclone"  — Google Drive or any rclone remote (recommended for remote teams)
#               Set RCLONE_REMOTE to your configured remote + folder.
#               Example: "gdrive:my-team-folder/cleaned"
#               One-time setup: run `rclone config` on your machine first.
#
#   "folder"  — Local or mounted network path (same LAN, mapped drive, NFS).
#               Set SYNC_FOLDER to the destination path.
#               Example: Path("/mnt/team-share/cleaned")
#
#   "off"     — No syncing. Pipeline runs exactly as original.
#               Use this if working alone or sharing is not yet configured.
#
# =============================================================================

SYNC_MODE     = "rclone"                            # "rclone" | "folder" | "off"
RCLONE_REMOTE = "gdrive:large-files/cleaned"   # only used when SYNC_MODE = "rclone"
SYNC_FOLDER   = Path("/mnt/team-share/cleaned")     # only used when SYNC_MODE = "folder"

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PIPELINE_DIR / "batch_pipeline.log", encoding="utf-8"),
    ],
)

# =============================================================================
# HELPERS
# =============================================================================

def cleaned_count() -> int:
    return len(list(OUTPUT_FOLDER.glob("*.csv")))

def target_reached() -> bool:
    return cleaned_count() >= TARGET_COUNT

def output_path_for(raw_path: Path) -> Path:
    stem = raw_path.stem
    if not stem.endswith("_cleaned"):
        stem = stem + "_cleaned"
    return OUTPUT_FOLDER / (stem + ".csv")

# =============================================================================
# SYNC: push one cleaned file to shared storage immediately after it is written
#
# Called once per batch, right after process_file succeeds.
# Runs in a background daemon thread so it never blocks the pipeline loop.
# A sync failure is logged as a warning only — it never affects the local file,
# which is already safely written to cleaned/ before sync is attempted.
# =============================================================================

def _sync_worker(out_path: Path):
    """
    Does the actual upload. Runs in a background thread.
    Never raises — all errors are caught and logged as warnings.
    """

    if SYNC_MODE == "rclone":
        # `rclone copy <file> <remote:folder>` uploads only that one file.
        # --retries and --low-level-retries handle transient network blips.
        try:
            result = subprocess.run(
                [
                    "rclone", "copy",
                    str(out_path),          # source: the finished local CSV
                    RCLONE_REMOTE,          # destination: configured remote folder
                    "--retries", "3",
                    "--low-level-retries", "3",
                ],
                capture_output=True,
                text=True,
                timeout=300,               # 5-minute hard timeout per file
            )
            if result.returncode == 0:
                logging.info(f"  SYNCED → {RCLONE_REMOTE}/{out_path.name}")
            else:
                logging.warning(
                    f"  SYNC FAILED (rclone): {out_path.name}\n"
                    f"    stderr: {result.stderr.strip()}"
                )
        except FileNotFoundError:
            logging.warning(
                "  SYNC SKIPPED: rclone not found on this machine. "
                "Install rclone or set SYNC_MODE = 'off'."
            )
        except subprocess.TimeoutExpired:
            logging.warning(
                f"  SYNC TIMEOUT: {out_path.name} took over 5 minutes — skipped. "
                "File is safe locally in cleaned/."
            )
        except Exception as e:
            logging.warning(f"  SYNC ERROR (rclone): {out_path.name} — {e}")

    elif SYNC_MODE == "folder":
        # shutil.copy2 copies the file and preserves its timestamps.
        try:
            SYNC_FOLDER.mkdir(parents=True, exist_ok=True)
            dest = SYNC_FOLDER / out_path.name
            shutil.copy2(out_path, dest)
            logging.info(f"  SYNCED → {dest}")
        except Exception as e:
            logging.warning(f"  SYNC FAILED (folder): {out_path.name} — {e}")


def sync_to_shared(out_path: Path):
    """
    Fire-and-forget sync. Launches _sync_worker in a background daemon thread
    and returns immediately — the pipeline loop is never blocked by upload speed.
    Does nothing if SYNC_MODE is "off".
    """
    if SYNC_MODE == "off":
        return

    t = threading.Thread(
        target=_sync_worker,
        args=(out_path,),
        daemon=True,                        # killed automatically if main process exits
        name=f"sync-{out_path.stem}",
    )
    t.start()

# =============================================================================
# SYNC: push one cleaned file to shared storage immediately after it is written
#
# Called once per batch, right after process_file succeeds.
# Runs in a background daemon thread so it never blocks the pipeline loop.
# A sync failure is logged as a warning only — it never affects the local file,
# which is already safely written to cleaned/ before sync is attempted.
# =============================================================================

def _sync_worker(out_path: Path):
    """
    Does the actual upload. Runs in a background thread.
    Never raises — all errors are caught and logged as warnings.
    """

    if SYNC_MODE == "rclone":
        # `rclone copy <file> <remote:folder>` uploads only that one file.
        # --retries and --low-level-retries handle transient network blips.
        try:
            result = subprocess.run(
                [
                    "rclone", "copy",
                    str(out_path),          # source: the finished local CSV
                    RCLONE_REMOTE,          # destination: configured remote folder
                    "--retries", "3",
                    "--low-level-retries", "3",
                ],
                capture_output=True,
                text=True,
                timeout=300,               # 5-minute hard timeout per file
            )
            if result.returncode == 0:
                logging.info(f"  SYNCED → {RCLONE_REMOTE}/{out_path.name}")
            else:
                logging.warning(
                    f"  SYNC FAILED (rclone): {out_path.name}\n"
                    f"    stderr: {result.stderr.strip()}"
                )
        except FileNotFoundError:
            logging.warning(
                "  SYNC SKIPPED: rclone not found on this machine. "
                "Install rclone or set SYNC_MODE = 'off'."
            )
        except subprocess.TimeoutExpired:
            logging.warning(
                f"  SYNC TIMEOUT: {out_path.name} took over 5 minutes — skipped. "
                "File is safe locally in cleaned/."
            )
        except Exception as e:
            logging.warning(f"  SYNC ERROR (rclone): {out_path.name} — {e}")

    elif SYNC_MODE == "folder":
        # shutil.copy2 copies the file and preserves its timestamps.
        try:
            SYNC_FOLDER.mkdir(parents=True, exist_ok=True)
            dest = SYNC_FOLDER / out_path.name
            shutil.copy2(out_path, dest)
            logging.info(f"  SYNCED → {dest}")
        except Exception as e:
            logging.warning(f"  SYNC FAILED (folder): {out_path.name} — {e}")


def sync_to_shared(out_path: Path):
    """
    Fire-and-forget sync. Launches _sync_worker in a background daemon thread
    and returns immediately — the pipeline loop is never blocked by upload speed.
    Does nothing if SYNC_MODE is "off".
    """
    if SYNC_MODE == "off":
        return

    t = threading.Thread(
        target=_sync_worker,
        args=(out_path,),
        daemon=True,                        # killed automatically if main process exits
        name=f"sync-{out_path.stem}",
    )
    t.start()

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

        # Delete raw file after success
        raw_path.unlink()
        logging.info(f"  DONE: {raw_path.name} -> {out_path.name} (raw deleted)  |  cleaned so far: {cleaned_count()}/{TARGET_COUNT}\n")

        # Push the finished cleaned file to shared storage immediately.
        # Runs in background — pipeline does not wait for the upload to finish.
        sync_to_shared(out_path)

        # Push the finished cleaned file to shared storage immediately.
        # Runs in background — pipeline does not wait for the upload to finish.
        sync_to_shared(out_path)

    except Exception as e:
        logging.error(f"  FAILED: {raw_path.name} — {e} (raw file kept for inspection)\n")
        if tmp_path:
            tmp_path.unlink(missing_ok=True)

# =============================================================================
# WATCHER: detects new files dropped by the scraper
# =============================================================================

_new_file_arrived: list = []

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        src = str(event.src_path)
        if not event.is_directory and src.endswith(".csv"):
            _new_file_arrived.append(Path(src))

    def on_moved(self, event):
        dest = str(event.dest_path)
        if not event.is_directory and dest.endswith(".csv"):
            _new_file_arrived.append(Path(dest))

# =============================================================================
# SCRAPER: runs url_scraping.run_all() in a background thread
# =============================================================================

def start_scraper():
    def _run():
        logging.info("Scraper thread started — running url_scraping.run_all()")
        try:
            url_scraping.run_all()
            logging.info("Scraper thread finished — all batches scraped.")
        except Exception as e:
            logging.error(f"Scraper thread crashed: {e}", exc_info=True)

    t = threading.Thread(target=_run, daemon=True, name="scraper")
    t.start()
    return t

# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    BATCHES_FOLDER.mkdir(parents=True, exist_ok=True)
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

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
    logging.info(f"  Sync mode      : {SYNC_MODE}")
    if SYNC_MODE == "rclone":
        logging.info(f"  Sync remote    : {RCLONE_REMOTE}")
    elif SYNC_MODE == "folder":
        logging.info(f"  Sync folder    : {SYNC_FOLDER}")
    logging.info("=" * 55 + "\n")

    # Start scraper in background
    start_scraper()

    # Start file watcher
    handler  = CSVHandler()
    observer = Observer()
    observer.schedule(handler, str(BATCHES_FOLDER), recursive=False)
    observer.start()

    try:
        while not target_reached():

            # Phase 1: drain whatever is already in batches/
            existing = sorted(BATCHES_FOLDER.glob("*.csv"))

            if existing:
                logging.info(f"Found {len(existing)} file(s) in batches/ — processing now...")
                for csv_file in existing:
                    if target_reached():
                        break
                    process_file(csv_file)

            else:
                # Phase 2: batches/ is empty — wait for scraper to drop next batch
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