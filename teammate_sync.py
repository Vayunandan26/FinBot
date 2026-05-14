"""
teammate_sync.py
================
Run this once on a teammate's machine to continuously pull new cleaned
CSV files from shared Google Drive storage as the pipeline produces them.

    python teammate_sync.py

Polls every 60 seconds. Copies any new or updated files from the shared
Drive folder into your local cleaned/ directory. Stops on Ctrl+C.

Requirements:
    rclone installed and configured with a remote named "gdrive"
    See DATA_SETUP.md for rclone setup instructions.
"""

import subprocess
import time
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# =============================================================================
# CONFIG — update RCLONE_REMOTE to match your rclone configuration
# =============================================================================

RCLONE_REMOTE  = "gdrive:your-team-folder/cleaned"   # remote source
LOCAL_CLEANED  = Path(__file__).parent / "cleaned"    # local destination
POLL_INTERVAL  = 60                                   # seconds between syncs

# =============================================================================

def sync_once():
    """Pull any new files from shared storage into local cleaned/."""
    LOCAL_CLEANED.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            "rclone", "sync",
            RCLONE_REMOTE,              # source: shared Google Drive folder
            str(LOCAL_CLEANED),         # destination: your local cleaned/
            "--update",                 # skip files that are already up to date
            "--transfers", "4",         # download up to 4 files in parallel
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )

    if result.returncode == 0:
        # Count how many CSVs are now local
        count = len(list(LOCAL_CLEANED.glob("*.csv")))
        logging.info(f"Sync complete — {count} cleaned file(s) in cleaned/")
    else:
        logging.warning(f"Sync error:\n{result.stderr.strip()}")


def main():
    logging.info("=" * 50)
    logging.info("  Teammate sync watcher started")
    logging.info(f"  Remote : {RCLONE_REMOTE}")
    logging.info(f"  Local  : {LOCAL_CLEANED}")
    logging.info(f"  Polling every {POLL_INTERVAL}s — Ctrl+C to stop")
    logging.info("=" * 50 + "\n")

    # Run once immediately on startup
    sync_once()

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            logging.info("Checking for new files...")
            sync_once()
        except KeyboardInterrupt:
            logging.info("Stopped by user.")
            break
        except subprocess.TimeoutExpired:
            logging.warning("Sync timed out — will retry next cycle.")
        except Exception as e:
            logging.warning(f"Unexpected error: {e} — will retry next cycle.")


if __name__ == "__main__":
    main()
