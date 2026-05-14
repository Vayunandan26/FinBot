"""
teammate_sync.py
================
Run this once on your machine to continuously pull new cleaned CSV files
from the shared Google Drive folder as the pipeline owner produces them.

    python teammate_sync.py

How it works:
    - Polls the shared Google Drive folder every 60 seconds
    - Downloads any new or updated CSV files into your local cleaned/ folder
    - Skips files that are already up to date (no redundant downloads)
    - Stops cleanly on Ctrl+C

Requirements:
    rclone installed and configured — see DATA_SETUP.md for step-by-step setup.

Before running:
    1. Install rclone (see DATA_SETUP.md)
    2. Run `rclone config` to connect your Google account
    3. Ask the pipeline owner to share the Drive folder with your Gmail
    4. Update RCLONE_REMOTE below to match the shared folder name
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
# CONFIG
# Update RCLONE_REMOTE to match the shared folder name the owner gave you.
# Format: "gdrive:folder-name/cleaned"
# Example: if the shared folder is called "finance-chatbot-data", use:
#          "gdrive:finance-chatbot-data/cleaned"
# =============================================================================

RCLONE_REMOTE = "gdrive:your-team-folder/cleaned"   # ← update this
LOCAL_CLEANED = Path(__file__).parent / "cleaned"    # local destination folder
POLL_INTERVAL = 60                                   # seconds between each check

# =============================================================================


def check_rclone():
    """Verify rclone is installed and the remote is reachable before starting."""
    try:
        result = subprocess.run(
            ["rclone", "lsd", RCLONE_REMOTE],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            logging.error(
                f"Cannot reach remote '{RCLONE_REMOTE}'.\n"
                f"  Error: {result.stderr.strip()}\n"
                f"  Make sure:\n"
                f"    1. rclone is installed\n"
                f"    2. You ran `rclone config` and named the remote 'gdrive'\n"
                f"    3. The pipeline owner shared the Drive folder with your Gmail\n"
                f"    4. RCLONE_REMOTE in this file matches the actual folder name"
            )
            return False
        return True
    except FileNotFoundError:
        logging.error(
            "rclone not found. Install it first:\n"
            "  Mac:   brew install rclone\n"
            "  Linux: sudo apt install rclone\n"
            "Then run `rclone config` to connect your Google account."
        )
        return False


def sync_once():
    """
    Pull any new or updated CSV files from shared Drive into local cleaned/.
    Uses rclone sync with --update so only new/changed files are downloaded.
    """
    LOCAL_CLEANED.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            "rclone", "sync",
            RCLONE_REMOTE,          # source: shared Google Drive folder
            str(LOCAL_CLEANED),     # destination: local cleaned/ folder
            "--update",             # skip files that haven't changed
            "--transfers", "4",     # download up to 4 files simultaneously
            "--stats", "0",         # suppress per-file progress spam
        ],
        capture_output=True,
        text=True,
        timeout=300,                # 5-minute timeout per sync cycle
    )

    if result.returncode == 0:
        count = len(list(LOCAL_CLEANED.glob("*.csv")))
        logging.info(f"Sync complete — {count} cleaned file(s) in cleaned/")
    else:
        logging.warning(
            f"Sync error — will retry in {POLL_INTERVAL}s\n"
            f"  {result.stderr.strip()}"
        )


def main():
    logging.info("=" * 50)
    logging.info("  Teammate sync watcher started")
    logging.info(f"  Remote  : {RCLONE_REMOTE}")
    logging.info(f"  Local   : {LOCAL_CLEANED}")
    logging.info(f"  Polling : every {POLL_INTERVAL}s  |  Ctrl+C to stop")
    logging.info("=" * 50 + "\n")

    # Verify rclone works before entering the loop
    if not check_rclone():
        logging.error("Fix the issues above and re-run teammate_sync.py.")
        return

    # Sync immediately on startup so you don't wait 60s for the first pull
    logging.info("Running initial sync...")
    sync_once()

    # Poll loop
    while True:
        try:
            time.sleep(POLL_INTERVAL)
            logging.info("Checking for new files...")
            sync_once()

        except KeyboardInterrupt:
            logging.info("\nStopped by user. Your local cleaned/ folder is up to date.")
            break

        except subprocess.TimeoutExpired:
            logging.warning(f"Sync timed out — will retry in {POLL_INTERVAL}s.")

        except Exception as e:
            logging.warning(f"Unexpected error: {e} — will retry in {POLL_INTERVAL}s.")


if __name__ == "__main__":
    main()
