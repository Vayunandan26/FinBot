"""
finbot_sync.py — FinBot Data Sync
===================================
One-shot script that collects all FinBot data sources and pushes them
to a shared Google Drive folder via rclone.

Handles three data types independently — each goes into its own subfolder
on Drive so teammates can pull only what they need:

    gdrive:large-files/
        finbot/
            wikipedia/          ← wikipedia_business.csv
            filings/            ← edgar, esma, cvm, india_ir CSVs
            books/              ← PDF books for RAG ingestion

Teammates run:
    python finbot_sync.py --pull         pull everything from Drive to local
    python finbot_sync.py --pull --source wikipedia   pull one source only

You (the owner) run:
    python finbot_sync.py                push everything to Drive
    python finbot_sync.py --source books push one source only

Usage:
    python finbot_sync.py                     # push all sources
    python finbot_sync.py --source wikipedia  # push wikipedia only
    python finbot_sync.py --source filings    # push filings only
    python finbot_sync.py --source books      # push books only
    python finbot_sync.py --pull              # pull all sources (teammates)
    python finbot_sync.py --pull --source wikipedia
    python finbot_sync.py --dry-run           # preview without transferring
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

# =============================================================================
# CONFIG — update these paths to match your local folder layout
# =============================================================================

# Root of your FinBot project (folder containing pipeline/, cleaned/, books/ etc.)
PROJECT_ROOT = Path(__file__).parent

# Local source folders — update if your layout differs
LOCAL_SOURCES = {
    "wikipedia": PROJECT_ROOT / "data" /"cleaned_wikipedia",          # wikipedia_business.csv lives here     
    "books":     PROJECT_ROOT / "data" / "books",            # PDF books
}

# File patterns per source — controls which files get picked up
SOURCE_PATTERNS = {
    "wikipedia": ["wikipedia_business*.csv"],
    "books":     ["*.pdf"],
}

# Drive remote — must match what's in batch_pipeline.py
RCLONE_REMOTE_ROOT = "gdrive:large-files/finbot"

# Remote subfolder per source
REMOTE_SUBFOLDERS = {
    "wikipedia": f"{RCLONE_REMOTE_ROOT}/wikipedia",
    "books":     f"{RCLONE_REMOTE_ROOT}/books",
}

# Local destination when pulling (teammates)
PULL_DESTINATIONS = {
    "wikipedia": PROJECT_ROOT / "data" / "wikipedia",
    "books":     PROJECT_ROOT / "data" / "books",
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# HELPERS
# =============================================================================

def check_rclone() -> bool:
    """Verify rclone is installed and the remote is reachable."""
    try:
        result = subprocess.run(
            ["rclone", "lsd", RCLONE_REMOTE_ROOT],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            log.error(
                f"Cannot reach remote '{RCLONE_REMOTE_ROOT}'.\n"
                f"  {result.stderr.strip()}\n\n"
                f"  Setup steps:\n"
                f"    1. Install rclone:  brew install rclone  (Mac)\n"
                f"                        sudo apt install rclone  (Linux)\n"
                f"    2. Run:  rclone config  — add a remote named 'gdrive'\n"
                f"    3. Make sure the Drive folder is shared with your Gmail\n"
                f"    4. Check RCLONE_REMOTE_ROOT in this file matches the folder"
            )
            return False
        return True
    except FileNotFoundError:
        log.error(
            "rclone not found.\n"
            "  Mac:   brew install rclone\n"
            "  Linux: sudo apt install rclone\n"
            "Then run: rclone config"
        )
        return False


def collect_files(source: str) -> list[Path]:
    """Return list of local files that exist for a given source."""
    local_dir = LOCAL_SOURCES[source]
    patterns  = SOURCE_PATTERNS[source]
    found = []
    for pattern in patterns:
        found.extend(sorted(local_dir.glob(pattern)))
    return found


def push_source(source: str, dry_run: bool = False) -> bool:
    """Push files for one source to Drive using rclone copy."""
    files = collect_files(source)
    remote = REMOTE_SUBFOLDERS[source]

    if not files:
        log.warning(f"  [{source}] No files found in {LOCAL_SOURCES[source]} — skipping")
        return True

    log.info(f"  [{source}] {len(files)} file(s) → {remote}")
    for f in files:
        log.info(f"    {f.name}  ({f.stat().st_size / 1024 / 1024:.1f} MB)")

    success = True
    for file_path in files:
        cmd = [
            "rclone", "copy",
            str(file_path),
            remote,
            "--retries", "3",
            "--low-level-retries", "3",
            "--stats", "10s",
        ]
        if dry_run:
            cmd.append("--dry-run")

        log.info(f"  Uploading {file_path.name} …")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

        if result.returncode == 0:
            log.info(f"  ✓ {file_path.name} uploaded")
        else:
            log.error(f"  ✗ {file_path.name} failed: {result.stderr.strip()}")
            success = False

    return success


def pull_source(source: str, dry_run: bool = False) -> bool:
    """Pull all files for one source from Drive to local destination."""
    remote  = REMOTE_SUBFOLDERS[source]
    dest    = PULL_DESTINATIONS[source]
    dest.mkdir(parents=True, exist_ok=True)

    log.info(f"  [{source}] {remote} → {dest}")

    cmd = [
        "rclone", "sync",
        remote,
        str(dest),
        "--update",
        "--transfers", "4",
        "--retries", "3",
        "--stats", "10s",
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    if result.returncode == 0:
        files = list(dest.glob("*"))
        log.info(f"  ✓ {len(files)} file(s) in {dest}")
        for f in files:
            log.info(f"    {f.name}  ({f.stat().st_size / 1024 / 1024:.1f} MB)")
        return True
    else:
        log.error(f"  ✗ Pull failed: {result.stderr.strip()}")
        return False


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="FinBot Data Sync")
    parser.add_argument("--pull",     action="store_true",
                        help="Pull from Drive to local (teammate mode). Default: push.")
    parser.add_argument("--source",   type=str, default=None,
                        help="Single source: wikipedia | books")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Preview transfers without moving any files")
    args = parser.parse_args()

    sources = list(LOCAL_SOURCES.keys())
    if args.source:
        if args.source not in LOCAL_SOURCES:
            log.error(f"Unknown source '{args.source}'. Choose from: {sources}")
            sys.exit(1)
        sources = [args.source]

    mode = "PULL (teammate)" if args.pull else "PUSH (owner)"

    log.info("=" * 60)
    log.info(f"  FinBot Data Sync — {mode}")
    log.info(f"  Sources  : {', '.join(sources)}")
    log.info(f"  Remote   : {RCLONE_REMOTE_ROOT}")
    log.info(f"  Dry run  : {args.dry_run}")
    log.info("=" * 60)

    if not check_rclone():
        sys.exit(1)

    results = {}
    for source in sources:
        log.info(f"\n{'─' * 50}")
        if args.pull:
            results[source] = pull_source(source, dry_run=args.dry_run)
        else:
            results[source] = push_source(source, dry_run=args.dry_run)

    log.info(f"\n{'=' * 60}")
    log.info("  Summary")
    log.info(f"{'=' * 60}")
    for source, ok in results.items():
        status = "✓ OK" if ok else "✗ FAILED"
        log.info(f"  {source:<15} {status}")

    if not all(results.values()):
        sys.exit(1)

    if args.pull:
        log.info(f"\n  All data pulled to {PROJECT_ROOT / 'data'}/")
    else:
        log.info(f"\n  All data pushed to {RCLONE_REMOTE_ROOT}/")
        log.info("  Share this Drive folder with teammates:")
        log.info(f"  → gdrive: large-files/finbot")
        log.info("  They pull with: python finbot_sync.py --pull")


if __name__ == "__main__":
    main()
