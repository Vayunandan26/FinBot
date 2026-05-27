import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

PIPELINE_DIR = Path(__file__).parent
ROOT_DIR     = PIPELINE_DIR.parent

POLL_INTERVAL = 60

SOURCES = {
    "articles": {
        "local":    ROOT_DIR / "data" / "cleaned_articles",
        "remote":   "gdrive:large-files/cleaned",
        "patterns": ["*.csv"],
    },
    "wikipedia": {
        "local":    ROOT_DIR / "data" / "cleaned_wikipedia",
        "remote":   "gdrive:large-files/finbot/wikipedia",
        "patterns": ["wikipedia_business*.csv"],
    },
    "books": {
        "local":    ROOT_DIR / "data" / "books",
        "remote":   "gdrive:large-files/finbot/books",
        "patterns": ["*.pdf"],
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def check_rclone() -> bool:
    try:
        result = subprocess.run(
            ["rclone", "lsd", "gdrive:large-files"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            log.error(
                f"Cannot reach remote 'gdrive:large-files'.\n"
                f"  {result.stderr.strip()}\n\n"
                f"  Setup steps:\n"
                f"    1. Install rclone:  brew install rclone  (Mac)\n"
                f"                        sudo apt install rclone  (Linux)\n"
                f"    2. Run:  rclone config  — add a remote named 'gdrive'\n"
                f"    3. Make sure the Drive folder is shared with your Gmail\n"
                f"    4. Check the remote paths in SOURCES match your Drive layout"
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
    cfg       = SOURCES[source]
    local_dir = cfg["local"]
    found     = []
    for pattern in cfg["patterns"]:
        found.extend(sorted(local_dir.glob(pattern)))
    return found


def push_source(source: str, dry_run: bool = False) -> bool:
    local  = SOURCES[source]["local"]
    remote = SOURCES[source]["remote"]
    files  = collect_files(source)

    if not files:
        log.warning(f"  [{source}] No files found in {local} — skipping")
        return True

    total_mb = sum(f.stat().st_size for f in files) / 1_048_576
    log.info(f"  [{source}] {len(files)} file(s) ({total_mb:.1f} MB total) → {remote}")

    cmd = [
        "rclone", "sync",
        str(local),
        remote,
        "--update",
        "--transfers", "4",
        "--retries", "3",
        "--stats", "10s",
        "--progress",
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, timeout=3600)

    if result.returncode == 0:
        log.info(f"  ✓ [{source}] sync complete → {remote}")
        return True
    else:
        log.error(f"  ✗ [{source}] sync failed")
        return False


def pull_source(source: str, dry_run: bool = False) -> bool:
    remote = SOURCES[source]["remote"]
    dest   = SOURCES[source]["local"]
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


def pull_once(sources: list[str], dry_run: bool = False):
    for source in sources:
        pull_source(source, dry_run)


def watch_and_pull(sources: list[str], dry_run: bool = False):
    log.info(f"  Polling every {POLL_INTERVAL}s  |  Ctrl+C to stop\n")

    log.info("Running initial sync...")
    pull_once(sources, dry_run)

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            log.info("Checking for new files...")
            pull_once(sources, dry_run)
        except KeyboardInterrupt:
            log.info("\nStopped by user. Local data folders are up to date.")
            break
        except subprocess.TimeoutExpired:
            log.warning(f"Sync timed out — will retry in {POLL_INTERVAL}s.")
        except Exception as e:
            log.warning(f"Unexpected error: {e} — will retry in {POLL_INTERVAL}s.")


def main():
    parser = argparse.ArgumentParser(description="FinBot Data Sync")
    parser.add_argument("--pull",    action="store_true",
                        help="Pull from Drive to local. Default: push.")
    parser.add_argument("--watch",   action="store_true",
                        help="Keep polling for new files (use with --pull).")
    parser.add_argument("--source",  type=str, default=None,
                        help=f"Single source: {' | '.join(SOURCES)}")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview transfers without moving any files.")
    args = parser.parse_args()

    sources = list(SOURCES.keys())
    if args.source:
        if args.source not in SOURCES:
            log.error(f"Unknown source '{args.source}'. Choose from: {list(SOURCES)}")
            sys.exit(1)
        sources = [args.source]

    mode = "PULL (watch)" if (args.pull and args.watch) else "PULL" if args.pull else "PUSH"

    log.info("=" * 60)
    log.info(f"  FinBot Data Sync — {mode}")
    log.info(f"  Sources  : {', '.join(sources)}")
    log.info(f"  Remote   : gdrive:large-files/")
    log.info(f"  Dry run  : {args.dry_run}")
    log.info("=" * 60)

    if not check_rclone():
        sys.exit(1)

    if args.pull:
        if args.watch:
            watch_and_pull(sources, args.dry_run)
        else:
            results = {s: pull_source(s, args.dry_run) for s in sources}
            _print_summary(results, pulled=True)
    else:
        results = {s: push_source(s, args.dry_run) for s in sources}
        _print_summary(results, pulled=False)


def _print_summary(results: dict, pulled: bool):
    log.info(f"\n{'=' * 60}")
    log.info("  Summary")
    log.info(f"{'=' * 60}")
    for source, ok in results.items():
        log.info(f"  {source:<15} {'✓ OK' if ok else '✗ FAILED'}")

    if not all(results.values()):
        sys.exit(1)

    if pulled:
        log.info(f"\n  All data pulled to {ROOT_DIR / 'data'}/")
    else:
        log.info(f"\n  All data pushed to gdrive:large-files/")
        log.info("  Share these Drive folders with teammates:")
        log.info("    articles  → gdrive:large-files/cleaned")
        log.info("    wikipedia → gdrive:large-files/finbot/wikipedia")
        log.info("    books     → gdrive:large-files/finbot/books")
        log.info("  They pull with:  python rclone_sync.py --pull")
        log.info("  Or watch mode:   python rclone_sync.py --pull --watch")


if __name__ == "__main__":
    main()