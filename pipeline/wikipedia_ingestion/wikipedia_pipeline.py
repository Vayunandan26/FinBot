import logging
import argparse
from pathlib import Path

import pandas as pd
from datasets import load_dataset

from wikipedia_scrape import collect_business_titles, BUSINESS_KEYWORDS
from wikipedia_clean import clean_article, is_quality_article

PIPELINE_DIR         = Path(__file__).parent
ROOT_DIR             = PIPELINE_DIR.parent
OUTPUT_PATH          = ROOT_DIR / "data" / "cleaned_wikipedia" / "wikipedia_business.csv"
WIKI_DATASET_VERSION = "20231101.en"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PIPELINE_DIR / "wikipedia_pipeline.log", encoding="utf-8"),
    ],
)


def is_business_relevant(title: str, text: str) -> bool:
    combined = (title + " " + text[:800]).lower()
    return any(keyword in combined for keyword in BUSINESS_KEYWORDS)


def stream_and_process(target_titles: set, dry_run: bool = False) -> list[dict]:
    logging.info("=" * 55)
    logging.info("STEP 2: Streaming Wikipedia dataset from HuggingFace")
    logging.info(f"  Matching against {len(target_titles):,} target titles")
    if dry_run:
        logging.info("  DRY RUN MODE — will stop after 50,000 articles checked")
    logging.info("=" * 55)

    dataset = load_dataset(
        "wikimedia/wikipedia",
        WIKI_DATASET_VERSION,
        split="train",
        streaming=True,
    )

    results         = []
    checked         = 0
    matched         = 0
    dropped_quality = 0
    dropped_biz     = 0

    for article in dataset:
        checked  += 1
        title     = article.get("title", "")
        raw_text  = article.get("text", "")
        url       = article.get("url", f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}")

        if title not in target_titles:
            if checked % 100_000 == 0:
                logging.info(
                    f"  Checked {checked:,} | Matched {matched:,} | "
                    f"Kept {len(results):,} | "
                    f"Dropped quality={dropped_quality} biz={dropped_biz}"
                )
            if dry_run and checked >= 50_000:
                break
            continue

        matched += 1
        cleaned  = clean_article(raw_text)

        if not is_quality_article(cleaned):
            dropped_quality += 1
            continue

        if not is_business_relevant(title, cleaned):
            dropped_biz += 1
            continue

        results.append({
            "title":        title,
            "url":          url,
            "source":       "wikipedia",
            "word_count":   len(cleaned.split()),
            "Article_Text": cleaned,
        })

        if checked % 100_000 == 0:
            logging.info(
                f"  Checked {checked:,} | Matched {matched:,} | "
                f"Kept {len(results):,} | "
                f"Dropped quality={dropped_quality} biz={dropped_biz}"
            )

        if dry_run and checked >= 50_000:
            logging.info("  Dry run limit reached — stopping.")
            break

        if len(results) >= len(target_titles):
            logging.info("  All target titles found — stopping stream early.")
            break

    logging.info(
        f"\n  Stream complete.\n"
        f"  Total articles checked : {checked:,}\n"
        f"  Category match         : {matched:,}\n"
        f"  Dropped (quality)      : {dropped_quality:,}\n"
        f"  Dropped (not business) : {dropped_biz:,}\n"
        f"  Final kept             : {len(results):,}\n"
    )

    return results


def save_csv(articles: list[dict], output_path: Path):
    logging.info("=" * 55)
    logging.info("STEP 3: Saving to CSV")
    logging.info("=" * 55)

    if not articles:
        logging.warning("No articles to save — output file not created.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df     = pd.DataFrame(articles)
    df     = df[["title", "url", "source", "Article_Text", "word_count"]]
    before = len(df)
    df     = df.drop_duplicates(subset=["title"]).reset_index(drop=True)
    dupes  = before - len(df)

    if dupes:
        logging.info(f"  Removed {dupes:,} duplicate articles")

    df.to_csv(output_path, index=False, encoding="utf-8")

    logging.info(f"  Saved {len(df):,} articles → {output_path}")
    logging.info(f"  File size: {output_path.stat().st_size / 1_048_576:.1f} MB")
    logging.info(
        f"  Word count stats:\n"
        f"    Min    : {df['word_count'].min():,}\n"
        f"    Median : {int(df['word_count'].median()):,}\n"
        f"    Mean   : {int(df['word_count'].mean()):,}\n"
        f"    Max    : {df['word_count'].max():,}"
    )


def main():
    parser = argparse.ArgumentParser(description="Wikipedia business article pipeline for FinBot")
    parser.add_argument("--dry-run", action="store_true",
                        help="Process only the first 50,000 Wikipedia articles (for testing)")
    parser.add_argument("--output", type=str, default=str(OUTPUT_PATH),
                        help=f"Output CSV path (default: {OUTPUT_PATH})")
    args = parser.parse_args()

    output_path = Path(args.output)

    logging.info("\n" + "=" * 55)
    logging.info("  Wikipedia Business Pipeline — FinBot")
    logging.info(f"  Output : {output_path}")
    logging.info(f"  Mode   : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    logging.info("=" * 55 + "\n")

    target_titles = collect_business_titles()
    articles      = stream_and_process(target_titles, dry_run=args.dry_run)
    save_csv(articles, output_path)

    logging.info("\nPipeline complete.")


if __name__ == "__main__":
    main()