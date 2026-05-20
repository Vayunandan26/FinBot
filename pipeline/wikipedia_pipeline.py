"""
wikipedia_pipeline.py
=====================
Purpose-built pipeline for extracting, cleaning, and saving business-related
Wikipedia articles into a single CSV ready for RAG ingestion.

This is a completely separate pipeline from the news scraping pipeline.
It does not share schemas, batch files, or folder structures with batch_pipeline.py.

What it does:
    1. SCRAPE    — Fetches business article titles from Wikipedia's Category API
                   then streams matching articles from the HuggingFace dataset
    2. CLEAN     — Strips all non-content sections (References, See also,
                   External links, Further reading, Notes, Bibliography,
                   Categories, navbox lines, etc.)
    3. FILTER    — Drops articles that are too short, too sparse, or not
                   sufficiently business-relevant after cleaning
    4. SAVE      — Writes a single clean CSV to OUTPUT_PATH

Output schema:
    title        — article title
    url          — full Wikipedia URL
    source       — always "wikipedia"
    word_count   — word count after cleaning
    Article_Text — cleaned article body text

Usage:
    python wikipedia_pipeline.py

    # Dry run (process first 50,000 articles only, for testing):
    python wikipedia_pipeline.py --dry-run

Folder layout:
    FinBot/
        pipeline/
            wikipedia_pipeline.py    ← this file
        cleaned/
            wikipedia_business.csv   ← output (auto-created)

Requirements:
    pip install datasets huggingface_hub pandas requests
"""

import re
import time
import logging
import argparse
import requests
from pathlib import Path

import pandas as pd
from datasets import load_dataset

# =============================================================================
# CONFIG
# =============================================================================

PIPELINE_DIR = Path(__file__).parent
ROOT_DIR     = PIPELINE_DIR.parent
OUTPUT_PATH  = ROOT_DIR / "cleaned_wikipedia" / "wikipedia_business.csv"

WIKI_DATASET_VERSION = "20231101.en"    # update to latest monthly dump if needed

# Minimum quality thresholds (applied after cleaning)
# Raised from 100 → 300 to cut low-value stubs
MIN_WORD_COUNT  = 300    # drop articles shorter than this after cleaning
# Raised from 0.50 → 0.70 to cut list/table-heavy articles
MIN_ALPHA_RATIO = 0.70   # drop articles with too few letter-characters (lists/tables)

# How many articles to pull per category from the Wikipedia Category API.
# 3000 is enough to cover all major articles in each category without
# pulling every stub and disambiguation page.
MAX_PER_CATEGORY = 2000

# Wikipedia requires a descriptive User-Agent to avoid being blocked/throttled.
# See: https://www.mediawiki.org/wiki/API:Etiquette
WIKI_USER_AGENT = "FinBot/1.0 (wikipedia-business-pipeline; contact@example.com)"

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PIPELINE_DIR / "wikipedia_pipeline.log", encoding="utf-8"),
    ],
)

# =============================================================================
# STEP 1: SCRAPE — Get business article titles from Wikipedia Category API
# =============================================================================

# Balanced category list — core business/finance + precise sub-domains.
# Sector catch-alls (Retail, Manufacturing), broad institutions, and
# Economic history removed as they pull in too many tangential articles.
BUSINESS_CATEGORIES = [
    # Core
    "Category:Business",
    "Category:Finance",
    "Category:Economics",
    "Category:Macroeconomics",
    "Category:Microeconomics",
    "Category:Monetary economics",
    "Category:Financial economics",

    # Markets & investment
    "Category:Stock markets",
    "Category:Investment",
    "Category:Banking",
    "Category:Financial markets",
    "Category:Derivatives (finance)",
    "Category:Private equity",
    "Category:Venture capital",

    # Corporate & strategy
    "Category:Companies",
    "Category:Management",
    "Category:Corporate finance",
    "Category:Mergers and acquisitions",
    "Category:Entrepreneurship",
    "Category:Marketing",
    "Category:Accounting",

    # Trade & policy (precise sub-domains only)
    "Category:International trade",
    "Category:Financial regulation",
    "Category:Monetary policy",
    "Category:Fiscal policy",
]

# Keyword-based fallback filter — catches business articles that are not
# properly categorized under business categories on Wikipedia.
# Applied to title + first 800 chars of text.
BUSINESS_KEYWORDS = {
    "revenue", "profit", "earnings", "ebitda", "cash flow", "balance sheet",
    "market cap", "valuation", "ipo", "merger", "acquisition", "dividend",
    "stock", "equity", "bond", "yield", "gdp", "inflation", "interest rate",
    "federal reserve", "central bank", "monetary policy", "fiscal policy",
    "trade deficit", "supply chain", "venture capital", "private equity",
    "hedge fund", "bankruptcy", "restructuring", "layoffs", "regulation",
    "antitrust", "corporation", "startup", "entrepreneur", "shareholder",
    "accounting", "investment", "portfolio", "commodity", "real estate",
    "manufacturing", "retail", "banking", "insurance", "fintech",
    "cryptocurrency", "derivatives", "futures", "options trading",
    "economic growth", "recession", "unemployment", "consumer spending",
    "trade war", "tariff", "subsidy", "imf", "world bank", "wto", "oecd",
}


def get_category_members(category: str, max_articles: int = MAX_PER_CATEGORY) -> set:
    """
    Fetch article titles under a Wikipedia category using the MediaWiki API.
    Only fetches articles (cmtype=page), not subcategories.
    Handles pagination automatically via cmcontinue.
    """
    titles = set()
    url    = "https://en.wikipedia.org/w/api.php"
    headers = {"User-Agent": WIKI_USER_AGENT}
    params = {
        "action":  "query",
        "list":    "categorymembers",
        "cmtitle": category,
        "cmlimit": 500,         # max allowed per request by Wikipedia API
        "cmtype":  "page",      # articles only — skip subcategories and files
        "format":  "json",
    }

    retries = 0
    max_retries = 3

    while len(titles) < max_articles:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15).json()
            retries = 0  # reset on success
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                logging.warning(f"  API error for {category}: {e} — giving up after {max_retries} retries")
                break
            logging.warning(f"  API error for {category}: {e} — retry {retries}/{max_retries}")
            time.sleep(10 * (2 ** retries))  # exponential backoff: 20s, 40s
            continue

        for member in response.get("query", {}).get("categorymembers", []):
            titles.add(member["title"])

        if "continue" not in response:
            break
        params["cmcontinue"] = response["continue"]["cmcontinue"]
        time.sleep(0.3)     # polite delay — Wikipedia rate limit is generous but respect it

    return titles


def collect_business_titles() -> set:
    """
    Pull article titles from all BUSINESS_CATEGORIES via the Wikipedia API.
    Returns a deduplicated set of titles.
    """
    logging.info("=" * 55)
    logging.info("STEP 1: Collecting business article titles from Wikipedia categories")
    logging.info("=" * 55)

    all_titles = set()
    for i, category in enumerate(BUSINESS_CATEGORIES, 1):
        titles = get_category_members(category)
        logging.info(f"  [{i:02d}/{len(BUSINESS_CATEGORIES)}] {category}: {len(titles):,} articles")
        all_titles.update(titles)
        time.sleep(1.0)   # pause between categories to avoid rate limiting

    logging.info(f"\n  Total unique titles collected: {len(all_titles):,}\n")
    return all_titles


# =============================================================================
# STEP 2: CLEAN — Strip non-content sections from raw Wikipedia text
# =============================================================================

# These are the section headers that mark the end of real content.
# Everything from the first match of any of these onwards is stripped.
# Order matters — "See also" typically comes before "References".
NON_CONTENT_SECTION_HEADERS = re.compile(
    r"^==+\s*("
    r"See also"
    r"|References"
    r"|Further reading"
    r"|External links"
    r"|Notes"
    r"|Bibliography"
    r"|Footnotes"
    r"|Citations"
    r"|Sources"
    r"|Works cited"
    r"|Related pages"
    r"|Read more"
    r"|Navigation menu"
    r"|Contents"
    r"|Appendix"
    r"|Appendices"
    r"|Index"
    r")\s*==+\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Lines that are navigation/UI chrome even inside content sections
NAV_LINE = re.compile(
    r"^("
    r"Categories\s*:"           # category line at bottom
    r"|Retrieved from"          # Wikipedia retrieval notice
    r"|This page was last"      # edit timestamp
    r"|Wikipedia®"              # Wikipedia trademark line
    r"|Text is available"       # license notice
    r"|Privacy policy"
    r"|About Wikipedia"
    r"|Disclaimers"
    r"|Contact Wikipedia"
    r"|Mobile view"
    r"|Developers"
    r"|Cookie statement"
    r"|\{\{.*\}\}"              # remaining template markers e.g. {{reflist}}
    r"|\[\[.*\]\]"              # bare wikilinks that weren't cleaned
    r")",
    re.IGNORECASE,
)

# Lines that are pure list markers with no prose content
BARE_LIST_LINE = re.compile(r"^\*\s*\[\[.+?\]\]\s*$")      # * [[Article name]]
SECTION_HEADER = re.compile(r"^==+\s*.+?\s*==+\s*$")        # any == Header ==


def strip_non_content_sections(text: str) -> str:
    """
    Remove everything from the first non-content section header onwards.
    Handles both wiki-markup headers (== See also ==) and plain-text
    headers as delivered by the HuggingFace dataset (See also\n...).
    Also strips interwiki links (e.g. fi:Laskentatoimi) and trailing
    bare category tag lines at the end of articles.
    """
    # 1. Wiki-markup style headers  (== See also ==)
    match = NON_CONTENT_SECTION_HEADERS.search(text)
    if match:
        text = text[:match.start()]

    # 2. Plain-text section headers as delivered by HuggingFace parquet
    PLAIN_FOOTER = re.compile(
        r"\n\n("
        r"See also"
        r"|References"
        r"|Further reading"
        r"|External links"
        r"|Notes"
        r"|Bibliography"
        r"|Footnotes"
        r"|Citations"
        r"|Sources"
        r"|Works cited"
        r"|Related pages"
        r")\n.*",
        re.IGNORECASE | re.DOTALL,
    )
    text = PLAIN_FOOTER.sub("", text)

    # 3. Interwiki links at the end  e.g. "fi:Laskentatoimi"
    text = re.sub(r"\n+[a-z]{2,3}:[^\n]+$", "", text, flags=re.MULTILINE)

    # 4. Trailing bare category/tag lines (short lines, no punctuation)
    lines = text.rstrip().split("\n")
    while lines:
        last = lines[-1].strip()
        if last and re.match(r"^[A-Za-z][a-zA-Z\s]{0,49}$", last) \
                and "." not in last and len(last) < 50:
            lines.pop()
        else:
            break

    return "\n".join(lines).strip()


def clean_article(text: str) -> str:
    """
    Full cleaning pass on a single Wikipedia article's text field.

    Steps:
        1. Strip non-content sections (References, See also, etc.)
        2. Remove navigation/UI chrome lines
        3. Remove bare wikilink list lines
        4. Normalize section headers (== Header == → Header)
        5. Normalize unicode whitespace
        6. Collapse excess blank lines
        7. Strip leading/trailing whitespace
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    # 1. Cut off at first non-content section
    text = strip_non_content_sections(text)

    # 2. Normalize unicode whitespace & zero-width chars
    text = re.sub(r"[\u00a0\u200b\u200c\u200d\u2028\u2029\ufeff]", " ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = text.split("\n")
    cleaned_lines = []

    for line in lines:
        stripped = line.strip()

        # Skip empty lines (handle below with collapse)
        if not stripped:
            cleaned_lines.append("")
            continue

        # Skip nav/UI chrome lines
        if NAV_LINE.match(stripped):
            continue

        # Skip bare wikilink list lines like "* [[Deflation]]"
        if BARE_LIST_LINE.match(stripped):
            continue

        # Normalize section headers: "== Causes ==" → "Causes"
        if SECTION_HEADER.match(stripped):
            header_text = re.sub(r"^=+\s*", "", stripped)
            header_text = re.sub(r"\s*=+$", "", header_text).strip()
            if header_text:
                cleaned_lines.append(f"\n{header_text}")
            continue

        # Clean inline wikilink syntax: [[Target|Display]] → Display, [[Target]] → Target
        stripped = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]", r"\1", stripped)

        # Remove citation markers like [1], [2], [citation needed]
        stripped = re.sub(r"\[\d+\]", "", stripped)
        stripped = re.sub(r"\[citation needed\]", "", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"\[note \d+\]", "", stripped, flags=re.IGNORECASE)

        # Remove any remaining template markers {{...}}
        stripped = re.sub(r"\{\{[^}]*\}\}", "", stripped)

        # Collapse internal whitespace
        stripped = re.sub(r"[ \t]+", " ", stripped).strip()

        if stripped:
            cleaned_lines.append(stripped)

    # Collapse multiple consecutive blank lines → one blank line
    collapsed = []
    prev_blank = False
    for line in cleaned_lines:
        is_blank = line.strip() == ""
        if is_blank:
            if not prev_blank:
                collapsed.append("")
            prev_blank = True
        else:
            collapsed.append(line)
            prev_blank = False

    text = "\n".join(collapsed).strip()

    # Final pass: remove any remaining lines that are clearly just punctuation/symbols
    final_lines = []
    for line in text.split("\n"):
        s = line.strip()
        if not s:
            final_lines.append("")
            continue
        alpha_count = sum(c.isalpha() for c in s)
        # Keep if at least 40% of the line's non-space characters are letters
        if len(s.replace(" ", "")) == 0 or alpha_count / max(len(s.replace(" ", "")), 1) >= 0.40:
            final_lines.append(line)

    return "\n".join(final_lines).strip()


# =============================================================================
# STEP 3: FILTER — Quality gates after cleaning
# =============================================================================

def is_quality_article(text: str) -> bool:
    """
    Returns True if the cleaned article passes all quality thresholds.
    Drops:
        - Empty or near-empty articles
        - Articles too short to be useful for RAG (< MIN_WORD_COUNT words)
        - Articles that are mostly lists/tables (low alpha ratio)
        - Disambiguation pages
        - Redirect stubs
    """
    if not text:
        return False

    # Drop disambiguation pages — they contain no real content
    if re.search(r"\bmay refer to\b|\bdisambiguation\b", text[:300], re.IGNORECASE):
        return False

    # Drop redirect stubs
    if re.match(r"^#redirect", text, re.IGNORECASE):
        return False

    words = text.split()
    if len(words) < MIN_WORD_COUNT:
        return False

    # Check alpha ratio — articles that are mostly bullet lists have low ratios
    alpha = sum(c.isalpha() for c in text)
    if len(text) == 0 or alpha / len(text) < MIN_ALPHA_RATIO:
        return False

    return True


def is_business_relevant(title: str, text: str) -> bool:
    """
    Secondary business relevance check — catches articles pulled by category
    that turned out to be tangentially related after cleaning.
    Checks title + first 800 chars of cleaned text.
    """
    combined = (title + " " + text[:800]).lower()
    return any(keyword in combined for keyword in BUSINESS_KEYWORDS)


# =============================================================================
# STEP 4: STREAM + PROCESS from HuggingFace
# =============================================================================

def stream_and_process(target_titles: set, dry_run: bool = False) -> list[dict]:
    """
    Stream the HuggingFace Wikipedia dataset, match against target_titles,
    clean each matched article, and return a list of clean article dicts.

    dry_run: if True, stops after checking 50,000 articles (for testing).
    """
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

    results        = []
    checked        = 0
    matched        = 0
    dropped_quality= 0
    dropped_biz    = 0

    for article in dataset:
        checked += 1

        title    = article.get("title", "")
        raw_text = article.get("text", "")
        url      = article.get("url", f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}")

        # Match by title only (category API titles are precise Wikipedia classifications).
        # Keyword fallback removed — it inflates results with tangentially related articles.
        title_match = title in target_titles

        if not title_match:
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

        # Clean the article
        cleaned = clean_article(raw_text)

        # Quality filter
        if not is_quality_article(cleaned):
            dropped_quality += 1
            continue

        # Business relevance filter (catches borderline matches)
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

        # Early exit — no point scanning further once all targets are found
        if len(results) >= len(target_titles):
            logging.info("  All target titles found — stopping stream early.")
            break

    logging.info(
        f"\n  Stream complete.\n"
        f"  Total articles checked : {checked:,}\n"
        f"  Category/keyword match : {matched:,}\n"
        f"  Dropped (quality)      : {dropped_quality:,}\n"
        f"  Dropped (not business) : {dropped_biz:,}\n"
        f"  Final kept             : {len(results):,}\n"
    )

    return results


# =============================================================================
# STEP 5: SAVE — Write clean CSV
# =============================================================================

def save_csv(articles: list[dict], output_path: Path):
    """
    Save the cleaned articles to a CSV file.
    Creates the output directory if it doesn't exist.
    """
    logging.info("=" * 55)
    logging.info("STEP 3: Saving to CSV")
    logging.info("=" * 55)

    if not articles:
        logging.warning("No articles to save — output file not created.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(articles)

    # Reorder columns cleanly
    df = df[["title", "url", "source", "Article_Text", "word_count"]]

    # Final deduplication by title (in case category + keyword both matched)
    before = len(df)
    df = df.drop_duplicates(subset=["title"]).reset_index(drop=True)
    dupes = before - len(df)
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


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Wikipedia business article pipeline for FinBot")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process only the first 50,000 Wikipedia articles (for testing)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_PATH),
        help=f"Output CSV path (default: {OUTPUT_PATH})"
    )
    args = parser.parse_args()

    output_path = Path(args.output)

    logging.info("\n" + "=" * 55)
    logging.info("  Wikipedia Business Pipeline — FinBot")
    logging.info(f"  Output : {output_path}")
    logging.info(f"  Mode   : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    logging.info("=" * 55 + "\n")

    # Step 1: Collect target titles from Wikipedia Category API
    target_titles = collect_business_titles()

    # Step 2 + 3: Stream dataset, clean, filter
    articles = stream_and_process(target_titles, dry_run=args.dry_run)

    # Step 4: Save
    save_csv(articles, output_path)

    logging.info("\nPipeline complete.")


if __name__ == "__main__":
    main()