"""
normalize_scraped_news.py
=========================
Cleans and normalizes a single scraped newspaper CSV for RAG ingestion.

Usage:
    python normalize_scraped_news.py <input.csv> <output.csv>
"""

import re
import sys
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MIN_WORD_COUNT    = 20    # Drop articles with fewer words after cleaning
MAX_NEWLINE_RATIO = 0.25  # Drop nav/menu junk (too many line breaks vs words)
MIN_ALPHA_RATIO   = 0.40  # Drop boilerplate (too few letter-characters)

# ─── PATTERNS ─────────────────────────────────────────────────────────────────

SCRAPER_FAILURE = re.compile(
    r"^(Failed:|Error:|Skipped:|Could not extract|Please wait|\.{3,}|nan$)",
    re.IGNORECASE,
)

NAV_LINE = re.compile(
    r"^(Home|News|Sports|Entertainment|Business|Tech|Science|Life|World|"
    r"Politics|Opinion|Weather|Search|Menu|Connect with us|Subscribe|"
    r"Sign in|Log in|Log out|Advertisement|More Posts|Page \d+ of \d+|"
    r"Click here|Read more|Share|Tweet|Follow us|All rights reserved|"
    r"Copyright ©?|Privacy Policy|Terms of (Use|Service)|Cookie|GDPR|"
    r"RSS|Newsletter)[\s\W]*$",
    re.IGNORECASE,
)

SENTENCE_END = re.compile(r"[.!?:]\s*$")

# Footer lines appended by scrapers/CMS systems — strip from end of articles
FOOTER_LINE = re.compile(
    r"("
    # Publication stamps: "Posted February 9, 2021 Source: ..."  /  "Published Feb 8, 2021 at 5:49 PM"
    r"(Posted|Published)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s*\d{4}"
    r"|"
    # "Cite this: ... - Medscape - Feb 08, 2021."
    r"Cite this:.{0,120}(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s*\d{4}"
    r"|"
    # "This report by X was first published Feb. 9, 2021."
    r"(This (report|article|story|release).{0,60}(published|appeared))"
    r"|"
    # "Source: Truetzschler Nonwovens ..."  /  "Source: Noticias ao Minuto Read More ..."
    r"^Source:\s"
    r"|"
    # Wire/copyright stamps: "Copyright Business Wire 2021."  /  "PUB: 02/10/2021 ..."
    r"^(Copyright\s.{0,60}\d{4}\.?$|PUB:\s*\d{2}\/\d{2}\/\d{4})"
    r"|"
    # "View source version on businesswire.com: ..."
    r"^View source version on "
    r"|"
    # Medscape/WebMD boilerplate
    r"Medscape Medical News © \d{4} WebMD"
    r"|"
    # "© 2023 AlleyWatch | All Rights Reserved"
    r"©\s*\d{4}\s+\w.{0,60}All Rights Reserved"
    r"|"
    # "PUBLICATION/CITATION HISTORY"
    r"^PUBLICATION/CITATION HISTORY"
    r"|"
    # "Comments" lone word at end (Medscape footer)
    r"^Comments$"
    r"|"
    # "For more X news, follow us on Twitter and Facebook."
    r"follow us on (Twitter|Facebook|Instagram|LinkedIn|social media)"
    r")",
    re.IGNORECASE,
)

# ─── TEXT CLEANING ────────────────────────────────────────────────────────────

def clean_text(raw: str) -> str:
    if not isinstance(raw, str):
        return ""

    text = raw.strip()

    if SCRAPER_FAILURE.match(text):
        return ""

    # Normalize unicode whitespace & zero-width chars
    text = re.sub(r"[\u00a0\u200b\u200c\u200d\u2028\u2029\ufeff]", " ", text)

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Strip navigation / UI chrome lines
    lines = [ln for ln in text.split("\n") if not NAV_LINE.match(ln.strip())]

    # Strip trailing footer lines (dates, source stamps, copyright, "Comments", etc.)
    # Walk backwards and drop matching lines until we hit real content
    while lines and (not lines[-1].strip() or FOOTER_LINE.search(lines[-1].strip())):
        lines.pop()

    # Collapse multiple consecutive blank lines → one
    collapsed, blank_streak = [], 0
    for ln in lines:
        if ln.strip() == "":
            blank_streak += 1
            if blank_streak == 1:
                collapsed.append("")
        else:
            blank_streak = 0
            collapsed.append(re.sub(r"[ \t]+", " ", ln).strip())

    # Merge broken mid-sentence lines back into full sentences
    merged, buffer = [], ""
    for ln in collapsed:
        if ln == "":
            if buffer:
                merged.append(buffer)
                buffer = ""
            merged.append("")
            continue
        if buffer:
            if SENTENCE_END.search(buffer):
                merged.append(buffer)
                buffer = ln
            else:
                buffer = buffer + " " + ln
        else:
            buffer = ln
    if buffer:
        merged.append(buffer)

    text = "\n".join(merged).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    paragraphs = [re.sub(r" {2,}", " ", p).strip() for p in text.split("\n\n")]
    paragraphs = [p for p in paragraphs if p]

    return "\n\n".join(paragraphs)


# ─── QUALITY FILTERS ──────────────────────────────────────────────────────────

def is_quality(text: str) -> bool:
    if not text:
        return False
    words = len(text.split())
    if words < MIN_WORD_COUNT:
        return False
    if text.count("\n") / words > MAX_NEWLINE_RATIO:
        return False
    alpha = sum(c.isalpha() for c in text)
    if alpha / len(text) < MIN_ALPHA_RATIO:
        return False
    return True


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def process(input_path: str, output_path: str):
    logging.info(f"Reading:  {input_path}")
    df = pd.read_csv(input_path, dtype=str)

    if "Article_Text" not in df.columns or "URL" not in df.columns:
        raise ValueError("CSV must have 'URL' and 'Article_Text' columns.")

    total = len(df)

    df["Article_Text"] = df["Article_Text"].apply(clean_text)
    df = df[df["Article_Text"].apply(is_quality)].copy()
    df["word_count"] = df["Article_Text"].str.split().str.len()

    if {"Year", "Month", "Day"}.issubset(df.columns):
        df["date"] = pd.to_datetime(
            df[["Year", "Month", "Day"]].rename(
                columns={"Year": "year", "Month": "month", "Day": "day"}
            ),
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")

    kept = len(df)
    logging.info(
        f"Done:     {total} rows in → {kept} kept, {total - kept} dropped "
        f"({(total - kept) / max(total, 1):.1%} removal)"
    )
    df.to_csv(output_path, index=False)
    logging.info(f"Saved:    {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python normalize_scraped_news.py <input.csv> <output.csv>")
        sys.exit(1)
    process(sys.argv[1], sys.argv[2])
