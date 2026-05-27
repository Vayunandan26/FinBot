import pandas as pd
import requests
from trafilatura import extract
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
import time
from pathlib import Path
import logging
logging.getLogger("trafilatura").setLevel(logging.ERROR)

# =============================================================================
# CONFIG
# =============================================================================

PIPELINE_DIR = Path(__file__).parent
ROOT_DIR     = PIPELINE_DIR.parent.parent               # one level above pipeline/

FILENAME     = ROOT_DIR / 'bigquery_article_results.csv'
BATCH_SIZE   = 11628
MAX_WORKERS  = 10
OUTPUT_DIR   = ROOT_DIR / 'batches'             
CLEANED_DIR  = ROOT_DIR / "data" / "cleaned_articles"     

def get_text_with_timeout(url, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                timeout=10,
                headers={'User-Agent': 'Mozilla/5.0'}
            )
            if response.status_code == 200:
                result = extract(response.text)
                return result if result else "Could not extract text"
            else:
                return f"Failed: Status {response.status_code}"

        except requests.exceptions.Timeout:
            return "Skipped: Connection Timeout"

        except Exception as e:
            if "getaddrinfo failed" in str(e) and attempt < retries - 1:
                time.sleep(2 ** attempt)  # wait 1s, 2s before retrying
                continue
            return f"Error: {str(e)}"


def batch_output_path(batch_number):
    return OUTPUT_DIR / f'scraped_batch_{batch_number}.csv'


def cleaned_output_path(batch_number):
    return CLEANED_DIR / f'scraped_batch_{batch_number}_cleaned.csv'


def is_batch_done(batch_number):
    """
    A batch is considered done if either:
    - The raw file still exists in batches/  (scraped, not yet cleaned)
    - The cleaned file exists in cleaned/    (scraped and already cleaned+deleted by pipeline)
    """
    return (
        batch_output_path(batch_number).exists() or
        cleaned_output_path(batch_number).exists()
    )


def run_batch(df, batch_number):
    start = batch_number * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(df))
    subset = df.iloc[start:end].copy()

    print(f"\nBatch {batch_number} — rows {start} to {end} ({len(subset)} URLs)...")

    urls = subset['URL'].tolist()
    results = [''] * len(urls)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_index = {executor.submit(get_text_with_timeout, url): i
                           for i, url in enumerate(urls)}

        count = 0
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                result = future.result()
                results[idx] = result if result is not None else "Could not extract text"
            except Exception as exc:
                results[idx] = f"Error: {exc}"
            count += 1
            if count % 100 == 0:
                print(f"  {count}/{len(urls)} done...")

    subset['Article_Text'] = results
    subset.to_csv(batch_output_path(batch_number), index=False)
    print(f"  Saved → {batch_output_path(batch_number)}")


def run_all():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FILENAME)
    total_batches = math.ceil(len(df) / BATCH_SIZE)

    done      = [b for b in range(total_batches) if is_batch_done(b)]
    remaining = [b for b in range(total_batches) if not is_batch_done(b)]

    print(f"Total URLs   : {len(df)}")
    print(f"Total batches: {total_batches}")
    print(f"Already done : {len(done)} batches {done if len(done) <= 10 else str(done[:10]) + '...'}")
    print(f"Remaining    : {len(remaining)} batches")

    if not remaining:
        print("\nAll batches already scraped. Nothing to do.")
        return

    for batch_number in remaining:
        run_batch(df, batch_number)


if __name__ == "__main__":
    run_all()