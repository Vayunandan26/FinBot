import pandas as pd
import requests
from trafilatura import extract
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
import time


FILENAME = '/Users/vayunandan/rag/bigquery_article_results.csv'
BATCH_SIZE = 11628
MAX_WORKERS = 10
OUTPUT_DIR = 'batches'  


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
    return os.path.join(OUTPUT_DIR, f'scraped_batch_{batch_number}.csv')

def is_batch_done(batch_number):
    return os.path.exists(batch_output_path(batch_number))

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
    
def merge_batches(total_batches):
    files = [batch_output_path(b) for b in range(total_batches)]

    missing = [f for f in files if not os.path.exists(f)]
    if missing:
        print(f"Merge aborted — {len(missing)} file/s missing.")
        return

    print(f"Merging {len(files)} batch files...")

    df = pd.concat(
        [pd.read_csv(f) for f in files],
        ignore_index=True
    )

    output_path = 'merged_articles.csv'
    df.to_csv(output_path, index=False)
    print(f"Done! {len(df)} rows saved → {output_path}")

def run_all():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(FILENAME)
    total_batches = math.ceil(len(df) / BATCH_SIZE)

    done = [b for b in range(total_batches) if is_batch_done(b)]
    remaining = [b for b in range(total_batches) if not is_batch_done(b)]

    print(f"Total URLs   : {len(df)}")
    print(f"Total batches: {total_batches}")
    print(f"Already done : {len(done)} batches {done if len(done) <= 10 else str(done[:10]) + '...'}")
    print(f"Remaining    : {len(remaining)} batches")

    if not remaining:
        print("\nAll batches already complete. Nothing to do.")
        merge_batches(total_batches)   # all done, safe to merge
        return

    for batch_number in remaining:
        run_batch(df, batch_number)

    # verify every batch exists before merging
    all_done = all(is_batch_done(b) for b in range(total_batches))
    if all_done:
        print("\nAll batches scraped. Starting merge...")
        merge_batches(total_batches)
    else:
        missing = [b for b in range(total_batches) if not is_batch_done(b)]
        print(f"\nMerge skipped — {len(missing)} batches still missing: {missing}")

if __name__ == "__main__":
    run_all()