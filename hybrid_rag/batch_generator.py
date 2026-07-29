"""
hybrid_rag/graph_batch_generator.py
====================================
Generates JSONL batch request files for OpenAI Batch API to extract 
Knowledge Graph triplets (Entities and Relationships) at 50% lower cost.

Usage:
    python graph_batch_generator.py --input_dir ../data/cleaned_articles --output_jsonl batch_requests.jsonl
    python graph_batch_generator.py --submit batch_requests.jsonl
"""

import os
import json
import argparse
from pathlib import Path
import pandas as pd
from openai import OpenAI
from langchain_text_splitters import TokenTextSplitter

# Model to use for entity/relationship extraction in batch
MODEL_NAME = "gpt-4o-mini"

SYSTEM_PROMPT = """You are a financial knowledge graph extraction system. 
Analyze the input financial text and extract key entities and relationships.
Return ONLY a JSON object with this exact schema:
{
  "entities": [
    {"id": "Entity Name", "type": "Company|Person|Market|Indicator|Concept|Location"}
  ],
  "relationships": [
    {"source": "Entity Name", "target": "Entity Name", "type": "RELATION_TYPE"}
  ]
}
Use uppercase with underscores for relation types (e.g. ACQUIRED, AFFECTS_INFLATION, CEO_OF, INVESTED_IN, IMPORTS_FROM).
"""


def create_batch_file(input_dir: str, output_jsonl: str, chunk_size: int = 400):
    input_path = Path(input_dir)
    text_splitter = TokenTextSplitter(chunk_size=chunk_size, chunk_overlap=50)
    requests = []
    custom_id_counter = 0

    print(f"Reading CSV files from: {input_dir}")
    csv_files = list(input_path.rglob("*.csv"))

    for csv_file in csv_files:
        print(f"Processing: {csv_file.name}")
        try:
            df = pd.read_csv(csv_file)
            if "Article_Text" not in df.columns:
                continue

            df = df.dropna(subset=["Article_Text"])
            for idx, row in df.iterrows():
                text = str(row["Article_Text"]).strip()
                if len(text.split()) < 50:
                    continue

                chunks = text_splitter.split_text(text)
                for chunk_idx, chunk in enumerate(chunks[:2]):  # Limit chunks per article for batching
                    custom_id_counter += 1
                    custom_id = f"req_{csv_file.stem}_{idx}_{chunk_idx}_{custom_id_counter}"

                    payload = {
                        "custom_id": custom_id,
                        "method": "POST",
                        "url": "/v1/chat/completions",
                        "body": {
                            "model": MODEL_NAME,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": SYSTEM_PROMPT},
                                {"role": "user", "content": f"Text chunk:\n{chunk}"}
                            ],
                            "temperature": 0.0
                        }
                    }
                    requests.append(payload)

        except Exception as e:
            print(f"Error reading {csv_file}: {e}")

    print(f"Generated {len(requests)} batch request items.")
    with open(output_jsonl, "w", encoding="utf-8") as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")

    print(f"✅ Batch request file written to: {output_jsonl}")


def submit_batch_job(jsonl_file: str):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    print(f"Uploading {jsonl_file} to OpenAI...")
    with open(jsonl_file, "rb") as f:
        batch_input_file = client.files.create(
            file=f,
            purpose="batch"
        )

    print(f"File uploaded. File ID: {batch_input_file.id}")
    print("Creating batch job...")

    batch_job = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "FinBot Graph DB Batch Extraction"}
    )

    print("\n" + "=" * 60)
    print("🚀 Batch Job Submitted Successfully!")
    print(f"Batch Job ID : {batch_job.id}")
    print(f"Status       : {batch_job.status}")
    print(f"Input File ID: {batch_job.input_file_id}")
    print("=" * 60)
    print("\nSave this Batch Job ID to run graph_batch_loader.py when completed.")


def main():
    parser = argparse.ArgumentParser(description="FinBot Graph Batch Generator")
    parser.add_argument("--input_dir", type=str, default="../data/cleaned_articles", help="Path to input data CSVs")
    parser.add_argument("--output_jsonl", type=str, default="batch_requests.jsonl", help="Output JSONL payload path")
    parser.add_argument("--submit", action="store_true", help="Submit the generated JSONL to OpenAI Batch API")

    args = parser.parse_args()

    if not Path(args.output_jsonl).exists() and not args.submit:
        create_batch_file(args.input_dir, args.output_jsonl)
    elif args.submit:
        if not Path(args.output_jsonl).exists():
            create_batch_file(args.input_dir, args.output_jsonl)
        submit_batch_job(args.output_jsonl)


if __name__ == "__main__":
    main()