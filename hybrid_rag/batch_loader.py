"""
hybrid_rag/graph_batch_loader.py
=================================
Fetches completed OpenAI batch results and loads extracted Knowledge Graph
triplets directly into Neo4j.

Usage:
    python graph_batch_loader.py --batch_id <batch_job_id>
"""

import os
import json
import argparse
from openai import OpenAI
from langchain_neo4j import Neo4jGraph

# Neo4j Connection settings (Read from env or defaults)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def check_and_download_batch(batch_id: str, output_file: str = "batch_results.jsonl") -> str:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    print(f"Checking status for Batch Job: {batch_id}...")
    batch_status = client.batches.retrieve(batch_id)

    print(f"Current Batch Status: {batch_status.status}")

    if batch_status.status != "completed":
        raise RuntimeError(
            f"Batch job is not completed yet! Current status: '{batch_status.status}'. "
            f"Please wait and retry later."
        )

    output_file_id = batch_status.output_file_id
    print(f"Downloading batch output file: {output_file_id}...")

    content = client.files.content(output_file_id)

    with open(output_file, "wb") as f:
        f.write(content.read())

    print(f"✅ Saved results to {output_file}")
    return output_file


def ingest_to_neo4j(results_file: str):
    print(f"Connecting to Neo4j database at {NEO4J_URI}...")
    graph = Neo4jGraph(
        url=NEO4J_URI,
        username=NEO4J_USERNAME,
        password=NEO4J_PASSWORD
    )

    # Cypher queries for merging entities and relations
    MERGE_ENTITY_CYPHER = """
    MERGE (e:Entity {name: $name})
    ON CREATE SET e.type = $type
    """

    MERGE_RELATION_CYPHER = """
    MATCH (source:Entity {name: $source})
    MATCH (target:Entity {name: $target})
    MERGE (source)-[r:RELATION {type: $rel_type}]->(target)
    """

    entities_count = 0
    relations_count = 0

    print("Ingesting extracted graph data into Neo4j...")
    with open(results_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            item = json.loads(line)
            response_body = item.get("response", {}).get("body", {})
            choices = response_body.get("choices", [])

            if not choices:
                continue

            content_str = choices[0].get("message", {}).get("content", "")
            try:
                extracted_data = json.loads(content_str)
            except Exception:
                continue

            # Load entities
            entities = extracted_data.get("entities", [])
            for ent in entities:
                ent_id = ent.get("id")
                ent_type = ent.get("type", "Entity")
                if ent_id:
                    graph.query(MERGE_ENTITY_CYPHER, params={"name": ent_id, "type": ent_type})
                    entities_count += 1

            # Load relationships
            relationships = extracted_data.get("relationships", [])
            for rel in relationships:
                source = rel.get("source")
                target = rel.get("target")
                rel_type = rel.get("type", "RELATED_TO")
                if source and target:
                    graph.query(
                        MERGE_RELATION_CYPHER,
                        params={"source": source, "target": target, "rel_type": rel_type}
                    )
                    relations_count += 1

    print("\n" + "=" * 60)
    print("🎉 Knowledge Graph Ingestion Complete!")
    print(f"Entities merged    : {entities_count}")
    print(f"Relationships merged: {relations_count}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="FinBot Graph Batch Loader")
    parser.add_argument("--batch_id", type=str, required=True, help="OpenAI Batch Job ID")
    parser.add_argument("--local_file", type=str, default=None, help="Optional: local batch_results.jsonl if already downloaded")

    args = parser.parse_args()

    if args.local_file and os.path.exists(args.local_file):
        results_path = args.local_file
    else:
        results_path = check_and_download_batch(args.batch_id)

    ingest_to_neo4j(results_path)


if __name__ == "__main__":
    main()