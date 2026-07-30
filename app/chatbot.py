"""
hybrid_rag/chatbot.py
======================
Core Hybrid RAG Engine integrating:
- Redis (Short-lived conversation memory with TTL)
- ChromaDB (Vector Search)
- Neo4j (Knowledge Graph Traversal)
- OpenAI GPT via LangChain
"""

import os
import json
import redis
from dotenv import load_dotenv

from langchain_community.chat_models import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph

load_dotenv()

# Configuration
CHROMA_PERSIST_DIR = "../chroma_db"
COLLECTION_NAME = "finbot_newspapers"

# Redis Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
DEFAULT_TTL = int(os.getenv("REDIS_TTL_SECONDS", 3600))  # Short-lived memory default: 1 hour

# Neo4j Config
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


class RedisMemoryManager:
    """Manages user-isolated short-lived chat history in Redis with auto-expiring TTL."""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, ttl=DEFAULT_TTL):
        self.ttl = ttl
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                password=password,
                decode_responses=True,
                socket_connect_timeout=3
            )
            self.client.ping()
            self.is_connected = True
        except Exception as e:
            print(f"⚠️ Redis connection failed ({e}). Proceeding with stateless memory.")
            self.is_connected = False

    def _get_key(self, username: str) -> str:
        return f"finbot:memory:{username.strip().lower()}"

    def has_active_session(self, username: str) -> bool:
        if not self.is_connected:
            return False
        return bool(self.client.exists(self._get_key(username)))

    def get_history(self, username: str, limit: int = 6) -> str:
        if not self.is_connected:
            return "No previous conversation history available."

        key = self._get_key(username)
        raw_messages = self.client.lrange(key, -limit, -1)

        if not raw_messages:
            return "No previous conversation history."

        formatted_msgs = []
        for msg_str in raw_messages:
            try:
                data = json.loads(msg_str)
                formatted_msgs.append(f"{data['role']}: {data['content']}")
            except json.JSONDecodeError:
                continue

        # Refresh TTL on session access
        self.client.expire(key, self.ttl)
        return "\n".join(formatted_msgs)

    def add_message(self, username: str, role: str, content: str):
        if not self.is_connected:
            return

        key = self._get_key(username)
        payload = json.dumps({"role": role, "content": content})

        self.client.rpush(key, payload)
        # Reset short-lived memory TTL timer on every turn
        self.client.expire(key, self.ttl)


class HybridRAGChatbot:
    """Hybrid RAG Engine combining Vector, Graph, and Redis Session Memory."""

    def __init__(self):
        print("⚙️ Initializing FinBot Core Engine...")

        # 1. Initialize Redis Memory
        self.memory = RedisMemoryManager()

        # 2. Connect OpenAI LLM
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("⚠️ OPENAI_API_KEY environment variable is missing!")

        self.llm = ChatOpenAI(
            model_name="gpt-4o-mini",
            temperature=0.2,
            openai_api_key=api_key,
        )

        # 3. Connect Vector DB
        print("  Loading Embeddings & ChromaDB...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="BAAI/bge-large-en-v1.5",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True},
        )

        self.vector_store = Chroma(
            collection_name=COLLECTION_NAME,
            embedding_function=self.embeddings,
            persist_directory=CHROMA_PERSIST_DIR,
        )

        # 4. Connect Neo4j Graph DB
        print("  Connecting to Neo4j Graph DB...")
        try:
            self.graph_store = Neo4jGraph(
                url=NEO4J_URI,
                username=NEO4J_USERNAME,
                password=NEO4J_PASSWORD,
            )
            self.graph_enabled = True
            print("  ✅ Neo4j connected.")
        except Exception as e:
            print(f"  ⚠️ Could not connect to Neo4j ({e}). Falling back to Vector + Memory.")
            self.graph_enabled = False

        # 5. Build Prompt Template with Short-Lived Chat History
        self.prompt_template = ChatPromptTemplate.from_template("""
You are FinBot, an expert financial intelligence assistant. 
Answer the user's question accurately using the provided Short-Lived Conversation History, Vector Search Context, and Knowledge Graph Relationships.

=== SHORT-LIVED SESSION HISTORY ===
{chat_history}

=== VECTOR RETRIEVAL CONTEXT ===
{vector_context}

=== KNOWLEDGE GRAPH RELATIONSHIPS ===
{graph_context}

=== CURRENT QUESTION ===
{query}

=== ANSWER ===
""")

    def retrieve_vector_context(self, query: str, top_k: int = 3) -> str:
        results = self.vector_store.similarity_search(query, k=top_k)
        context_chunks = []
        for i, doc in enumerate(results, 1):
            source = doc.metadata.get("filename", "Unknown")
            context_chunks.append(f"[Vector Doc {i} | Source: {source}]\n{doc.page_content}")
        return "\n\n".join(context_chunks) if context_chunks else "No vector context found."

    def retrieve_graph_context(self, query: str) -> str:
        if not self.graph_enabled:
            return "No Graph Context available."

        cypher = """
        MATCH (e:Entity)
        WHERE toLower(e.name) IN [token in split(toLower($query), ' ') | token]
           OR any(word IN split(toLower($query), ' ') WHERE toLower(e.name) CONTAINS word)
        MATCH (e)-[r]->(target)
        RETURN e.name AS Source, r.type AS Relation, target.name AS Target
        LIMIT 10
        """
        try:
            records = self.graph_store.query(cypher, params={"query": query})
            if not records:
                return "No explicit graph relationships found."

            graph_lines = [f"({rec['Source']}) -[{rec['Relation']}]-> ({rec['Target']})" for rec in records]
            return "\n".join(graph_lines)
        except Exception as e:
            return f"Graph retrieval error: {e}"

    def ask(self, username: str, query: str) -> str:
        # Fetch user's short-lived conversation memory from Redis
        history_str = self.memory.get_history(username)

        # Retrieve Contexts
        vector_ctx = self.retrieve_vector_context(query)
        graph_ctx = self.retrieve_graph_context(query)

        # Format prompt & generate answer
        messages = self.prompt_template.format_messages(
            chat_history=history_str,
            vector_context=vector_ctx,
            graph_context=graph_ctx,
            query=query,
        )

        response = self.llm.invoke(messages)
        answer_text = response.content.strip()

        # Save turn to Redis short-lived session memory
        self.memory.add_message(username, "User", query)
        self.memory.add_message(username, "FinBot", answer_text)

        return answer_text