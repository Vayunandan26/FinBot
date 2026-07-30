"""
hybrid_rag/main.py
==================
Main Interactive CLI for FinBot. Prompts for a unique username to bind 
short-lived Redis memory before starting the hybrid RAG chat.

Run:
    python main.py
"""

import sys
from chatbot import HybridRAGChatbot


def prompt_unique_username(chatbot: HybridRAGChatbot) -> str:
    print("=" * 60)
    print("🤖 Welcome to FinBot Hybrid RAG Intelligence System")
    print("=" * 60)

    while True:
        username = input("\n👉 Enter your unique username to start session: ").strip()

        if not username:
            print("⚠️ Username cannot be empty. Please enter a valid name/ID.")
            continue

        if len(username) < 3:
            print("⚠️ Username must be at least 3 characters long.")
            continue

        # Check Redis for active session memory
        if chatbot.memory.has_active_session(username):
            print(f"🔄 Welcome back, [{username}]! Active short-lived Redis session restored.")
        else:
            print(f"✨ Session initialized for user [{username}]. Short-lived Redis memory enabled.")

        return username


def main():
    try:
        # Initialize Core Chatbot Engine
        chatbot = HybridRAGChatbot()
    except Exception as e:
        print(f"❌ Failed to initialize chatbot engine: {e}")
        sys.exit(1)

    # Strategy: Require unique username before continuing
    username = prompt_unique_username(chatbot)

    print("\n" + "=" * 60)
    print(f"💬 Session active for: [{username}]")
    print("   Type 'exit' or 'quit' to end session.")
    print("   Type 'switch' to change user session.")
    print("=" * 60 + "\n")

    while True:
        try:
            query = input(f"[{username}] Ask FinBot > ").strip()

            if not query:
                continue

            if query.lower() in ["exit", "quit"]:
                print(f"Goodbye, {username}! Your Redis session will automatically expire if idle.")
                break

            if query.lower() == "switch":
                username = prompt_unique_username(chatbot)
                continue

            print("\n🤔 Thinking (Searching Vector DB, Graph DB, & Redis History)...")
            answer = chatbot.ask(username, query)

            print(f"\n🤖 FinBot Response:\n{answer}\n")
            print("-" * 60)

        except (KeyboardInterrupt, EOFError):
            print(f"\nExiting FinBot session for {username}...")
            break


if __name__ == "__main__":
    main()