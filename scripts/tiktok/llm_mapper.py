import os
import json
from typing import Optional
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pymongo import MongoClient
import psycopg2


def get_llm():
    """Initialize the OpenRouter LLM"""
    openrouter_api_key = os.environ.get("OPENROUTER_API_KEY")
    if not openrouter_api_key:
        print("⚠️ Warning: OPENROUTER_API_KEY not found in environment")

    return ChatOpenAI(
        model="deepseek/deepseek-chat",
        openai_api_key=openrouter_api_key,
        openai_api_base="https://openrouter.ai/api/v1",
        max_tokens=200,
        temperature=0.1,
    )


def extract_place_name(text: str) -> Optional[str]:
    """
    Extract the main tourist attraction/place name from TikTok caption/text.
    """
    try:
        llm = get_llm()
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are an expert at extracting Thai tourist attraction names from text. "
                    "Extract ONLY the main place name mentioned in the text. "
                    "If it's just a province name without a specific place, return the province name. "
                    "If no place is mentioned, reply with 'NONE'. "
                    "Your response must be ONLY the place name in Thai, no other words.",
                ),
                ("human", "Text: {text}\n\nPlace name:"),
            ]
        )

        chain = prompt | llm
        result = chain.invoke({"text": text})

        place_name = result.content.strip()
        if place_name == "NONE" or not place_name:
            return None

        return place_name
    except Exception as e:
        print(f"❌ Error in LLM extraction: {e}")
        return None


def find_place_id(place_name: str, pg_conn_str: str) -> Optional[str]:
    """
    Look up the place_id in PostgreSQL by matching the name.
    """
    if not place_name:
        return None

    try:
        conn = psycopg2.connect(pg_conn_str)
        cur = conn.cursor()

        # Simple exact/ilike match first
        cur.execute(
            """
            SELECT id FROM places 
            WHERE name ILIKE %s OR name_en ILIKE %s
            LIMIT 1
        """,
            (f"%{place_name}%", f"%{place_name}%"),
        )

        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            return result[0]

        return None
    except Exception as e:
        print(f"❌ DB Error finding place '{place_name}': {e}")
        return None
