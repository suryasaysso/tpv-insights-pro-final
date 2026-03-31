"""
nl_to_sql.py
────────────
NL-to-SQL via Groq API with two-model fallback.

PRIMARY  → openai/llama-3.3-70b-versatile
FALLBACK → gpt-oss-120b (auto on primary 404)
"""

import os
import re
import time
from typing import Optional, Generator
from openai import OpenAI, RateLimitError, APIStatusError
from knowledge_base import get_system_prompt

PRIMARY_MODEL  = "llama-3.3-70b-versatile"
FALLBACK_MODEL = "openai/gpt-oss-120b"
DEFAULT_MODEL  = PRIMARY_MODEL
GROQ_BASE_URL  = "https://api.groq.com/openai/v1"

_active_model: str   = PRIMARY_MODEL
_primary_failed: bool = False
_client: Optional[OpenAI] = None


def get_active_model() -> str:
    return _active_model

def is_using_fallback() -> bool:
    return _primary_failed


def _load_api_key() -> str:
    try:
        import streamlit as st
        key = st.secrets.get("GROQ_API_KEY", "")
        if key:
            return key
    except Exception:
        pass
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise ValueError(
            "GROQ_API_KEY not set.\n"
            "  Local: add to .env:  GROQ_API_KEY=gsk_...\n"
            "  Streamlit Cloud: App Settings → Secrets"
        )
    return key


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        # Default retry configuration for standard transient errors
        _client = OpenAI(
            api_key=_load_api_key(), 
            base_url=GROQ_BASE_URL,
            max_retries=3
        )
    return _client


def check_groq_running() -> bool:
    try:
        _get_client().models.list()
        return True
    except Exception:
        return False


def list_available_models() -> list[str]:
    try:
        return [m.id for m in _get_client().models.list().data]
    except Exception:
        return [FALLBACK_MODEL]


def call_groq(
    prompt: str,
    system: str = "",
    model: str = PRIMARY_MODEL,
    temperature: float = 0.1,
    max_tokens: int = 2048,
) -> str:
    global _active_model, _primary_failed
    
    # If primary has failed permanently (404), use fallback
    if _primary_failed and model == PRIMARY_MODEL:
        model = FALLBACK_MODEL

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            resp = _get_client().chat.completions.create(
                model=model, messages=messages,
                temperature=temperature, max_tokens=max_tokens,
            )
            _active_model = model
            content = resp.choices[0].message.content
            if not content:
                raise ValueError(f"Model {model} returned an empty response. This may be due to a safety filter or a transient API issue.")
            return content
        
        except RateLimitError as e:
            # If we hit rate limit on last attempt or on fallback model, give up or try fallback
            if attempt == max_attempts - 1 or model == FALLBACK_MODEL:
                if model == PRIMARY_MODEL:
                    # Try falling back immediately if primary is rate limited
                    model = FALLBACK_MODEL
                    continue
                raise e
            
            # Wait with exponential backoff
            wait_time = (2 ** attempt) + 1
            time.sleep(wait_time)
            continue

        except APIStatusError as e:
            err = str(e).lower()
            # If model is not found (404), switch to fallback permanently for this session
            if model == PRIMARY_MODEL and any(x in err for x in ["404", "not found", "does not exist"]):
                _primary_failed = True
                model = FALLBACK_MODEL
                continue
            
            # For other 5xx errors, retry
            if e.status_code >= 500 and attempt < max_attempts - 1:
                time.sleep(1)
                continue
            raise e
        
        except Exception as e:
            raise e


def call_groq_stream(
    prompt: str, system: str = "",
    model: str = PRIMARY_MODEL,
    temperature: float = 0.1, max_tokens: int = 2048,
) -> Generator[str, None, None]:
    global _active_model, _primary_failed
    
    if _primary_failed and model == PRIMARY_MODEL:
        model = FALLBACK_MODEL
        
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            with _get_client().chat.completions.create(
                model=model, messages=messages,
                temperature=temperature, max_tokens=max_tokens, stream=True,
            ) as stream:
                _active_model = model
                for chunk in stream:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        yield delta
            return # Success

        except RateLimitError as e:
            if attempt == max_attempts - 1 or model == FALLBACK_MODEL:
                if model == PRIMARY_MODEL:
                    model = FALLBACK_MODEL
                    continue
                raise e
            time.sleep((2 ** attempt) + 1)
            continue
            
        except APIStatusError as e:
            err = str(e).lower()
            if model == PRIMARY_MODEL and any(x in err for x in ["404", "not found", "does not exist"]):
                _primary_failed = True
                model = FALLBACK_MODEL
                continue
            if e.status_code >= 500 and attempt < max_attempts - 1:
                time.sleep(1)
                continue
            raise e
        
        except Exception as e:
            raise e


def extract_sql(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    for pat in [
        r"```sql\s*(.*?)\s*```",
        r"```\s*((?:SELECT|WITH|INSERT|UPDATE|DELETE|CREATE)\s+.*?)\s*```",
        r"`(SELECT[^`]+)`",
    ]:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            sql = m.group(1).strip()
            if sql:
                return sql
    m = re.search(r"((?:SELECT|WITH)\s+.+)", text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def extract_thought(text: str) -> str:
    if not isinstance(text, str):
        return ""
    m = re.search(
        r"(?:THOUGHT|APPROACH|ANALYSIS)[:\s]*(.+?)(?:SQL:|```|$)",
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        return m.group(1).strip()
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return " ".join(sentences[:2]) if sentences else ""


class NLToSQL:
    def __init__(self, model: str = PRIMARY_MODEL):
        self.model         = model
        self.system_prompt = get_system_prompt()

    def generate(self, question: str) -> dict:
        prompt = (
            f"Business Question: {question}\n\n"
            "1. THOUGHT: Your analytical approach\n"
            "2. SQL:\n```sql\n-- DuckDB SQL\n```\n"
            "3. Leave INTERPRETATION blank.\n\nGenerate SQL now:"
        )
        try:
            raw = call_groq(prompt=prompt, system=self.system_prompt,
                            model=self.model, temperature=0.05, max_tokens=2048)
            return {"thought": extract_thought(raw), "sql": extract_sql(raw),
                    "raw_response": raw, "error": None}
        except Exception as e:
            return {"thought": "", "sql": None, "raw_response": "", "error": str(e)}

    def refine_sql(self, original_sql: str, error_msg: str, question: str) -> Optional[str]:
        prompt = (
            f"Fix this DuckDB SQL.\nQuestion: {question}\n\n"
            f"Broken SQL:\n```sql\n{original_sql}\n```\n\nError:\n{error_msg}\n\n"
            "Return ONLY the corrected SQL in a ```sql block."
        )
        try:
            raw = call_groq(prompt=prompt, system=self.system_prompt,
                            model=self.model, temperature=0.0, max_tokens=1024)
            return extract_sql(raw)
        except Exception:
            return None
