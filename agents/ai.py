#!/usr/bin/env python3
# agents/ai.py
"""Single entry point for every LLM call in the project.

strategy.py and decision.py each used to hand-roll the same request -- same
URL, same payload envelope, same `AIKEY` header, same response unwrapping --
so a prompt or provider change meant editing two near-identical blocks. This
collapses that into one async helper, `generate_json`: a caller hands over
just the *input* (a prompt and the JSON schema it wants back) and gets the
parsed dict, with nothing provider-specific leaking out.

Which model actually answers is one env var, `AI_PROVIDER` (default
"gemini"). Each provider is a small `Format` that knows three things: the
env var holding its API key (or None for a keyless local server), how to
turn (prompt, schema) into an HTTP POST, and how to dig the JSON back out of
that provider's reply. Add a provider by adding one entry to `FORMATS`;
callers never change.

Schemas are written once in Gemini's shape (uppercase types, as in
strategy.py / decision.py). Providers that speak standard JSON Schema
(OpenAI, Anthropic, Ollama) get it translated on the way out by
`_to_json_schema`.

Keys / endpoints in the project .env, by provider:
  gemini     -> AIKEY              (default; the one this project runs on)
  openai     -> OPENAI_API_KEY     (OPENAI_BASE_URL to point at a local
                                    OpenAI-compatible server: LM Studio,
                                    llama.cpp, vLLM, ...)
  anthropic  -> ANTHROPIC_API_KEY
  ollama     -> (no key)           local models via Ollama; OLLAMA_BASE_URL
                                    default http://localhost:11434
Slow local models may need a bigger AI_TIMEOUT (seconds, default 180). Only
the gemini path is exercised by the running pipeline; the rest follow each
provider's documented JSON mode but are otherwise untested.
"""
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import aiohttp

_ROOT = Path(__file__).resolve().parent.parent


def _load_env(env_file=_ROOT / ".env"):
    """Loads KEY=value lines from .env without overriding real env vars."""
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_env()


def _require_key(name):
    key = os.environ.get(name)
    if not key:
        raise RuntimeError(f"{name} is not set (expected in {_ROOT / '.env'})")
    return key


# --------------------------------------------------------------------------
# Schema translation: Gemini uses uppercase types; everyone else wants
# standard JSON Schema.
# --------------------------------------------------------------------------

_JSON_TYPES = {"OBJECT": "object", "ARRAY": "array", "STRING": "string",
               "INTEGER": "integer", "NUMBER": "number", "BOOLEAN": "boolean"}


def _to_json_schema(node):
    """Rewrites a Gemini-style schema into standard JSON Schema: lowercases
    the type names and, on every object, forbids extra keys and marks all
    listed properties required -- what OpenAI strict mode and Anthropic
    structured outputs both expect."""
    if not isinstance(node, dict):
        return node
    out = {}
    for key, value in node.items():
        if key == "type" and isinstance(value, str):
            out[key] = _JSON_TYPES.get(value, value.lower())
        elif key == "properties" and isinstance(value, dict):
            out[key] = {name: _to_json_schema(sub)
                        for name, sub in value.items()}
        elif key == "items":
            out[key] = _to_json_schema(value)
        else:
            out[key] = value
    if out.get("type") == "object" and "properties" in out:
        out["additionalProperties"] = False
        out["required"] = list(out["properties"])
    return out


# --------------------------------------------------------------------------
# Provider formats: (prompt, schema, key) -> HTTP request, and reply -> dict
# --------------------------------------------------------------------------

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-8")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
LOCAL_MODEL = os.environ.get("LOCAL_MODEL", "llama3.1")

_TIMEOUT = float(os.environ.get("AI_TIMEOUT", "180"))  # local models can be slow


@dataclass(frozen=True)
class Format:
    """One provider: where its key lives (None = keyless local server), how
    to build its request, how to read its reply, and a label for logging."""
    key_env: str | None
    build: Callable[[str, dict, str | None], tuple]  # (prompt, schema, key) -> (url, headers, payload)
    parse: Callable[[dict], dict]                    # provider reply -> parsed JSON dict
    label: str


def _gemini_build(prompt, schema, key):
    url = ("https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    headers = {"x-goog-api-key": key}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": schema,
        },
    }
    return url, headers, payload


def _parse_ai_json(reply):
    """Unwraps the structured JSON from a Gemini generateContent reply.

    Kept under this name because strategy.py re-exports it and the tests
    import it from there."""
    try:
        text = reply["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected Gemini response: {reply}") from exc
    return json.loads(text)


def _openai_build(prompt, schema, key):
    url = f"{OPENAI_BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "result", "strict": True,
                            "schema": _to_json_schema(schema)},
        },
    }
    return url, headers, payload


def _openai_parse(reply):
    try:
        return json.loads(reply["choices"][0]["message"]["content"])
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected OpenAI response: {reply}") from exc


def _anthropic_build(prompt, schema, key):
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": prompt}],
        "output_config": {"format": {"type": "json_schema",
                                     "schema": _to_json_schema(schema)}},
    }
    return url, headers, payload


def _anthropic_parse(reply):
    try:
        text = next(block["text"] for block in reply["content"]
                    if block.get("type") == "text")
    except (KeyError, StopIteration) as exc:
        raise RuntimeError(f"Unexpected Anthropic response: {reply}") from exc
    return json.loads(text)


def _ollama_build(prompt, schema, key):
    # Ollama's native chat endpoint takes a JSON schema in `format` to
    # constrain the reply to structured JSON. Runs locally, no key -- but
    # honour one if the server is behind an auth proxy.
    url = f"{OLLAMA_BASE_URL}/api/chat"
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    payload = {
        "model": LOCAL_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "format": _to_json_schema(schema),
        "stream": False,
    }
    return url, headers, payload


def _ollama_parse(reply):
    try:
        return json.loads(reply["message"]["content"])
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected Ollama response: {reply}") from exc


FORMATS = {
    "gemini": Format("AIKEY", _gemini_build, _parse_ai_json, GEMINI_MODEL),
    "openai": Format("OPENAI_API_KEY", _openai_build, _openai_parse,
                     OPENAI_MODEL),
    "anthropic": Format("ANTHROPIC_API_KEY", _anthropic_build,
                        _anthropic_parse, ANTHROPIC_MODEL),
    "ollama": Format(None, _ollama_build, _ollama_parse, LOCAL_MODEL),
}

PROVIDER = os.environ.get("AI_PROVIDER", "gemini").lower()


def active_label(provider=None):
    """Human label for the provider in use, e.g. 'gemini (gemini-2.5-flash)'."""
    name = provider or PROVIDER
    return f"{name} ({FORMATS[name].label})"


async def generate_json(session, prompt, schema, provider=None):
    """Sends `prompt` to the configured AI provider and returns the parsed
    JSON object it produces, shaped to `schema`.

    The caller supplies only the input; the selected `Format` owns every
    provider detail (endpoint, auth header, payload envelope, response
    unwrapping). `provider` overrides the `AI_PROVIDER` env var for one call.
    """
    fmt = FORMATS[provider or PROVIDER]
    key = _require_key(fmt.key_env) if fmt.key_env else None
    url, headers, payload = fmt.build(prompt, schema, key)
    async with session.post(url, json=payload, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=_TIMEOUT)) as resp:
        resp.raise_for_status()
        return fmt.parse(await resp.json())
