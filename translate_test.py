#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust monthly translator:
- Safe Mongo query (no duplicate keys), projection-only fetch.
- Cleans and caps text sensibly before translation.
- Batch translate with retries + exponential backoff + jitter.
- Per-item fallback when batch fails (salvage what you can).
- Optional MarianMT fallback (if available) for stubborn failures.
- Always returns lists (never a bare string) from translate_text().
- Skips only the failed docs after MAX_RETRIES; continues processing.
- Bulk Mongo writes for speed; defensive exception handling throughout.
"""

import os
import re
import time
import random
from typing import List, Optional, Tuple, Dict

import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from deep_translator import GoogleTranslator

# Optional fallback (won't crash if unavailable)
try:
    import torch
    from transformers import MarianMTModel, MarianTokenizer
    HAS_MARIAN = True
except Exception:
    HAS_MARIAN = False

# ---------------------------
# Config
# ---------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true",
)

DB_NAME = "ml4p"
DATE_START = "2012-01-01"
BATCH_SIZE = 30                # number of docs per translation batch
CLEAN_CAP = 800                # cap maintext length per doc before translation
MAX_RETRIES = 4                # retries for a batch
PER_ITEM_RETRIES = 2           # retries for single-item salvage
BACKOFF_BASE = 1.6             # exponential backoff base (seconds)
BACKOFF_JITTER = (0.25, 0.75)  # random jitter added to backoff waits (seconds)
MONGO_TIMEOUT_MS = 20000
TODAY = pd.Timestamp.now()

# Marian fallback map (extend as needed)
MARIAN_MODELS = {
    # ISO-639 -> model
    "uz": "Helsinki-NLP/opus-mt-uz-en",
    "az": "Helsinki-NLP/opus-mt-az-en",
    "ka": "Helsinki-NLP/opus-mt-ka-en",
    "ne": "Helsinki-NLP/opus-mt-ne-en",
    "sw": "Helsinki-NLP/opus-mt-sw-en",
    "sr": "Helsinki-NLP/opus-mt-sr-en",
    "am": "Helsinki-NLP/opus-mt-am-en",
}

_marian_cache: Dict[str, Tuple[MarianTokenizer, MarianMTModel]] = {}

def _get_db() -> MongoClient:
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=MONGO_TIMEOUT_MS,
            connectTimeoutMS=MONGO_TIMEOUT_MS,
            socketTimeoutMS=MONGO_TIMEOUT_MS,
            tls=True,
        )
        # trigger server selection once
        client.admin.command("ping")
        return client[DB_NAME]
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(f"MongoDB not reachable: {e}")

def month_colname(dt: pd.Timestamp) -> str:
    # Keep your existing naming convention
    return f"articles-{dt.year}-{dt.month}"

def clean_text(s: Optional[str], cap: int = CLEAN_CAP) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace("\n", " ")
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    s = s.replace("\\", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:cap] if cap and len(s) > cap else s

def safe_sleep(base: float, attempt: int):
    # exponential backoff with jitter
    wait = (base ** attempt) + random.uniform(*BACKOFF_JITTER)
    time.sleep(wait)

def _ensure_list(x, expected_len: Optional[int] = None) -> List[Optional[str]]:
    """
    Coerce translator outputs into a list of strings (or None).
    If x is a bare string, wrap it. If None/Falsey, return a list of None.
    """
    if isinstance(x, list):
        return [xi if isinstance(xi, str) else None for xi in x]
    if isinstance(x, str):
        return [x]
    if expected_len is not None:
        return [None] * expected_len
    return []

# ---------------------------
# Translators
# ---------------------------
def google_translate_batch(src_lang: str, texts: List[str]) -> List[Optional[str]]:
    """Batch translate via deep_translator; returns list of strings/None, length preserved."""
    if not texts:
        return []
    # deep_translator can sometimes return a single string or raise
    out = GoogleTranslator(source=src_lang, target="en").translate_batch(texts)
    out_list = _ensure_list(out, expected_len=len(texts))

    # Length-guard: if mismatch, force per-item salvage
    if len(out_list) != len(texts):
        out_list = []
        for t in texts:
            try:
                single = GoogleTranslator(source=src_lang, target="en").translate(t)
                out_list.append(single if isinstance(single, str) else None)
                time.sleep(0.05)  # small pacing for single calls
            except Exception:
                out_list.append(None)
    return out_list

def marian_fallback(src_lang: str, texts: List[str]) -> List[Optional[str]]:
    """Optional MarianMT fallback; returns list of strings/None; length preserved."""
    if not HAS_MARIAN:
        return [None] * len(texts)
    model_name = MARIAN_MODELS.get(src_lang.lower())
    if not model_name:
        return [None] * len(texts)

    try:
        if model_name not in _marian_cache:
            tok = MarianTokenizer.from_pretrained(model_name)
            mdl = MarianMTModel.from_pretrained(model_name)
            _marian_cache[model_name] = (tok, mdl)
        else:
            tok, mdl = _marian_cache[model_name]

        outputs: List[Optional[str]] = []
        # modest micro-batching for memory safety
        step = 8
        for i in range(0, len(texts), step):
            chunk = texts[i : i + step]
            try:
                batch = tok.prepare_seq2seq_batch(chunk, return_tensors="pt")
            except TypeError:
                # for newer transformers versions
                batch = tok(chunk, return_tensors="pt", padding=True, truncation=True)
            with torch.no_grad():
                gen = mdl.generate(**batch, max_length=1024)
            decoded = tok.batch_decode(gen, skip_special_tokens=True)
            # preserve length
            if len(decoded) != len(chunk):
                # pad with None if mis-sized
                decoded = decoded + [None] * (len(chunk) - len(decoded))
                decoded = decoded[: len(chunk)]
            outputs.extend(decoded)
        return outputs
    except Exception:
        return [None] * len(texts)

def translate_text(src_lang: str, texts: List[str]) -> List[Optional[str]]:
    """
    Robust translate with:
      1) batched GoogleTranslator + retries/backoff,
      2) per-item salvage if batch fails,
      3) optional MarianMT fallback for remaining None entries.
    Always returns a list with len(texts).
    """
    texts = [clean_text(t, cap=None) for t in texts]  # don't cap titles here
    # 1) Batch with retries
    for attempt in range(MAX_RETRIES):
        try:
            out = google_translate_batch(src_lang, texts)
            if out and len(out) == len(texts) and any(o is not None for o in out):
                break
        except Exception:
            out = [None] * len(texts)
        safe_sleep(BACKOFF_BASE, attempt)
    else:
        # exhausted attempts
        out = [None] * len(texts)

    # 2) Per-item salvage for Nones
    for idx, (inp, val) in enumerate(zip(texts, out)):
        if val is not None:
            continue
        for attempt in range(PER_ITEM_RETRIES):
            try:
                single = GoogleTranslator(source=src_lang, target="en").translate(inp)
                if isinstance(single, str) and single.strip():
                    out[idx] = single
                    break
            except Exception:
                pass
            safe_sleep(BACKOFF_BASE, attempt)

    # 3) Marian fallback for remaining Nones (best-effort)
    remaining_idx = [i for i, v in enumerate(out) if v is None]
    if remaining_idx:
        fallback_out = marian_fallback(src_lang, [texts[i] for i in remaining_idx])
        for j, val in zip(remaining_idx, fallback_out):
            out[j] = val

    # Final sanitize
    out = [clean_text(x, cap=None) if isinstance(x, str) else None for x in out]
    # Never return bare strings; always a list of str/None
    return out

# ---------------------------
# Data / Mongo
# ---------------------------
def pull_data(db, colname: str, sources: List[str], lang: str, limit: Optional[int] = None) -> List[dict]:
    """
    Defensive Mongo query:
      - projection only fields we need
      - proper $and to avoid duplicate key overwrites in Python dicts
      - ensures string types and non-empty values
      - only docs missing at least one of the translated fields
    """
    query = {
        "source_domain": {"$in": sources},
        "include": True,
        "language": lang,
        "$and": [
            {"title": {"$type": "string"}},
            {"title": {"$ne": ""}},
            {"maintext": {"$type": "string"}},
            {"maintext": {"$ne": ""}},
        ],
        "$or": [
            {"title_translated": {"$exists": False}},
            {"maintext_translated": {"$exists": False}},
        ],
    }
    projection = {"_id": 1, "title": 1, "maintext": 1}
    try:
        cursor = db[colname].find(query, projection=projection)
        if limit:
            cursor = cursor.limit(limit)
        docs = []
        for doc in cursor:
            title = clean_text(doc.get("title", ""), cap=None)
            main = clean_text(doc.get("maintext", ""), cap=CLEAN_CAP)
            if len(title) >= 3 and len(main) >= 5:
                doc["title"] = title
                doc["maintext"] = main
                docs.append(doc)
        return docs
    except PyMongoError as e:
        print(f"[WARN] Mongo query failed for {colname}: {e}")
        return []

def bulk_update_translations(db, colname: str, batch_docs: List[dict],
                             titles_en: List[Optional[str]],
                             mains_en: List[Optional[str]]):
    """
    Only update docs where we have both title and maintext translated.
    Uses bulk_write for speed and robustness.
    """
    ops = []
    for doc, t_en, m_en in zip(batch_docs, titles_en, mains_en):
        if isinstance(t_en, str) and t_en.strip() and isinstance(m_en, str) and m_en.strip():
            ops.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "language_translated": "en",
                        "title_translated": t_en,
                        "maintext_translated": m_en,
                    }},
                )
            )
    if not ops:
        return 0
    try:
        result = db[colname].bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)
    except PyMongoError as e:
        print(f"[WARN] bulk_write failed for {colname}: {e}")
        return 0

def chunked(lst: List, size: int) -> List[List]:
    if size <= 0:
        size = 1
    return [lst[i : i + size] for i in range(0, len(lst), size)]

# ---------------------------
# Main
# ---------------------------
def main():
    db = _get_db()

    # Example: Uzbekistan sources + language
    lang = "sr"
     #lan = az, uz, ka, ne, sw, sr, am
    sources = db.sources.distinct(
        "source_domain",
        filter={"include": True, "primary_location": {"$in": ["SRB"]}},
    )
    print(f"[INFO] {len(sources)} sources found for lang={lang}: {sources}")

    # Monthly date range through next month end to match your behavior
    df = pd.DataFrame({"date": pd.date_range(DATE_START, TODAY + pd.Timedelta(31, "d"), freq="M")})
    df.set_index("date", inplace=True)

    for dt in df.index:
        colname = month_colname(dt)
        docs = pull_data(db, colname, sources, lang)
        if not docs:
            print(f"[INFO] {colname}: 0 docs to translate; skipping.")
            continue

        print(f"[INFO] {colname}: {len(docs)} docs to process.")
        total_updated = 0

        for bi, batch in enumerate(tqdm(chunked(docs, BATCH_SIZE), desc=f"{colname}")):
            titles = [d["title"] for d in batch]
            mains  = [d["maintext"] for d in batch]

            # Translate with robust function (always returns lists of len=batch)
            titles_en = translate_text(lang, titles)
            mains_en  = translate_text(lang, mains)

            # Safe preview (only if we have at least 2 items and strings)
            if len(batch) > 1 and isinstance(titles_en[0], str) and isinstance(titles_en[-1], str):
                print(
                    f"\n{colname} (Batch {bi+1}/{(len(docs)+BATCH_SIZE-1)//BATCH_SIZE})"
                    f"\n1st: (title) {titles_en[0][:120]}"
                    f"\nlast: (title) {titles_en[-1][:120]}\n"
                )
            if len(batch) > 1 and isinstance(mains_en[0], str) and isinstance(mains_en[-1], str):
                print(
                    f"{colname} (Batch {bi+1}/{(len(docs)+BATCH_SIZE-1)//BATCH_SIZE})"
                    f"\n1st: (main) {mains_en[0][:200]}"
                    f"\nlast: (main) {mains_en[-1][:200]}\n"
                )

            # Write results (skip rows with missing pieces)
            updated = bulk_update_translations(db, colname, batch, titles_en, mains_en)
            total_updated += updated

            # Gentle pacing between batches to avoid rate-limits
            time.sleep(0.3)

        print(f"[INFO] {colname}: updated {total_updated} documents.\n")

if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        # e.g., Mongo not reachable
        print(f"[FATAL] {e}")
    except Exception as e:
        print(f"[FATAL] Unexpected error: {e}")
