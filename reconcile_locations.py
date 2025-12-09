#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
reconcile_locations.py

Reconcile gemini_locations + cliff_locations into KB-grounded admin0/admin1/admin2 tags.

Key design goals:
- Never assign a country/admin ladder purely from weak single-token matching.
- Only promote ADMIN1/ADMIN2 when the KB match is plausibly an alias of the *surface* mention (the dict key).
- Prefer (and preserve) Gemini hints when we cannot refute them.
- Use CLiFF mostly as ISO context + optional safe “cliff-only” adds (exact KB match only).
- Avoid generic/global and ultra-ambiguous tokens (“earth”, “europe”, “south”, etc).
- Revisit is NOT “no locations”; revisit only when we appear to have a real admin-unit mention
  under a KB-grounded country but cannot produce a grounded admin1/admin2 ladder.
- De-duplicate when multiple surface forms map to the same (level, admin0, admin1, admin2) ladder.
- NEW: persist match evidence (which surface/token triggered the match) for debugging/aggregation.
"""

import os
import re
import json
import ast
import unicodedata
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List, Set, Optional

import pandas as pd
from pymongo import MongoClient
from rapidfuzz import process, fuzz


# =============================================================================
# DB field names
# =============================================================================
RECONCILED_FIELD = "reconciled_locations"
RECONCILED_META_FIELD = "reconciled_locations_meta"
RECONCILED_DEBUG_FIELD = "reconciled_locations_debug"
RECONCILED_REVISIT_FIELD = "reconciled_locations_revisit"

# NEW: easier aggregation/debugging
RECONCILED_EVIDENCE_FIELD = "reconciled_locations_evidence"
RECONCILED_MATCH_TERMS_FIELD = "reconciled_locations_match_terms"


# =============================================================================
# String normalization + parsing
# =============================================================================
UNKNOWN_SENTINELS = {
    "", "unknown", "unk", "none", "null", "nan", "n/a", "na",
    "not sure", "unspecified", "not specified",
    "other",
    # Gemini placeholders
    "country", "country or unknown", "unknown or country",
    "country or unk", "unknown or admin0",
    "admin0", "admin1", "admin2",
}

LEVEL_ALIASES = {
    # admin0
    "country": "ADMIN0",
    "nation": "ADMIN0",
    "admin0": "ADMIN0",
    "level0": "ADMIN0",
    "national": "ADMIN0",
    # admin1
    "admin1": "ADMIN1",
    "state": "ADMIN1",
    "province": "ADMIN1",
    "region": "ADMIN1",
    "oblast": "ADMIN1",
    "governorate": "ADMIN1",
    "department": "ADMIN1",
    # NOTE: "district" can be admin1-ish in some countries; we treat as admin1 hint.
    "district": "ADMIN1",
    # admin2
    "admin2": "ADMIN2",
    "city": "ADMIN2",
    "town": "ADMIN2",
    "municipality": "ADMIN2",
    "county": "ADMIN2",
    "commune": "ADMIN2",
    "locality": "ADMIN2",
    # other
    "other": "UNKNOWN",
}

# generic / global concepts (never geocode)
GENERIC_GLOBAL_NAMES = {
    # whole-world
    "earth", "planet earth", "the earth",
    "world", "the world", "global", "globe", "the globe",
    "universe", "planet", "mother earth",
    # regions / continents
    "africa", "asia", "europe", "eu", "e u", "e.u", "e.u.",
    "european union", "latin america", "north america", "south america",
    "central america", "middle east", "southeast asia",
    "sub saharan africa", "sub-saharan africa",
    "caribbean", "oceania", "antarctica", "antarctic",
    # polar
    "arctic", "arctic ocean", "antarctic ocean",
    # oceans / seas
    "pacific ocean", "atlantic ocean", "indian ocean", "southern ocean",
    "arctic sea", "mediterranean sea",
    "global south", "global north",
}

# direction-only phrases (skip as locations)
DIRECTION_TOKENS = {
    "north", "south", "east", "west", "central",
    "northern", "southern", "eastern", "western",
    "northwest", "northeast", "southwest", "southeast",
    "upper", "lower", "mid", "middle",
}

# single-token stopwords for token shortcuts (do NOT include in token index)
# and also used as "too generic to match alone" in fuzzy safeguards.
TOKEN_STOPWORDS = {
    # admin-ish / feature words
    "province", "district", "region", "oblast", "state", "county",
    "municipality", "department", "prefecture", "commune", "governorate",
    "republic", "kingdom", "emirate", "federation",
    "city", "town", "village", "villa",
    # geographic features
    "river", "lake", "mount", "mountain", "valley", "bay", "gulf",
    "ocean", "sea", "strait", "channel", "island", "islands",
    "airport", "road", "street", "avenue", "highway", "bridge", "port",
    # directions / ultra-ambiguous
    "north", "south", "east", "west", "central",
    "northern", "southern", "eastern", "western",
    "northwest", "northeast", "southwest", "southeast",
    "upper", "lower", "mid", "middle",
    # too vague
    "new", "old",
}

# Names where CLiFF name->ISO anchoring is known-unreliable
CLIFF_NAME_ISO_UNTRUSTED = {
    "danube",
    "crimea",  # CLiFF often maps this to AUS; we handle via Gemini/doc hints
}

# Qualifier tokens allowed when matching a *single token* query to a *multi-token* KB value
# (e.g., "Odessa" -> "Odessa Oblast" is OK; "Odessa" -> "Nova Odessa" is NOT)
MULTI_TOKEN_QUALIFIERS = {
    "oblast", "province", "region", "state", "district", "department",
    "governorate", "prefecture", "county", "commune", "municipality",
    "parish",
    "city", "capital", "metropolitan", "special", "federal",
    "territory", "republic", "autonomous", "of", "the",
    "saint", "st",  # common prefixes that shouldn't block "Petersburg" -> "Saint Petersburg"
}

# Known exonyms / common transliterations / typos seen in logs
# (Used only as a matching aid; output still comes from KB canonical values)
EXONYM_MAP = {
    # Ukraine
    "kiev": "kyiv",
    "odessa": "odesa",
    "lvov": "lviv",
    # Indonesia typo
    "jakarda": "jakarta",
}

# POI-ish names are often not in admin gazetteer; avoid revisiting these
POI_KEYWORDS = {
    "school", "primary school", "secondary school",
    "university", "college", "hospital", "clinic",
    "airport", "road", "street", "avenue", "highway",
    "bridge", "station", "market", "mosque", "church", "temple",
    "park", "national park", "reserve", "forest",
}

# Feature-ish geography phrases we usually should not try to "admin match"
FEATURE_KEYWORDS = {
    "gulf", "sea", "ocean", "river", "lake", "bay", "strait", "channel",
    "mount", "mountain", "valley", "island", "islands",
}


def normalize_for_match(s: str) -> str:
    """Accent/punctuation tolerant normalization for matching."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_str(v) -> str:
    if v is None:
        return "Unknown"
    s = str(v).strip()
    if not s:
        return "Unknown"
    if normalize_for_match(s) in UNKNOWN_SENTINELS:
        return "Unknown"
    return s


def safe_load_dict(v):
    """Accept dict (Mongo), JSON string, or Python-literal string."""
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            try:
                obj = ast.literal_eval(s)
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}
    return {}


def is_generic_location_name(name: str) -> bool:
    return normalize_for_match(name) in GENERIC_GLOBAL_NAMES


def is_pure_directional(name: str) -> bool:
    """True if name consists only of direction tokens ('north', 'south east', etc)."""
    n = normalize_for_match(name)
    if not n:
        return False
    toks = n.split()
    if not toks:
        return False
    return all(t in DIRECTION_TOKENS for t in toks)


def looks_like_poi(loc_name: str) -> bool:
    n = normalize_for_match(loc_name)
    return any(k in n for k in POI_KEYWORDS)


def looks_like_admin_unit(loc_name: str) -> bool:
    n = normalize_for_match(loc_name)
    admin_terms = {"province", "oblast", "region", "district", "state", "department", "governorate", "prefecture", "county", "commune", "municipality"}
    return any(t in n for t in admin_terms)


def looks_like_feature_phrase(loc_name: str) -> bool:
    """
    Heuristic: phrases like 'Gulf of Mexico', 'North Sea', 'Danube River' are not admin units.
    We still keep them as mentions, but we do NOT try to match them to KB admin units.
    """
    # If the phrase explicitly looks like an administrative unit (e.g. 'Gulf Province'),
    # do NOT treat it as a pure geographic feature.
    if looks_like_admin_unit(loc_name):
        return False
    n = normalize_for_match(loc_name)
    if not n:
        return False
    toks = n.split()
    if not toks:
        return False
    # must contain a feature keyword, and be mostly feature/prep-ish tokens
    if not any(t in FEATURE_KEYWORDS for t in toks):
        return False
    allowed = FEATURE_KEYWORDS | {"of", "the"} | DIRECTION_TOKENS
    # if there's any substantial non-allowed token, don't treat as pure feature phrase
    substantial = [t for t in toks if t not in allowed]
    # allow 0-1 substantial tokens (e.g. 'mexico' in 'gulf of mexico')
    return len(substantial) <= 1


def sanitize_location_info(info: dict) -> dict:
    """Normalize a Gemini / reconciled location dict into a safe canonical shape."""
    info = info or {}
    raw_level = str(info.get("location_level", "Unknown") or "Unknown").strip()
    level_norm = normalize_for_match(raw_level)
    level = LEVEL_ALIASES.get(level_norm, raw_level.upper())
    if level not in {"ADMIN0", "ADMIN1", "ADMIN2"}:
        level = "UNKNOWN"

    out = {
        "location_level": level,
        "admin0": _clean_str(info.get("admin0", "Unknown")),
        "admin1": _clean_str(info.get("admin1", "Unknown")),
        "admin2": _clean_str(info.get("admin2", "Unknown")),
    }

    # Gemini sometimes sets admin0="Country" or similar placeholders
    if normalize_for_match(out["admin0"]) in {"country", "admin0"}:
        out["admin0"] = "Unknown"

    return out


# =============================================================================
# KB Index
# =============================================================================
class KBIndex:
    LEVELS = ["Admin0", "Admin1", "Admin2"]

    def __init__(self, kb_df: pd.DataFrame):
        kb = kb_df.copy()
        for col in ["Admin0", "Admin1", "Admin2", "Admin0_ISO3"]:
            if col not in kb.columns:
                raise ValueError(f"KB missing required column: {col}")
            kb[col] = kb[col].fillna("").astype(str)

        kb["Admin0_ISO3"] = kb["Admin0_ISO3"].str.upper()
        self.kb = kb

        # ISO -> Admin0 (choose most frequent name)
        self.iso_to_admin0: Dict[str, str] = {}
        for iso, g in kb.groupby("Admin0_ISO3"):
            g2 = g[g["Admin0"].astype(str).str.strip() != ""]
            if len(g2) > 0:
                self.iso_to_admin0[iso] = g2["Admin0"].value_counts().idxmax()

        # exact value -> row indices
        self.value_to_indices = {col: defaultdict(list) for col in self.LEVELS}
        # normalized exact -> original values
        self.norm_to_values = {col: defaultdict(set) for col in self.LEVELS}
        # token index: token -> set(values)
        self.token_index = {col: defaultdict(set) for col in self.LEVELS}

        # token frequency (how many unique values contain token)
        self.token_value_count = {col: defaultdict(int) for col in self.LEVELS}

        # fuzzy choices
        self.choices_global = {col: sorted(kb[col].dropna().unique().tolist()) for col in self.LEVELS}
        self.choices_by_iso = {col: defaultdict(list) for col in self.LEVELS}  # col -> iso -> list
        tmp_iso_sets = {col: defaultdict(set) for col in self.LEVELS}

        for i, row in kb.iterrows():
            iso = row["Admin0_ISO3"]
            for col in self.LEVELS:
                val = row[col]
                if not val:
                    continue

                self.value_to_indices[col][val].append(i)
                self.norm_to_values[col][normalize_for_match(val)].add(val)
                tmp_iso_sets[col][iso].add(val)

        # Build token indexes + counts (unique values per token)
        for col in self.LEVELS:
            for val in self.choices_global[col]:
                for tok in normalize_for_match(val).split():
                    if tok and tok not in TOKEN_STOPWORDS:
                        self.token_index[col][tok].add(val)
            for tok, vals in self.token_index[col].items():
                self.token_value_count[col][tok] = len(vals)

        for col in self.LEVELS:
            for iso, s in tmp_iso_sets[col].items():
                self.choices_by_iso[col][iso] = sorted(s)

    @staticmethod
    def get_iso_set(cliff_context) -> Set[str]:
        ISO3_RE = re.compile(r"^[A-Z]{3}$")
        BAD_ISOS = {"NONE", "UNK", "UNKNOWN", "NULL", "N/A"}
        if not isinstance(cliff_context, dict):
            return set()
        out = set()
        for k in cliff_context.keys():
            iso = str(k).upper().strip()
            if ISO3_RE.match(iso) and iso not in BAD_ISOS:
                out.add(iso)
        return out

    def cliff_isos_for_name(self, name: str, cliff_context: dict) -> Set[str]:
        """Return ISO3 set for which CLiFF explicitly listed this exact place name."""
        if not isinstance(cliff_context, dict):
            return set()
        target = normalize_for_match(name)
        out = set()
        for iso, places in cliff_context.items():
            iso3 = str(iso).upper().strip()
            if iso3 not in self.iso_to_admin0:
                continue
            if not isinstance(places, (list, tuple)):
                continue
            for p in places:
                if normalize_for_match(p) == target:
                    out.add(iso3)
                    break
        return out

    def is_admin0_name(self, name: str) -> bool:
        return normalize_for_match(name) in self.norm_to_values["Admin0"]

    def resolve_admin0(
        self,
        text: str,
        allowed_isos: Optional[Set[str]] = None,
        min_score: int = 88,
    ) -> Tuple[Optional[str], float, str]:
        """
        Resolve a raw country string (or ISO3) to a KB Admin0 value.

        Uses WRatio (more robust than token_set_ratio for 'Russia' vs 'Russian Federation').
        Returns (admin0_or_None, score, method).
        """
        if text is None:
            return None, 0.0, "empty"
        raw = str(text).strip()
        if not raw:
            return None, 0.0, "empty"

        up = raw.upper().strip()
        if re.fullmatch(r"[A-Z]{3}", up) and up in self.iso_to_admin0:
            return self.iso_to_admin0[up], 100.0, "iso3"

        norm = normalize_for_match(raw)
        if norm in self.norm_to_values["Admin0"]:
            val = sorted(self.norm_to_values["Admin0"][norm])[0]
            return val, 100.0, "exact"

        isos = sorted({x.upper() for x in (allowed_isos or set()) if x})
        choices = self.choices_global["Admin0"]
        if isos:
            union = []
            for iso in isos:
                union.extend(self.choices_by_iso["Admin0"].get(iso, []))
            if union:
                choices = sorted(set(union))

        m = process.extractOne(
            raw,
            choices,
            scorer=fuzz.WRatio,
            processor=normalize_for_match,
        )
        if m and float(m[1]) >= float(min_score):
            return m[0], float(m[1]), "fuzzy"
        return None, 0.0, "no_match"

    def token_frequency(self, token: str, cols: Optional[List[str]] = None) -> int:
        """How many unique KB values contain this token across provided cols."""
        token = normalize_for_match(token)
        if not token:
            return 10**9
        cols = cols or ["Admin1", "Admin2"]
        return sum(self.token_value_count.get(c, {}).get(token, 0) for c in cols)

    def token_hits(self, name: str, cols=None, max_values: int = 18) -> List[Tuple[str, str]]:
        """
        Single-token token hits across selected cols.

        Safety:
        - only single token
        - token must not be stopword
        - token must not be too common in KB within those cols (<= max_values)
        """
        cols = cols or self.LEVELS
        clean = normalize_for_match(name)
        if not clean or len(clean.split()) != 1:
            return []
        if clean in TOKEN_STOPWORDS:
            return []
        if len(clean) < 4:
            return []
        # too common => skip token shortcut entirely
        if self.token_frequency(clean, cols=cols) > max_values:
            return []

        hits = []
        for col in cols:
            for val in self.token_index[col].get(clean, []):
                hits.append((val, col))
        return hits

    def rows_for_value(
        self,
        col: str,
        value: str,
        allowed_isos: Optional[Set[str]] = None,
        prefer_admin0: Optional[str] = None,
        prefer_admin1: Optional[str] = None,
        iso_filter_mode: str = "soft",
    ) -> pd.DataFrame:
        """Rows matching a candidate value with optional ISO/admin preferences."""
        if col not in self.LEVELS:
            return self.kb.iloc[0:0]

        indices = list(self.value_to_indices[col].get(value, []))
        if not indices:
            norm = normalize_for_match(value)
            for v in self.norm_to_values[col].get(norm, []):
                indices.extend(self.value_to_indices[col].get(v, []))

        if not indices:
            return self.kb.iloc[0:0]

        cand = self.kb.loc[indices]

        if allowed_isos:
            allowed_isos = {x.upper() for x in allowed_isos}
            cand2 = cand[cand["Admin0_ISO3"].isin(allowed_isos)]
            if iso_filter_mode == "strict":
                cand = cand2
            else:
                if len(cand2) > 0:
                    cand = cand2

        if prefer_admin0 and prefer_admin0 not in {"Unknown", "", None}:
            cand2 = cand[cand["Admin0"].apply(lambda x: normalize_for_match(x) == normalize_for_match(prefer_admin0))]
            if len(cand2) > 0:
                cand = cand2

        if prefer_admin1 and prefer_admin1 not in {"Unknown", "", None}:
            cand2 = cand[cand["Admin1"].apply(lambda x: normalize_for_match(x) == normalize_for_match(prefer_admin1))]
            if len(cand2) > 0:
                cand = cand2

        return cand.sort_values(["Admin0", "Admin1", "Admin2"]).drop_duplicates()

    def fuzzy_best(
        self,
        name: str,
        allowed_isos: Optional[Set[str]] = None,
        cols=None,
        iso_filter_mode: str = "soft",
    ) -> Tuple[Optional[str], float, Optional[str]]:
        """Best fuzzy match over given cols (+ optional ISO filter)."""
        cols = cols or self.LEVELS
        best_val, best_score, best_col = None, 0.0, None
        iso_list = sorted({x.upper() for x in (allowed_isos or set()) if x})

        for col in cols:
            choices = self.choices_global[col]
            if iso_list:
                union = []
                for iso in iso_list:
                    union.extend(self.choices_by_iso[col].get(iso, []))
                if union:
                    choices = sorted(set(union))
                elif iso_filter_mode == "strict":
                    continue  # enforce: no choices => no match allowed

            if not choices:
                continue

            m = process.extractOne(
                name,
                choices,
                scorer=fuzz.WRatio,
                processor=normalize_for_match,
            )
            if m and float(m[1]) > best_score:
                best_val, best_score, best_col = m[0], float(m[1]), col

        return best_val, best_score, best_col


# =============================================================================
# Grounding helpers
# =============================================================================
def admin0_isos(admin0: str, kb_index: KBIndex) -> Set[str]:
    if admin0 in {None, "", "Unknown"}:
        return set()
    norm = normalize_for_match(admin0)
    # fast path: exact normalized Admin0 hit
    if norm in kb_index.norm_to_values["Admin0"]:
        canonical = sorted(kb_index.norm_to_values["Admin0"][norm])[0]
        rows = kb_index.kb[kb_index.kb["Admin0"].apply(lambda x: normalize_for_match(x) == normalize_for_match(canonical))]
        return set(rows["Admin0_ISO3"].dropna().astype(str).str.upper().unique().tolist())
    # fallback scan (rare)
    rows = kb_index.kb[kb_index.kb["Admin0"].apply(lambda x: normalize_for_match(x) == norm)]
    return set(rows["Admin0_ISO3"].dropna().astype(str).str.upper().unique().tolist())


def is_kb_pair(admin0: str, admin1: str, kb_index: KBIndex, allowed_isos: Optional[Set[str]] = None) -> bool:
    if admin0 in {"Unknown", "", None} or admin1 in {"Unknown", "", None}:
        return False
    a0n = normalize_for_match(admin0)
    a1n = normalize_for_match(admin1)
    rows = kb_index.kb[
        (kb_index.kb["Admin0"].apply(lambda x: normalize_for_match(x) == a0n)) &
        (kb_index.kb["Admin1"].apply(lambda x: normalize_for_match(x) == a1n))
    ]
    if allowed_isos:
        rows = rows[rows["Admin0_ISO3"].isin({x.upper() for x in allowed_isos})]
    return len(rows) > 0


def is_kb_triple(admin0: str, admin1: str, admin2: str, kb_index: KBIndex, allowed_isos: Optional[Set[str]] = None) -> bool:
    if admin0 in {"Unknown", "", None} or admin1 in {"Unknown", "", None} or admin2 in {"Unknown", "", None}:
        return False
    a0n = normalize_for_match(admin0)
    a1n = normalize_for_match(admin1)
    a2n = normalize_for_match(admin2)
    rows = kb_index.kb[
        (kb_index.kb["Admin0"].apply(lambda x: normalize_for_match(x) == a0n)) &
        (kb_index.kb["Admin1"].apply(lambda x: normalize_for_match(x) == a1n)) &
        (kb_index.kb["Admin2"].apply(lambda x: normalize_for_match(x) == a2n))
    ]
    if allowed_isos:
        rows = rows[rows["Admin0_ISO3"].isin({x.upper() for x in allowed_isos})]
    return len(rows) > 0


# =============================================================================
# Candidate & ISO constraints
# =============================================================================
def apply_exonym(text: str) -> str:
    """Apply exonym mapping cheaply (normalized key lookup)."""
    n = normalize_for_match(text)
    if n in EXONYM_MAP:
        return EXONYM_MAP[n]
    return text


def iso_filter_mode_from(iso_source: str, iso_set: Set[str]) -> str:
    if iso_source in {"gemini_admin0"}:
        return "strict"
    if iso_source == "cliff_name":
        return "strict" if len(iso_set) <= 2 else "soft"
    if iso_source == "doc_context":
        return "strict" if len(iso_set) <= 2 else "soft"
    return "soft"


def derive_iso_constraints(
    loc_name: str,
    info: dict,
    cliff_context: dict,
    kb_index: KBIndex,
    doc_context_isos: Set[str],
) -> Tuple[Set[str], str, str]:
    """
    Determine ISO constraint set for matching this location.

    Priority:
      1) Grounded Gemini admin0 -> strict
      2) CLiFF explicitly listed this name under iso(s) -> strict-ish
      3) Doc context ISO set (CLiFF keys + Gemini-resolved countries) -> soft/strict by size
    """
    name_norm = normalize_for_match(loc_name)

    # If Gemini admin0 is grounded, it dominates
    a0 = info.get("admin0", "Unknown")
    isos_from_a0 = admin0_isos(a0, kb_index)
    if isos_from_a0:
        return isos_from_a0, "gemini_admin0", iso_filter_mode_from("gemini_admin0", isos_from_a0)

    iso_by_name = kb_index.cliff_isos_for_name(loc_name, cliff_context)
    if iso_by_name and name_norm not in CLIFF_NAME_ISO_UNTRUSTED and not is_generic_location_name(loc_name):
        return iso_by_name, "cliff_name", iso_filter_mode_from("cliff_name", iso_by_name)

    if doc_context_isos:
        return doc_context_isos, "doc_context", iso_filter_mode_from("doc_context", doc_context_isos)

    return set(), "none", "soft"


def prepare_candidate_info(
    loc_name: str,
    raw_loc_info: dict,
    cliff_dict: dict,
    kb_index: KBIndex,
    doc_context_isos: Set[str],
) -> Tuple[dict, dict]:
    """
    Keep Gemini suggestions unless we can anchor/canonicalize to KB.

    - Resolve admin0 to KB if possible.
    - Do NOT wipe admin1/admin2 just because they don't exactly match KB.
      We'll try to canonicalize them later; otherwise keep as fallback.
    - If Gemini says country-level, only accept that if the country can be resolved
      to KB with reasonable confidence; else treat as UNKNOWN.
    - If Gemini provides no admin0 but doc_context_isos is very tight (1 ISO), fill admin0 from it.
    """
    raw_level = str((raw_loc_info or {}).get("location_level", "Unknown") or "Unknown")
    raw_level_norm = normalize_for_match(raw_level)

    orig = sanitize_location_info(raw_loc_info)
    dbg = {"admin0_resolve": None, "country_like": False, "raw_level": raw_level, "raw_level_norm": raw_level_norm}

    # Detect “country-like” (but do not blindly trust Gemini)
    country_like = (raw_level_norm in {"country", "nation", "admin0", "level0", "national"}) or kb_index.is_admin0_name(loc_name)
    dbg["country_like"] = bool(country_like)

    # Resolve admin0 if we have a signal
    a0_raw = orig.get("admin0", "Unknown")
    if a0_raw == "Unknown" and country_like:
        a0_raw = loc_name

    resolved_a0, a0_score, a0_method = (None, 0.0, "none")
    if a0_raw != "Unknown":
        allowed = None
        # if context is tight, allow it to guide admin0 resolution
        if doc_context_isos and len(doc_context_isos) <= 4:
            allowed = doc_context_isos
        resolved_a0, a0_score, a0_method = kb_index.resolve_admin0(a0_raw, allowed_isos=allowed, min_score=88)

    candidate = orig.copy()
    if resolved_a0:
        candidate["admin0"] = resolved_a0

    dbg["admin0_resolve"] = {"raw": a0_raw, "resolved": resolved_a0, "score": a0_score, "method": a0_method}

    # If no admin0 from Gemini but doc context is a single ISO, anchor to that country name
    if candidate.get("admin0") in {"Unknown", "", None} and doc_context_isos and len(doc_context_isos) == 1:
        iso = next(iter(doc_context_isos))
        if iso in kb_index.iso_to_admin0:
            candidate["admin0"] = kb_index.iso_to_admin0[iso]
            dbg["admin0_from_doc_iso"] = iso

    # Only keep ADMIN0 level if we can actually resolve to a KB country
    if country_like:
        if admin0_isos(candidate["admin0"], kb_index):
            candidate["location_level"] = "ADMIN0"
            candidate["admin1"] = "Unknown"
            candidate["admin2"] = "Unknown"
        else:
            candidate["location_level"] = "UNKNOWN"

    return candidate, dbg


# =============================================================================
# Query generation + matching guards
# =============================================================================
def extract_salient_tokens(text: str, kb_index: KBIndex, max_tokens: int = 3) -> List[str]:
    """
    Extract "useful" tokens for matching from a surface form.
    Priority: rarer tokens in KB first, then longer tokens.
    """
    n = normalize_for_match(text)
    toks = [t for t in n.split() if t and t not in TOKEN_STOPWORDS and len(t) >= 4]
    if not toks:
        return []
    # de-dup while preserving
    seen = set()
    uniq = []
    for t in toks:
        if t not in seen:
            uniq.append(t)
            seen.add(t)

    uniq.sort(key=lambda t: (kb_index.token_frequency(t, cols=["Admin1", "Admin2"]), -len(t), t))
    return uniq[:max_tokens]


def clean_admin_phrase(text: str) -> str:
    """
    Remove common admin qualifiers/prepositions so 'Tuva Republic' -> 'Tuva',
    'Republic of Ecuador' -> 'Ecuador', etc.
    """
    n = normalize_for_match(text)
    toks = [t for t in n.split() if t and t not in (TOKEN_STOPWORDS | {"of", "the"})]
    return " ".join(toks).strip()


def looks_like_alias(surface: str, candidate: str) -> bool:
    """
    True only when candidate is plausibly the same place as surface (typo/qualifier),
    not a parent admin unit (e.g., Kazarman vs Jalal-Abad province -> False).
    """
    s0 = normalize_for_match(surface)
    c0 = normalize_for_match(candidate)
    if not s0 or not c0:
        return False

    # strip admin qualifiers for alias checks
    s = clean_admin_phrase(s0)
    c = clean_admin_phrase(c0)

    if not s or not c:
        return False
    if s == c:
        return True

    # substring with a moderate length gap (allow "saint"/"province"/etc)
    if (s in c or c in s) and min(len(s), len(c)) >= 4 and abs(len(s) - len(c)) <= 18:
        return True

    # otherwise require very high similarity
    return fuzz.WRatio(s, c) >= 92


def content_tokens(text: str) -> Set[str]:
    """
    Content tokens for overlap checks (drop stopwords + very short tokens).
    """
    n = normalize_for_match(text)
    toks = [t for t in n.split() if t and t not in TOKEN_STOPWORDS and t not in {"of", "the"} and len(t) >= 4]
    return set(toks)


def has_nontrivial_token_overlap(a: str, b: str) -> bool:
    """
    Prevent matches driven only by generic admin words ("region", "province", ...):
    require overlap on at least one content token (len>=4) between query and candidate.
    """
    return len(content_tokens(a) & content_tokens(b)) > 0


@dataclass(frozen=True)
class QueryVariant:
    text: str
    kind: str  # surface | cleaned | alias_admin2 | alias_admin1 | token
    source: str  # loc_name | admin2 | admin1 | derived


def build_query_variants(loc_name: str, info: dict, kb_index: KBIndex) -> List[QueryVariant]:
    """
    Candidate query variants to avoid matching on generic tokens like 'republic'/'gulf'.
    Ordering is "best effort" priority list:
      1) loc_name (exonym-applied)
      2) cleaned loc_name (qualifiers removed)
      3) Gemini admin2/admin1 ONLY if they look like aliases of the surface form
      4) salient tokens from (1)-(3), sorted by rarity in KB
    """
    bases: List[QueryVariant] = []
    bases.append(QueryVariant(loc_name, "surface", "loc_name"))

    # Add Gemini admin fields ONLY if they look like aliases of the surface form.
    for k in ("admin2", "admin1"):
        v = info.get(k, "Unknown")
        if v not in {"Unknown", "", None} and looks_like_alias(loc_name, v):
            bases.append(QueryVariant(v, f"alias_{k}", k))

    # De-dup bases (normalized)
    base_norm_seen = set()
    bases2: List[QueryVariant] = []
    for b in bases:
        bn = normalize_for_match(b.text)
        if not bn or bn == "unknown":
            continue
        if bn in base_norm_seen:
            continue
        base_norm_seen.add(bn)
        bases2.append(b)

    variants: List[QueryVariant] = []
    seen = set()

    def _add(text: str, kind: str, source: str):
        text = str(text).strip()
        if not text:
            return
        xn = normalize_for_match(text)
        if not xn or xn == "unknown":
            return
        if xn in seen:
            return
        seen.add(xn)
        variants.append(QueryVariant(text, kind, source))

    # Priority: exonym + cleaned forms first
    for b in bases2:
        _add(apply_exonym(b.text), b.kind, b.source)
        cleaned = clean_admin_phrase(b.text)
        if cleaned and cleaned != normalize_for_match(b.text):
            _add(cleaned, "cleaned", b.source)

    # Salient tokens (rarity-based)
    tokens: List[str] = []
    for v in variants:
        tokens.extend(extract_salient_tokens(v.text, kb_index, max_tokens=3))

    # De-dup tokens and sort by rarity
    tok_seen = set()
    tok_uniq = []
    for t in tokens:
        if t not in tok_seen:
            tok_uniq.append(t)
            tok_seen.add(t)
    tok_uniq.sort(key=lambda t: (kb_index.token_frequency(t, cols=["Admin1", "Admin2"]), -len(t), t))

    for t in tok_uniq:
        _add(t, "token", "derived")

    return variants


def is_bad_fuzzy_target(value: str, query: str) -> bool:
    """
    Reject pathological fuzzy targets that are generic words (e.g., 'Republic', 'Gulf')
    that match many unrelated mentions via token overlap.
    """
    if not value:
        return True
    vn = normalize_for_match(value)
    if not vn:
        return True

    # reject very short targets unless exact
    if len(vn) <= 3 and normalize_for_match(query) != vn:
        return True

    vtoks = vn.split()
    if len(vtoks) == 1 and vtoks[0] in TOKEN_STOPWORDS:
        # allow only if query is exactly that token (rare)
        return normalize_for_match(query) != vtoks[0]
    return False


def single_token_multi_token_allowed(query: str, match_val: str) -> bool:
    """
    Allow single-token query -> multi-token match only when the extra tokens
    are admin-qualifiers (e.g. 'oblast', 'city', 'special', ...), or very short
    abbreviations like 'dki' (but not stopwords like 'new', 'north', ...).
    """
    q = normalize_for_match(query)
    m = normalize_for_match(match_val)
    qt = q.split()
    mt = m.split()
    if len(qt) != 1 or len(mt) <= 1:
        return True
    qtok = qt[0]
    if qtok not in mt:
        return False

    def is_short_abbrev(t: str) -> bool:
        return len(t) <= 3 and t.isalpha() and t not in TOKEN_STOPWORDS and t not in {"new", "old"}

    extras = [t for t in mt if t != qtok]
    return all((t in MULTI_TOKEN_QUALIFIERS) or is_short_abbrev(t) for t in extras)


def canonicalize_admin_field(
    value: str,
    col: str,
    kb_index: KBIndex,
    iso_set: Set[str],
    iso_mode: str,
    min_score: float,
) -> Tuple[str, Optional[dict]]:
    """Try to canonicalize a Gemini admin1/admin2 string to KB within ISO context; if can't, keep original."""
    if value in {"Unknown", "", None}:
        return value, None
    if not iso_set:
        return value, None

    best, score, _ = kb_index.fuzzy_best(value, allowed_isos=iso_set, cols=[col], iso_filter_mode=iso_mode)
    if best and score >= min_score:
        if is_bad_fuzzy_target(best, value):
            return value, None
        # extra safety: require some textual compatibility
        direct = fuzz.ratio(normalize_for_match(value), normalize_for_match(best))
        if direct >= 80 or normalize_for_match(value) in normalize_for_match(best) or normalize_for_match(best) in normalize_for_match(value):
            return best, {"from": value, "to": best, "score": score, "direct": direct, "col": col}
    return value, None


# =============================================================================
# Reconciliation logic
# =============================================================================
def reconcile_location(
    loc_name: str,
    raw_loc_info: dict,
    candidate_info: dict,
    kb_index: KBIndex,
    threshold: int,
    cliff_context: dict,
    doc_context_isos: Set[str],
    debug: bool = False,
) -> Tuple[dict, str, dict]:
    """
    Reconcile one location mention using KB + Gemini + CLiFF.

    Output behavior:
    - If we find a KB-grounded Admin1/Admin2, we output canonical ladder + level.
    - Promotion safety:
        * We ONLY promote when the KB match looks like an alias of the surface mention (loc_name).
        * We do NOT promote based solely on Gemini's admin1/admin2 fields if they don't resemble loc_name.
    - If we cannot ground, we keep Gemini fields (possibly admin0 resolved) and do NOT
      promote to ADMIN1/ADMIN2 merely because admin1/admin2 fields were filled.
    """
    cliff_context = cliff_context or {}
    info = sanitize_location_info(candidate_info)
    raw_level_norm = normalize_for_match(str((raw_loc_info or {}).get("location_level", "Unknown") or "Unknown"))

    dbg: Dict[str, Any] = {"method": None, "raw_level_norm": raw_level_norm}

    # 0) hard skips
    if is_generic_location_name(loc_name):
        updated = {"location_level": "UNKNOWN", "admin0": "Unknown", "admin1": "Unknown", "admin2": "Unknown"}
        return updated, "Generic/global term (skipped)", {"method": "generic_skip", "match": None}

    if is_pure_directional(loc_name):
        updated = {"location_level": "UNKNOWN", "admin0": "Unknown", "admin1": "Unknown", "admin2": "Unknown"}
        return updated, "Directional-only term (skipped)", {"method": "direction_skip", "match": None}

    # 1) If it looks like a country mention (and resolvable), resolve admin0
    locally_country_like = (raw_level_norm in {"country", "nation", "admin0", "level0", "national"}) or kb_index.is_admin0_name(loc_name)
    if locally_country_like:
        # Prefer CLiFF ISO if it explicitly lists this name AND is not untrusted
        iso_by_name = kb_index.cliff_isos_for_name(loc_name, cliff_context)
        iso_by_name = {iso for iso in iso_by_name if iso in kb_index.iso_to_admin0}

        chosen = None
        method_detail = None
        if iso_by_name and normalize_for_match(loc_name) not in CLIFF_NAME_ISO_UNTRUSTED:
            if len(iso_by_name) == 1:
                iso = next(iter(iso_by_name))
                chosen = kb_index.iso_to_admin0.get(iso)
                method_detail = f"cliff_iso:{iso}"

        if not chosen:
            raw = info["admin0"] if info["admin0"] != "Unknown" else loc_name
            chosen, score, m = kb_index.resolve_admin0(raw, allowed_isos=None, min_score=88)
            if chosen:
                method_detail = f"{m}:{score:.1f}"
            else:
                method_detail = f"unresolved_country:{raw}"

        # If still unresolved, keep Gemini label but do not force KB-like ladder
        if not chosen:
            updated = {
                "location_level": "ADMIN0",
                "admin0": info["admin0"] if info["admin0"] != "Unknown" else loc_name,
                "admin1": "Unknown",
                "admin2": "Unknown",
            }
            return updated, f"Country unresolved in KB ({method_detail})", {"method": "country_unresolved", "detail": method_detail, "match": {"evidence_term": loc_name, "kind": "surface", "matched_level": "ADMIN0", "matched_value": updated["admin0"], "score": None}}

        updated = {"location_level": "ADMIN0", "admin0": chosen, "admin1": "Unknown", "admin2": "Unknown"}
        return updated, f"Country resolved via {method_detail}", {"method": "country_resolve", "detail": method_detail, "match": {"evidence_term": loc_name, "kind": "surface", "matched_level": "ADMIN0", "matched_value": chosen, "score": None}}

    # 2) ISO constraints for this location
    iso_set, iso_source, iso_mode = derive_iso_constraints(loc_name, info, cliff_context, kb_index, doc_context_isos)
    dbg["iso_set"] = sorted(iso_set) if iso_set else []
    dbg["iso_source"] = iso_source
    dbg["iso_mode"] = iso_mode

    # Special-case: Crimea often mis-ISOed by CLiFF
    if normalize_for_match(loc_name) == "crimea" and iso_set == {"AUS"}:
        iso_set = set()
        iso_source = "cliff_name_dropped_for_crimea"
        iso_mode = "soft"
        dbg["iso_set"] = []
        dbg["iso_source"] = iso_source
        dbg["iso_mode"] = iso_mode

    # Anchor admin0 from ISO if we have a tight ISO context and no admin0 yet
    if info.get("admin0") in {"Unknown", "", None} and iso_set and len(iso_set) == 1 and iso_source in {"cliff_name", "doc_context"}:
        iso = next(iter(iso_set))
        if iso in kb_index.iso_to_admin0:
            info["admin0"] = kb_index.iso_to_admin0[iso]
            dbg["admin0_from_iso"] = iso

    # 3) If this is a feature phrase (gulf/sea/river...), do not attempt admin matching.
    # (We still keep anchored admin0 if present.)
    if looks_like_feature_phrase(loc_name):
        updated = info.copy()
        updated["location_level"] = "UNKNOWN"
        updated["admin1"] = "Unknown"
        updated["admin2"] = "Unknown"
        note = "Feature/geography phrase (skipped admin matching)"
        return updated, note, {"method": "feature_skip", **dbg, "match": None}

    # 4) Canonicalize Gemini admin1/admin2 hints (do NOT wipe if it fails)
    canon_changes = []
    if iso_set:
        new_a1, ch1 = canonicalize_admin_field(info["admin1"], "Admin1", kb_index, iso_set, iso_mode, min_score=93.0)
        if ch1 and new_a1 != info["admin1"]:
            info["admin1"] = new_a1
            canon_changes.append(ch1)

        new_a2, ch2 = canonicalize_admin_field(info["admin2"], "Admin2", kb_index, iso_set, iso_mode, min_score=95.0)
        if ch2 and new_a2 != info["admin2"]:
            info["admin2"] = new_a2
            canon_changes.append(ch2)

    if canon_changes:
        dbg["canonicalized"] = canon_changes

    # Strong admin0 (grounded) should never be overridden by a mismatch
    strong_a0 = info.get("admin0") not in {"Unknown", "", None} and bool(admin0_isos(info["admin0"], kb_index))

    # 5) Build query variants from the SURFACE mention (plus safe aliases + tokens).
    query_variants = build_query_variants(loc_name, info, kb_index)
    dbg["query_variants"] = [f"{v.kind}:{v.text}" for v in query_variants[:10]]

    # If Gemini explicitly said "Other", don't use level hinting; only accept matches with real evidence.
    gemini_hint_level = info.get("location_level", "UNKNOWN")
    if raw_level_norm == "other":
        gemini_hint_level = "UNKNOWN"

    # Token shortcut: only within strong ISO context (or very rare tokens with no ISO)
    has_strong_iso = bool(iso_set) and (iso_source in {"gemini_admin0", "cliff_name"} or (iso_source == "doc_context" and len(iso_set) <= 2))

    best_found = None  # (score, match_level, best_match, row_dict, QueryVariant)

    # 6) Try variants
    for v in query_variants:
        q = v.text
        qn = normalize_for_match(q)
        if not qn or qn == "unknown":
            continue
        if is_pure_directional(q):
            continue

        is_token_variant = (v.kind == "token" and len(qn.split()) == 1)

        # Dynamic threshold: keep aggressive matching, but tighten when no country context.
        base_thresh = float(threshold)
        if not iso_set and not strong_a0:
            base_thresh = max(base_thresh, 88.0)
            if is_token_variant:
                freq = kb_index.token_frequency(qn, cols=["Admin1", "Admin2"])
                if freq > 25:
                    base_thresh = max(base_thresh, 95.0)
                elif freq > 12:
                    base_thresh = max(base_thresh, 92.0)

        # A) Token shortcut (single token; low ambiguity by KB frequency)
        if is_token_variant:
            # allow without ISO only for very rare tokens
            allow_token_without_iso = kb_index.token_frequency(qn, cols=["Admin1", "Admin2"]) <= 3
            if has_strong_iso or allow_token_without_iso:
                direct_hits = kb_index.token_hits(q, cols=["Admin1", "Admin2"])
                # If multiple hits, we still can resolve safely using ISO + alias gating
                if direct_hits:
                    scored_candidates = []
                    for val, lvl in direct_hits:
                        cand = kb_index.rows_for_value(
                            lvl,
                            val,
                            allowed_isos=iso_set if iso_set else None,
                            prefer_admin0=info.get("admin0") if strong_a0 else None,
                            prefer_admin1=info.get("admin1") if info.get("admin1") not in {"Unknown", "", None} else None,
                            iso_filter_mode=iso_mode if iso_set else "soft",
                        )
                        if len(cand) == 0:
                            continue
                        row = cand.iloc[0].to_dict()
                        # Promotion safety: match must look like alias of surface mention
                        if not looks_like_alias(loc_name, val):
                            continue
                        # Also require some content token overlap unless very high similarity
                        if not has_nontrivial_token_overlap(loc_name, val) and fuzz.WRatio(qn, normalize_for_match(val)) < 95:
                            continue
                        # Prefer Admin2 if Gemini hinted Admin2; otherwise keep score by similarity
                        sim = fuzz.WRatio(clean_admin_phrase(loc_name), clean_admin_phrase(val))
                        tie = 0
                        if gemini_hint_level == "ADMIN2" and lvl == "Admin2":
                            tie = 1
                        scored_candidates.append((tie, sim, lvl, val, row))
                    if scored_candidates:
                        scored_candidates.sort(reverse=True)  # highest tie then sim
                        _, sim, lvl, val, row = scored_candidates[0]
                        updated = info.copy()
                        updated["admin0"] = row.get("Admin0") or updated.get("admin0", "Unknown")
                        if lvl == "Admin1":
                            updated["admin1"] = val
                            updated["admin2"] = "Unknown"
                            updated["location_level"] = "ADMIN1"
                        else:
                            updated["admin2"] = val
                            updated["admin1"] = row.get("Admin1") or updated.get("admin1", "Unknown")
                            updated["location_level"] = "ADMIN2"
                        note = f"Token match '{q}' → {lvl} '{val}' (sim={sim:.1f})"
                        match = {"evidence_term": q, "kind": v.kind, "matched_level": updated["location_level"], "matched_value": val, "score": 100.0, "iso_source": iso_source, "iso_set": sorted(iso_set) if iso_set else []}
                        return updated, note, {"method": "token", "hit": (val, lvl), "query": q, "match": match, **dbg}

        # B) Fuzzy match (Admin1/Admin2 only)
        attempt_iso = iso_set if iso_set else None
        iso_mode_eff = iso_mode if iso_set else "soft"

        a1_val, a1_score, _ = kb_index.fuzzy_best(q, allowed_isos=attempt_iso, cols=["Admin1"], iso_filter_mode=iso_mode_eff)
        a2_val, a2_score, _ = kb_index.fuzzy_best(q, allowed_isos=attempt_iso, cols=["Admin2"], iso_filter_mode=iso_mode_eff)

        # choose best, respecting hint
        best_match, best_score, match_level = None, 0.0, None
        if gemini_hint_level == "ADMIN2":
            if a2_val and a2_score >= (a1_score - 5):
                best_match, best_score, match_level = a2_val, a2_score, "Admin2"
            elif a1_val:
                best_match, best_score, match_level = a1_val, a1_score, "Admin1"
        elif gemini_hint_level == "ADMIN1":
            if a1_val and a1_score >= (a2_score - 5):
                best_match, best_score, match_level = a1_val, a1_score, "Admin1"
            elif a2_val:
                best_match, best_score, match_level = a2_val, a2_score, "Admin2"
        else:
            if a2_score > a1_score:
                best_match, best_score, match_level = a2_val, a2_score, "Admin2"
            else:
                best_match, best_score, match_level = a1_val, a1_score, "Admin1"

        if not best_match or not match_level:
            continue

        # Guard1: reject generic targets like 'Republic' or 'Gulf'
        if is_bad_fuzzy_target(best_match, q):
            continue

        # Guard2: reject single-token -> multi-token unless qualifiers/abbrev
        if not single_token_multi_token_allowed(q, best_match):
            continue

        # Guard3: require some content overlap between query and candidate (unless extremely high similarity)
        if not has_nontrivial_token_overlap(q, best_match) and fuzz.WRatio(qn, normalize_for_match(best_match)) < 95:
            continue

        # Guard4: Promotion safety — KB match must look like an alias of the SURFACE mention
        if not looks_like_alias(loc_name, best_match):
            continue

        # Guard5: avoid matching long phrases to a single generic token by overlap
        mn = normalize_for_match(best_match)
        if len(qn.split()) > 1 and len(mn.split()) == 1:
            if mn in qn.split():
                direct = fuzz.ratio(qn, mn)
                if direct < 85 and best_score < 97:
                    continue

        if best_score < base_thresh:
            continue

        cand_rows = kb_index.rows_for_value(
            match_level,
            best_match,
            allowed_isos=iso_set if iso_set else None,
            prefer_admin0=info.get("admin0") if strong_a0 else None,
            prefer_admin1=info.get("admin1") if info.get("admin1") not in {"Unknown", "", None} else None,
            iso_filter_mode=iso_mode_eff,
        )
        if len(cand_rows) == 0:
            continue

        row = cand_rows.iloc[0].to_dict()

        if strong_a0 and normalize_for_match(row["Admin0"]) != normalize_for_match(info["admin0"]):
            continue

        # Store best found (keep highest score; tie-break: prefer Admin2 if hinted)
        candidate_tuple = (best_score, match_level, best_match, row, v)
        if best_found is None:
            best_found = candidate_tuple
        else:
            prev_score, prev_level, _, _, _ = best_found
            if best_score > prev_score + 0.1:
                best_found = candidate_tuple
            elif abs(best_score - prev_score) <= 0.1:
                if gemini_hint_level == "ADMIN2" and match_level == "Admin2" and prev_level != "Admin2":
                    best_found = candidate_tuple

    if best_found:
        best_score, match_level, best_match, row, used_variant = best_found
        updated = info.copy()
        updated["admin0"] = row.get("Admin0") or updated.get("admin0", "Unknown")
        if match_level == "Admin1":
            updated["admin1"] = best_match
            updated["admin2"] = "Unknown"
            updated["location_level"] = "ADMIN1"
            note = f"Matched '{used_variant.text}' → Admin1 '{best_match}' (score={best_score:.1f})"
        else:
            updated["admin2"] = best_match
            updated["admin1"] = row.get("Admin1") or updated.get("admin1", "Unknown")
            updated["location_level"] = "ADMIN2"
            note = f"Matched '{used_variant.text}' → Admin2 '{best_match}' (score={best_score:.1f})"

        match = {"evidence_term": used_variant.text, "kind": used_variant.kind, "matched_level": updated["location_level"], "matched_value": best_match, "score": float(best_score), "iso_source": iso_source, "iso_set": sorted(iso_set) if iso_set else []}
        return updated, note, {"method": f"fuzzy_{match_level.lower()}", "score": best_score, "query": used_variant.text, "match": match, **dbg}

    # 7) If none matched: KEEP Gemini info (do not erase), and do not auto-promote level.
    updated = info.copy()
    updated["location_level"] = updated.get("location_level", "UNKNOWN")
    if updated["location_level"] not in {"ADMIN0", "ADMIN1", "ADMIN2"}:
        updated["location_level"] = "UNKNOWN"
    note = "No reliable KB match"
    return updated, note, {"method": "no_match_keep_gemini", "match": None, **dbg}


# =============================================================================
# Revisit logic (less sensitive)
# =============================================================================
def validate_reconciled(
    reconciled_info: dict,
    loc_name: str,
    orig_raw_info: dict,
    kb_index: KBIndex,
    cliff_dict: dict,
    doc_context_isos: Set[str],
) -> Tuple[bool, str]:
    """
    Less-sensitive revisit:
    - never revisit generic/global or directional-only
    - never revisit if we cannot ground admin0 to KB (no clear country)
    - country mentions: ok as long as admin0 is KB-known (or at least not contradictory)
    - only revisit for admin-unit-like mentions (province/oblast/region/etc) where we
      have a KB-grounded country but cannot ground admin1/admin2 ladder.
    """
    info = sanitize_location_info(reconciled_info)
    orig = sanitize_location_info(orig_raw_info or {})
    cliff_dict = cliff_dict or {}

    if is_generic_location_name(loc_name) or is_pure_directional(loc_name):
        return True, "generic/directional (no revisit)"

    # Determine if admin0 is grounded to KB
    a0, a1, a2 = info["admin0"], info["admin1"], info["admin2"]
    if a0 in {"Unknown", "", None}:
        return True, "no country resolved (no revisit)"

    if not admin0_isos(a0, kb_index):
        return False, f"admin0 '{a0}' not found in KB"

    raw_level_norm = normalize_for_match(str((orig_raw_info or {}).get("location_level", "Unknown") or "Unknown"))
    if raw_level_norm in {"country", "nation", "admin0", "level0", "national"} or kb_index.is_admin0_name(loc_name):
        return True, "country-level ok"

    # Feature phrases: no revisit
    if looks_like_feature_phrase(loc_name):
        return True, "feature phrase (no revisit)"

    # If it looks like a POI, don't revisit unless ladder is obviously bogus
    if looks_like_poi(loc_name):
        return True, "POI-like (likely not in KB; no revisit)"

    # Require a strong “this is admin-unit-like” signal before revisiting
    likely_admin = looks_like_admin_unit(loc_name) or looks_like_admin_unit(orig.get("admin1", "")) or looks_like_admin_unit(orig.get("admin2", ""))
    gemini_admin_hint = orig.get("location_level") in {"ADMIN1", "ADMIN2"}
    cliff_name_hit = bool(kb_index.cliff_isos_for_name(loc_name, cliff_dict))

    strong_signal = likely_admin or (gemini_admin_hint and not looks_like_poi(loc_name)) or cliff_name_hit
    if not strong_signal:
        return True, "no strong admin-unit signal (no revisit)"

    # Also require that doc context is not wildly broad
    if doc_context_isos and len(doc_context_isos) > 8 and not cliff_name_hit and not gemini_admin_hint:
        return True, "context too broad & weak signal (no revisit)"

    if info["location_level"] == "ADMIN2":
        if is_kb_triple(a0, a1, a2, kb_index):
            return True, "admin2 grounded"
        return False, f"admin2 ladder not in KB: ({a0},{a1},{a2})"

    if info["location_level"] == "ADMIN1":
        if is_kb_pair(a0, a1, kb_index):
            return True, "admin1 grounded"
        return False, f"admin1 ladder not in KB: ({a0},{a1})"

    return False, "country known but admin1/admin2 not grounded for admin-unit-like mention"


# =============================================================================
# Doc-level reconciliation (Gemini + optional CLiFF-only)
# =============================================================================
def add_cliff_only_locations(
    reconciled: Dict[str, dict],
    debug_map: Dict[str, dict],
    cliff_dict: dict,
    kb_index: KBIndex,
    max_add: int = 25,
) -> int:
    """
    Conservative:
    - Only add CLiFF-only locations if they are NOT already present in Gemini keys
    - Only add when we can EXACT-match to KB Admin1/Admin2 for that ISO (unique row)
      OR when it's clearly a country mention resolvable via ISO->Admin0.
    """
    if not isinstance(cliff_dict, dict) or not cliff_dict:
        return 0

    existing_norm = {normalize_for_match(k) for k in reconciled.keys()}
    added = 0

    for iso, places in cliff_dict.items():
        iso3 = str(iso).upper().strip()
        if iso3 not in kb_index.iso_to_admin0:
            continue
        if not isinstance(places, (list, tuple)):
            continue

        for p in places:
            if added >= max_add:
                return added
            if not p:
                continue
            name = str(p).strip()
            if not name:
                continue

            n = normalize_for_match(name)
            if not n or n == "unknown":
                continue
            if n in existing_norm:
                continue
            if is_generic_location_name(name) or is_pure_directional(name) or looks_like_feature_phrase(name):
                continue
            if n in CLIFF_NAME_ISO_UNTRUSTED:
                continue
            # skip very generic single-token names
            if len(n.split()) == 1 and n in TOKEN_STOPWORDS:
                continue

            # First: treat as country mention if resolvable within ISO
            resolved_a0, score, method = kb_index.resolve_admin0(name, allowed_isos={iso3}, min_score=90)
            if resolved_a0:
                reconciled[name] = {"location_level": "ADMIN0", "admin0": resolved_a0, "admin1": "Unknown", "admin2": "Unknown"}
                debug_map[name] = {"note": f"Added from CLiFF ({iso3}) as country via {method}:{score:.1f}", "method": "cliff_only_country", "iso": iso3}
                existing_norm.add(n)
                added += 1
                continue

            # Admin1 exact (unique)
            cand_a1 = kb_index.rows_for_value("Admin1", name, allowed_isos={iso3}, iso_filter_mode="strict")
            if len(cand_a1) == 1:
                row = cand_a1.iloc[0]
                reconciled[name] = {"location_level": "ADMIN1", "admin0": row["Admin0"], "admin1": row["Admin1"], "admin2": "Unknown"}
                debug_map[name] = {"note": f"Added from CLiFF ({iso3}) exact KB Admin1", "method": "cliff_only_admin1_exact", "iso": iso3}
                existing_norm.add(n)
                added += 1
                continue

            # Admin2 exact (unique)
            cand_a2 = kb_index.rows_for_value("Admin2", name, allowed_isos={iso3}, iso_filter_mode="strict")
            if len(cand_a2) == 1:
                row = cand_a2.iloc[0]
                reconciled[name] = {"location_level": "ADMIN2", "admin0": row["Admin0"], "admin1": row["Admin1"], "admin2": row["Admin2"]}
                debug_map[name] = {"note": f"Added from CLiFF ({iso3}) exact KB Admin2", "method": "cliff_only_admin2_exact", "iso": iso3}
                existing_norm.add(n)
                added += 1
                continue

    return added


def build_doc_context_isos(gemini_dict: dict, cliff_dict: dict, kb_index: KBIndex) -> Set[str]:
    """Doc-level country context: CLiFF ISOs + any Gemini-resolved admin0 values."""
    doc_isos = set(kb_index.get_iso_set(cliff_dict))
    if isinstance(gemini_dict, dict):
        for loc_name, loc_info in gemini_dict.items():
            if not loc_name or normalize_for_match(loc_name) == "unknown":
                continue

            raw_level_norm = normalize_for_match(str((loc_info or {}).get("location_level", "Unknown") or "Unknown"))
            orig = sanitize_location_info(loc_info)

            raw = orig.get("admin0", "Unknown")
            if raw == "Unknown" and (raw_level_norm in {"country", "nation", "admin0", "level0", "national"} or kb_index.is_admin0_name(loc_name)):
                raw = loc_name
            if raw != "Unknown":
                resolved, _, _ = kb_index.resolve_admin0(raw, allowed_isos=None, min_score=88)
                if resolved:
                    doc_isos |= admin0_isos(resolved, kb_index)
    return doc_isos


def deduplicate_reconciled(
    reconciled: Dict[str, dict],
    debug_map: Dict[str, dict],
) -> Tuple[Dict[str, dict], Dict[str, dict], int]:
    """
    Deduplicate surface forms that map to the same ladder.

    Signature:
      ADMIN0: (ADMIN0, admin0)
      ADMIN1: (ADMIN1, admin0, admin1)
      ADMIN2: (ADMIN2, admin0, admin1, admin2)

    Keep the most "standardized" key:
      - Prefer key that equals the canonical KB name for that level (admin0/admin1/admin2)
      - Else prefer shorter / fewer stopword tokens
    """
    def sig(name: str, info: dict):
        lvl = info.get("location_level", "UNKNOWN")
        a0 = normalize_for_match(info.get("admin0", ""))
        a1 = normalize_for_match(info.get("admin1", ""))
        a2 = normalize_for_match(info.get("admin2", ""))
        if lvl == "ADMIN0":
            return ("ADMIN0", a0)
        if lvl == "ADMIN1":
            return ("ADMIN1", a0, a1)
        if lvl == "ADMIN2":
            return ("ADMIN2", a0, a1, a2)
        return None

    def canonical_name_for(info: dict) -> str:
        lvl = info.get("location_level", "UNKNOWN")
        if lvl == "ADMIN0":
            return info.get("admin0", "") or ""
        if lvl == "ADMIN1":
            return info.get("admin1", "") or ""
        if lvl == "ADMIN2":
            return info.get("admin2", "") or ""
        return ""

    def key_quality(name: str, info: dict) -> Tuple[int, int, int]:
        """
        Lower is better.
          0: exact canonical match at this level, else 1
          token penalty: count of stopword tokens in name
          length penalty: len(normalized string)
        """
        n = normalize_for_match(name)
        canon = normalize_for_match(canonical_name_for(info))
        exact = 0 if canon and n == canon else 1
        toks = n.split()
        stop_pen = sum(1 for t in toks if t in TOKEN_STOPWORDS or t in {"of", "the"})
        return (exact, stop_pen, len(n))

    groups: Dict[Tuple, List[str]] = defaultdict(list)
    for name, info in reconciled.items():
        s = sig(name, info)
        if s is None:
            continue
        groups[s].append(name)

    dropped = 0
    out = dict(reconciled)
    out_debug = dict(debug_map)

    for s, names in groups.items():
        if len(names) <= 1:
            continue
        candidates = sorted(names, key=lambda nm: key_quality(nm, reconciled[nm]))
        keep = candidates[0]
        for nm in candidates[1:]:
            out.pop(nm, None)
            dropped += 1
            if keep in out_debug:
                out_debug[keep].setdefault("dedup_dropped", []).append(nm)
            out_debug.pop(nm, None)

    return out, out_debug, dropped


def reconcile_doc(
    gemini_dict: dict,
    cliff_dict: dict,
    kb_index: KBIndex,
    threshold: int = 85,
    debug: bool = False,
    include_cliff_only: bool = True,
) -> Tuple[dict, dict, list, dict, dict, list]:
    """
    Reconcile a document's locations.

    Returns:
      reconciled: dict {name: {location_level, admin0, admin1, admin2}}
      debug_map:  per-name debug
      revisit:    locations needing manual review
      meta:       summary stats
      evidence:   per-name match evidence (compact)
      match_terms: sorted unique list of evidence terms used for matches
    """
    gemini_dict = gemini_dict or {}
    if gemini_dict is None or (not isinstance(gemini_dict, dict)):
        gemini_dict = {}

    cliff_dict = cliff_dict or {}
    if not isinstance(cliff_dict, dict):
        cliff_dict = {}

    doc_context_isos = build_doc_context_isos(gemini_dict, cliff_dict, kb_index)

    reconciled: Dict[str, dict] = {}
    debug_map: Dict[str, dict] = {}
    revisit: List[dict] = []

    evidence_map: Dict[str, dict] = {}
    match_terms: Set[str] = set()

    # Process Gemini items first
    for loc_name, loc_info in gemini_dict.items():
        if not loc_name or normalize_for_match(loc_name) == "unknown":
            continue

        # Keep generics out (but retain in debug to understand)
        if is_generic_location_name(loc_name) or is_pure_directional(loc_name):
            debug_map[loc_name] = {"note": "Generic/directional term skipped", "method": "generic_skip", "original": loc_info, "match": None}
            evidence_map[loc_name] = {}
            continue

        candidate, prep_dbg = prepare_candidate_info(loc_name, loc_info, cliff_dict, kb_index, doc_context_isos)

        updated, note, dbg = reconcile_location(
            loc_name=loc_name,
            raw_loc_info=loc_info,
            candidate_info=candidate,
            kb_index=kb_index,
            threshold=threshold,
            cliff_context=cliff_dict,
            doc_context_isos=doc_context_isos,
            debug=debug,
        )

        reconciled[loc_name] = updated
        debug_map[loc_name] = {
            "note": note,
            "original": sanitize_location_info(loc_info),
            "candidate_input": candidate,
            **prep_dbg,
            **dbg,
        }

        m = dbg.get("match") if isinstance(dbg, dict) else None
        if isinstance(m, dict) and m.get("evidence_term"):
            evidence_map[loc_name] = m
            match_terms.add(str(m["evidence_term"]))
        else:
            evidence_map[loc_name] = {}

        ok, reason = validate_reconciled(updated, loc_name, loc_info, kb_index, cliff_dict, doc_context_isos)
        if not ok:
            revisit.append({"location": loc_name, "reason": reason, "note": note, "original": sanitize_location_info(loc_info), "reconciled": updated})

    # Optionally add CLiFF-only (exact KB matches only)
    n_added_cliff_only = 0
    if include_cliff_only:
        n_added_cliff_only = add_cliff_only_locations(reconciled, debug_map, cliff_dict, kb_index)

    # Deduplicate by ladder signature
    reconciled_dedup, debug_dedup, n_dedup_dropped = deduplicate_reconciled(reconciled, debug_map)

    status = "OK"
    if (len(gemini_dict) == 0) and (len(cliff_dict) == 0):
        status = "NO_LOCATIONS"
    elif len(revisit) > 0:
        status = "REVISIT"

    meta = {
        "status": status,
        "n_gemini": len(gemini_dict) if isinstance(gemini_dict, dict) else 0,
        "n_reconciled_before_dedup": len(reconciled),
        "n_reconciled": len(reconciled_dedup),
        "n_dedup_dropped": n_dedup_dropped,
        "n_revisit": len(revisit),
        "n_added_cliff_only": n_added_cliff_only,
        "cliff_isos": sorted(kb_index.get_iso_set(cliff_dict)) if cliff_dict else [],
        "doc_context_isos": sorted(doc_context_isos) if doc_context_isos else [],
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return reconciled_dedup, debug_dedup, revisit, meta, evidence_map, sorted(match_terms)


# =============================================================================
# Runner
# =============================================================================
@dataclass
class ReconcileRunner:
    uri: str
    db_name: str
    countries: list  # ISO3 list like ["IND","IDN",...]
    kb_path: str = "/home/diego/peace/KB_DF.csv"
    start_year: int = 2012
    end_year: int = 2025
    end_month: int = 12
    threshold: int = 85
    dry_run: bool = False
    debug_rows_per_collection: int = 3
    force: bool = False  # if True, overwrite existing reconciled field
    include_cliff_only: bool = True

    def __post_init__(self):
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        kb_df = pd.read_csv(self.kb_path)
        self.kb_index = KBIndex(kb_df)

    def _source_domains_for_countries(self):
        return [
            doc.get("source_domain")
            for doc in self.db["sources"].find(
                {"primary_location": {"$in": self.countries}, "include": True},
                {"source_domain": 1},
            )
            if doc.get("source_domain")
        ]

    def run(self):
        loc_domains = self._source_domains_for_countries()

        # Match docs that have at least one of the two fields
        base_query = {
            "source_domain": {"$in": loc_domains},
            "environmental_binary.result": "Yes",
            "$or": [
                {"env_classifier.env_max": {"$nin": ["-999", "environmental -999", None]}},
                {"env_classifier.env_sec": {"$nin": ["-999", "environmental -999", None]}},
            ],
            "$and": [
                {"$or": [
                    {"gemini_locations": {"$exists": True}},
                    {"gemini_locations_normalized": {"$exists": True}},
                    {"cliff_locations": {"$exists": True}},
                ]}
            ],
        }

        if not self.force:
            base_query[RECONCILED_FIELD] = {"$exists": False}

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                if year == self.end_year and month > self.end_month:
                    break

                collection_name = f"articles-{year}-{month}"
                collection = self.db[collection_name]

                total = collection.count_documents(base_query)
                if total == 0:
                    continue

                print("\n\n" + "=" * 90)
                print(f"📦 {collection_name}: {total} doc(s) | countries={self.countries}")
                print("=" * 90)

                cursor = collection.find(
                    base_query,
                    {
                        "_id": 1,
                        "gemini_locations": 1,
                        "gemini_locations_normalized": 1,
                        "cliff_locations": 1,
                    },
                )

                revisit_docs = 0
                processed = 0

                for doc in cursor:
                    processed += 1
                    doc_id = doc["_id"]

                    gem = doc.get("gemini_locations_normalized") or doc.get("gemini_locations") or {}
                    cliff = doc.get("cliff_locations") or {}

                    gem = safe_load_dict(gem)
                    cliff = safe_load_dict(cliff)

                    verbose = processed <= self.debug_rows_per_collection

                    reconciled, debug_map, revisit, meta, evidence, match_terms = reconcile_doc(
                        gemini_dict=gem,
                        cliff_dict=cliff,
                        kb_index=self.kb_index,
                        threshold=self.threshold,
                        debug=verbose,
                        include_cliff_only=self.include_cliff_only,
                    )

                    if meta["status"] == "REVISIT":
                        revisit_docs += 1

                    print(
                        f"[{collection_name}] ({processed}/{total}) _id={doc_id} "
                        f"reconciled={meta['n_reconciled']} (dropped={meta['n_dedup_dropped']}) revisit={meta['n_revisit']} "
                        f"added_cliff_only={meta['n_added_cliff_only']} "
                        f"cliff_isos={meta['cliff_isos']} status={meta['status']}"
                    )

                    if verbose and meta["n_reconciled"] > 0:
                        shown = 0
                        for loc_name, d in debug_map.items():
                            if shown >= 12:
                                break
                            print(f"   - {loc_name}: {d.get('note','')}")
                            shown += 1

                    update_doc = {
                        RECONCILED_FIELD: reconciled,
                        RECONCILED_META_FIELD: meta,
                        RECONCILED_DEBUG_FIELD: debug_map,
                        RECONCILED_REVISIT_FIELD: revisit,
                        RECONCILED_EVIDENCE_FIELD: evidence,
                        RECONCILED_MATCH_TERMS_FIELD: match_terms,
                    }

                    if not self.dry_run:
                        collection.update_one({"_id": doc_id}, {"$set": update_doc})

                print(f"✅ Done {collection_name}: processed={processed}, revisit_docs={revisit_docs}/{processed}")


if __name__ == "__main__":
    # For security, set MONGO_URI via environment variable.
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true")
    if not MONGO_URI:
        raise RuntimeError("Please set MONGO_URI in your environment (do not hardcode credentials).")
    DB_NAME = os.environ.get("MONGO_DB", "ml4p")

    runner = ReconcileRunner(
        uri=MONGO_URI,
        db_name=DB_NAME,
        countries=[
            'ALB', 'BEN', 'COL', 'ECU', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA',
            'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD',
            'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 'UZB',
            'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'PER', 'DZA', 'MKD', 'SSD', 'LBR', 'PAK', 'NPL', 'NAM', 'BFA',
            'DOM', 'TLS', 'SLB', 'CRI', 'PAN'
        ],
        kb_path="/home/diego/peace/KB_DF.csv",
        start_year=2012,
        end_year=2025,
        end_month=12,
        threshold=85,
        dry_run=False,
        debug_rows_per_collection=3,
        force=True,              # set False to only fill missing
        include_cliff_only=True, # conservative: exact KB matches only
    )

    runner.run()
