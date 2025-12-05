import os
import re
import unicodedata
import json
import ast
from collections import defaultdict
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pymongo import MongoClient
from rapidfuzz import process, fuzz


# =========================
# Config: DB field names
# =========================
RECONCILED_FIELD = "reconciled_locations"
RECONCILED_META_FIELD = "reconciled_locations_meta"
RECONCILED_DEBUG_FIELD = "reconciled_locations_debug"

# "Revisit" = actionable review items: country is known but admin mapping is suspicious/incomplete
RECONCILED_REVISIT_FIELD = "reconciled_locations_revisit"

# "Unresolved" = could not reliably ground to a KB country (admin0)
RECONCILED_UNRESOLVED_FIELD = "reconciled_locations_unresolved"


# =========================
# Tunables
# =========================
DEFAULT_THRESHOLD = 85

# If CLiFF gives a small ISO set, we can *use* it as a helpful constraint for matching.
ISO_FILTER_MAX = 3

# If CLiFF gives a very small ISO set, mismatch is meaningful enough to flag.
ISO_ENFORCE_MAX = 2


# =========================
# Helpers: parsing + normalization
# =========================
def safe_load_dict(v: Any) -> Dict[str, Any]:
    """Accept dict (Mongo), JSON string, or Python-literal string. Return {} on failure."""
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


def normalize_for_match(s: Any) -> str:
    """Accent + punctuation tolerant normalization used for all matching."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def sanitize_location_info(info: Any) -> Dict[str, str]:
    info = info or {}
    out = {
        "location_level": str(info.get("location_level", "Unknown") or "Unknown").upper(),
        "admin0": info.get("admin0", "Unknown") or "Unknown",
        "admin1": info.get("admin1", "Unknown") or "Unknown",
        "admin2": info.get("admin2", "Unknown") or "Unknown",
    }
    if out["location_level"] not in {"ADMIN0", "ADMIN1", "ADMIN2", "UNKNOWN"}:
        out["location_level"] = "UNKNOWN"
    return out


# =========================
# Name heuristics (used ONLY for revisit gating)
# =========================
ADMIN_CUES = {
    "province", "state", "region", "district", "county", "municipality", "prefecture",
    "governorate", "department", "territory", "division", "subcounty", "ward", "village",
    "commune", "township", "parish", "sector", "canton", "subdivision"
}
POI_CUES = {
    "school", "primary", "secondary", "university", "college", "hospital", "clinic",
    "airport", "stadium", "port", "bridge", "road", "highway", "mine", "plant",
    "factory", "dam", "market", "prison", "mosque", "church", "temple", "hotel",
    "station", "camp", "base"
}


def looks_like_admin_unit(name: str) -> bool:
    toks = set(normalize_for_match(name).split())
    return any(c in toks for c in ADMIN_CUES)


def looks_like_poi(name: str) -> bool:
    n = normalize_for_match(name)
    toks = set(n.split())
    if any(c in toks for c in POI_CUES):
        return True
    if any(ch.isdigit() for ch in str(name)):
        return True
    # long entity names are typically POIs/entities, not admin units
    return len(toks) >= 5


# =========================
# KB Index for fast lookups
# =========================
class KBIndex:
    LEVELS = ["Admin0", "Admin1", "Admin2"]

    def __init__(self, kb_df: pd.DataFrame):
        kb = kb_df.copy()
        for col in ["Admin0", "Admin1", "Admin2", "Admin0_ISO3"]:
            if col not in kb.columns:
                raise ValueError(f"KB missing required column: {col}")
            kb[col] = kb[col].fillna("").astype(str)

        kb["Admin0_ISO3"] = kb["Admin0_ISO3"].str.upper().str.strip()
        self.kb = kb

        self.value_to_indices = {col: defaultdict(list) for col in self.LEVELS}
        self.norm_to_values = {col: defaultdict(set) for col in self.LEVELS}
        self.token_index = {col: defaultdict(set) for col in self.LEVELS}

        self.choices_global = {col: [] for col in self.LEVELS}
        self.choices_by_iso = {col: defaultdict(list) for col in self.LEVELS}
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

                for tok in normalize_for_match(val).split():
                    if tok:
                        self.token_index[col][tok].add(val)

        for col in self.LEVELS:
            self.choices_global[col] = sorted([v for v in kb[col].dropna().unique().tolist() if v])
            for iso, s in tmp_iso_sets[col].items():
                self.choices_by_iso[col][iso] = sorted(s)

    @staticmethod
    def get_iso_set(cliff_context: Any) -> set:
        """Extract valid ISO3 keys from CLiFF context dict; filter junk like NONE."""
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

    def token_hits(self, name: str) -> List[Tuple[str, str]]:
        clean = normalize_for_match(name)
        if not clean or len(clean.split()) > 1:
            return []
        hits: List[Tuple[str, str]] = []
        for col in self.LEVELS:
            for val in self.token_index[col].get(clean, []):
                hits.append((val, col))
        return hits

    def rows_for_value(
        self,
        col: str,
        value: str,
        allowed_isos: Optional[set] = None,
        prefer_admin0: Optional[str] = None,
        prefer_admin1: Optional[str] = None,
    ) -> pd.DataFrame:
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

        # Soft ISO filter: apply only if it actually leaves something.
        if allowed_isos:
            allowed_isos = {x.upper() for x in allowed_isos}
            cand2 = cand[cand["Admin0_ISO3"].isin(allowed_isos)]
            if len(cand2) > 0:
                cand = cand2

        if prefer_admin0 and prefer_admin0 not in {"Unknown", "", None}:
            cand2 = cand[cand["Admin0"].str.casefold() == str(prefer_admin0).casefold()]
            if len(cand2) > 0:
                cand = cand2

        if prefer_admin1 and prefer_admin1 not in {"Unknown", "", None}:
            cand2 = cand[cand["Admin1"].str.casefold() == str(prefer_admin1).casefold()]
            if len(cand2) > 0:
                cand = cand2

        return cand.sort_values(["Admin0", "Admin1", "Admin2"]).drop_duplicates()

    def fuzzy_best(self, name: str, allowed_isos: Optional[set] = None) -> Tuple[Optional[str], float, Optional[str]]:
        """
        Best fuzzy across Admin0/Admin1/Admin2.
        If allowed_isos is small, constrain choices. Otherwise keep universal matching.
        """
        best_val: Optional[str] = None
        best_score: float = 0.0
        best_col: Optional[str] = None

        iso_list = [x.upper() for x in (allowed_isos or []) if x]
        use_iso_filter = bool(iso_list) and len(iso_list) <= ISO_FILTER_MAX

        for col in self.LEVELS:
            choices = self.choices_global[col]
            if use_iso_filter:
                union: List[str] = []
                for iso in iso_list:
                    union.extend(self.choices_by_iso[col].get(iso, []))
                if union:
                    choices = sorted(set(union))

            m = process.extractOne(
                name,
                choices,
                scorer=fuzz.token_set_ratio,
                processor=normalize_for_match,
            )
            if m and m[1] > best_score:
                best_val, best_score, best_col = m[0], float(m[1]), col

        return best_val, best_score, best_col


# =========================
# KB grounding helpers
# =========================
def admin0_isos(admin0: str, kb_index: KBIndex) -> set:
    if admin0 in {None, "", "Unknown"}:
        return set()
    rows = kb_index.kb[kb_index.kb["Admin0"].str.casefold() == str(admin0).casefold()]
    return set(rows["Admin0_ISO3"].dropna().astype(str).str.upper().unique().tolist())


def kb_combo_ok(info: Dict[str, str], kb_index: KBIndex) -> bool:
    info = sanitize_location_info(info)
    a0, a1, a2 = info["admin0"], info["admin1"], info["admin2"]
    if a0 in {"Unknown", "", None}:
        return False
    if not admin0_isos(a0, kb_index):
        return False

    if a2 not in {"Unknown", "", None}:
        rows = kb_index.kb[
            (kb_index.kb["Admin0"].str.casefold() == str(a0).casefold()) &
            (kb_index.kb["Admin1"].str.casefold() == str(a1).casefold()) &
            (kb_index.kb["Admin2"].str.casefold() == str(a2).casefold())
        ]
        return len(rows) > 0

    if a1 not in {"Unknown", "", None}:
        rows = kb_index.kb[
            (kb_index.kb["Admin0"].str.casefold() == str(a0).casefold()) &
            (kb_index.kb["Admin1"].str.casefold() == str(a1).casefold())
        ]
        return len(rows) > 0

    return True  # country-only is always OK


def is_grounded(info: Dict[str, str], kb_index: KBIndex, allowed_isos: Optional[set] = None) -> bool:
    """
    Used only for hallucination guard. Conservative (not overly sensitive):
    only enforce ISO mismatch if CLiFF ISO set is small/confident.
    """
    info = sanitize_location_info(info)
    allowed_isos = {x.upper() for x in (allowed_isos or []) if x}
    a0, a1, a2 = info["admin0"], info["admin1"], info["admin2"]

    if a0 in {"Unknown", "", None}:
        return False

    isos = admin0_isos(a0, kb_index)
    if not isos:
        return False

    if allowed_isos and len(allowed_isos) <= ISO_ENFORCE_MAX and isos.isdisjoint(allowed_isos):
        return False

    if a2 not in {"Unknown", "", None}:
        rows = kb_index.kb[
            (kb_index.kb["Admin0"].str.casefold() == str(a0).casefold()) &
            (kb_index.kb["Admin1"].str.casefold() == str(a1).casefold()) &
            (kb_index.kb["Admin2"].str.casefold() == str(a2).casefold())
        ]
        if allowed_isos and len(allowed_isos) <= ISO_FILTER_MAX:
            rows = rows[rows["Admin0_ISO3"].isin(allowed_isos)]
        return len(rows) > 0

    if a1 not in {"Unknown", "", None}:
        rows = kb_index.kb[
            (kb_index.kb["Admin0"].str.casefold() == str(a0).casefold()) &
            (kb_index.kb["Admin1"].str.casefold() == str(a1).casefold())
        ]
        if allowed_isos and len(allowed_isos) <= ISO_FILTER_MAX:
            rows = rows[rows["Admin0_ISO3"].isin(allowed_isos)]
        return len(rows) > 0

    return True


def admin0_name_from_iso(iso: str, kb_index: KBIndex) -> Optional[str]:
    iso = (iso or "").upper().strip()
    rows = kb_index.kb[kb_index.kb["Admin0_ISO3"] == iso]
    if len(rows) == 0:
        return None
    return rows.iloc[0]["Admin0"]


# =========================
# Core: reconcile one location (Gemini entry) using KB + CLiFF
# =========================
def reconcile_location(
    name: str,
    location_info: Dict[str, str],
    kb_index: KBIndex,
    threshold: int = DEFAULT_THRESHOLD,
    cliff_context: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> Tuple[Dict[str, str], str, Dict[str, Any]]:
    """
    Token-first → fuzzy → CLiFF ISO fallback → rule-based upfill.

    CONFIRMED behavior: even if admin0 is missing/unknown, Admin1/Admin2 matches
    can upfill admin0 from the KB row ("universal match" from lower level → country).
    """
    info = sanitize_location_info(location_info)
    allowed_isos = kb_index.get_iso_set(cliff_context)
    iso_hint = allowed_isos if (allowed_isos and len(allowed_isos) <= ISO_FILTER_MAX) else set()

    # --- TOKEN FIRST (single-token only)
    direct_hits = kb_index.token_hits(name)
    multiple_token_hits = False

    if direct_hits and len(direct_hits) == 1:
        val, lvl = direct_hits[0]

        # If token hit is a country, do safe country-only (avoid arbitrary Admin1/Admin2 from KB row).
        if lvl == "Admin0":
            updated = info.copy()
            updated["admin0"] = val
            updated["admin1"] = "Unknown"
            updated["admin2"] = "Unknown"
            updated["location_level"] = "ADMIN0"
            note = f"Token match (Admin0) → KB country '{val}'"
            if debug:
                print(f"✅ TOKEN COUNTRY MATCH '{name}' → {val}")
            return updated, note, {"method": "token_admin0", "token_hits": direct_hits, "iso_hint": sorted(iso_hint)}

        cand = kb_index.rows_for_value(
            lvl, val,
            allowed_isos=iso_hint,
            prefer_admin0=info.get("admin0"),
            prefer_admin1=info.get("admin1"),
        )
        if len(cand) == 1:
            row = cand.iloc[0]
            updated = info.copy()
            updated["admin0"] = row["Admin0"] or updated["admin0"]
            updated["admin1"] = row["Admin1"] or updated["admin1"]
            updated["admin2"] = row["Admin2"] or updated["admin2"]
            updated["location_level"] = lvl.upper()
            note = f"Token match ({lvl}) → KB (100)"
            if debug:
                print(f"✅ SINGLE TOKEN+KB MATCH '{name}': {row['Admin0']} → {row['Admin1']} → {row['Admin2']}")
            return updated, note, {"method": "token", "token_hits": direct_hits, "iso_hint": sorted(iso_hint)}
        else:
            multiple_token_hits = True

    elif direct_hits and len(direct_hits) > 1:
        multiple_token_hits = True

    if multiple_token_hits and debug:
        print(f"\n✅ TOKEN MATCHES for '{name}' (ambiguous): {len(direct_hits)} hit(s)")

    # --- Skip fuzzy if multiple token hits, unless CLiFF explicitly mentions this name
    skip_fuzzy = multiple_token_hits
    if skip_fuzzy and cliff_context:
        cliff_can_resolve = False
        for iso, places in (cliff_context or {}).items():
            places = places or []
            if any(normalize_for_match(name) == normalize_for_match(p) for p in places):
                cliff_can_resolve = True
                break
        if not cliff_can_resolve:
            skip_fuzzy = False

    best_match: Optional[str] = None
    best_score: float = 0.0
    match_level: Optional[str] = None

    # --- FUZZY (universal match across Admin0/Admin1/Admin2; can upfill missing admin0)
    if not skip_fuzzy:
        best_match, best_score, match_level = kb_index.fuzzy_best(name, allowed_isos=iso_hint)

        # safeguard: reject 1-word match for multi-word query unless very strong
        if best_match:
            if len(normalize_for_match(name).split()) > 1 and len(normalize_for_match(best_match).split()) == 1:
                direct_ratio = fuzz.ratio(normalize_for_match(name), normalize_for_match(best_match))
                if direct_ratio < 90:
                    if debug:
                        print(f"🚫 Rejecting weak match: '{best_match}' for '{name}' (direct_ratio={direct_ratio})")
                    best_match, best_score, match_level = None, 0.0, None

        if debug:
            print(f"\n🔍 Best fuzzy for '{name}': {best_match} @ {match_level} ({best_score}) ISO={sorted(iso_hint) if iso_hint else None}")
    else:
        if debug:
            print("❗Skipping fuzzy due to multiple token hits — waiting for CLiFF ISO resolution.")

    # =====================================================================================
    # CLiFF ISO fallback
    # =====================================================================================
    if (not best_match or best_score < threshold) and cliff_context:
        cliff_hit_iso: Optional[str] = None
        for iso, places in (cliff_context or {}).items():
            places = places or []
            if any(normalize_for_match(name) == normalize_for_match(p) for p in places):
                cliff_hit_iso = str(iso).upper().strip()
                break

        if cliff_hit_iso:
            kb_iso = kb_index.kb[kb_index.kb["Admin0_ISO3"] == cliff_hit_iso]
            if len(kb_iso) > 0:
                admin0_name = kb_iso.iloc[0]["Admin0"]
                updated = info.copy()
                updated["admin0"] = admin0_name or updated["admin0"]

                # Admin2 exact match
                kb_city = kb_iso[kb_iso["Admin2"].apply(lambda x: normalize_for_match(x) == normalize_for_match(name))]
                if len(kb_city) > 0:
                    row_city = kb_city.iloc[0]
                    updated["admin1"] = row_city["Admin1"] or updated["admin1"]
                    updated["admin2"] = row_city["Admin2"] or updated["admin2"]
                    updated["location_level"] = "ADMIN2"
                    note = f"Filled from CLiFF ISO={cliff_hit_iso}: matched Admin2 '{name}' under {admin0_name}"
                    return updated, note, {"method": "cliff_iso_admin2", "iso": cliff_hit_iso}

                # Admin1 fuzzy within ISO if admin-looking
                if looks_like_admin_unit(name):
                    admin1_choices = sorted(set(kb_iso["Admin1"].dropna().astype(str).unique().tolist()))
                    m1 = process.extractOne(name, admin1_choices, scorer=fuzz.token_set_ratio, processor=normalize_for_match)
                    if m1 and m1[1] >= threshold:
                        updated["admin1"] = m1[0]
                        updated["admin2"] = "Unknown"
                        updated["location_level"] = "ADMIN1"
                        note = f"Filled from CLiFF ISO={cliff_hit_iso}: matched Admin1 '{m1[0]}' for '{name}' under {admin0_name} (score={m1[1]})"
                        return updated, note, {"method": "cliff_iso_admin1", "iso": cliff_hit_iso, "score": float(m1[1])}

                # Otherwise: country-only (do NOT fabricate admin1=name)
                updated["admin1"] = "Unknown"
                updated["admin2"] = "Unknown"
                updated["location_level"] = "ADMIN0"
                note = f"Filled from CLiFF ISO={cliff_hit_iso}: country-only for '{name}' under {admin0_name} (no KB Admin1/Admin2 match)"
                return updated, note, {"method": "cliff_iso_country_only", "iso": cliff_hit_iso}

    # --- No match above threshold
    if not best_match or best_score < threshold or not match_level:
        updated = info.copy()
        note = f"No reliable match (best={best_match}, score={best_score})"

        if updated.get("admin2") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN2"
        elif updated.get("admin1") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN1"
        elif updated.get("admin0") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN0"
        else:
            updated["location_level"] = "UNKNOWN"

        return updated, note, {"method": "no_match", "best": best_match, "score": best_score, "iso_hint": sorted(iso_hint)}

    # --- Map fuzzy match back to KB rows
    cand_rows = kb_index.rows_for_value(
        match_level, best_match,
        allowed_isos=iso_hint,
        prefer_admin0=info.get("admin0"),
        prefer_admin1=info.get("admin1"),
    )

    # Admin0 match should not pick random Admin1/Admin2
    if match_level == "Admin0":
        updated = info.copy()
        updated["admin0"] = best_match
        updated["admin1"] = "Unknown"
        updated["admin2"] = "Unknown"
        updated["location_level"] = "ADMIN0"
        note = f"Matched '{name}' → Admin0 '{best_match}' (score={best_score})"
        return updated, note, {"method": "fuzzy_admin0", "score": best_score, "iso_hint": sorted(iso_hint)}

    if len(cand_rows) == 0:
        updated = info.copy()
        note = f"Fuzzy matched '{best_match}' ({match_level}) but no KB rows after filtering"
        return updated, note, {"method": "edge_no_kb_rows", "best": best_match, "score": best_score, "match_level": match_level}

    row = cand_rows.iloc[0].to_dict()
    updated = info.copy()
    note = ""
    did_fill = False

    # --- Case 0: fully filled already
    if all(v not in [None, "", "Unknown"] for v in [updated["admin0"], updated["admin1"], updated["admin2"]]):
        if updated.get("admin2") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN2"
        elif updated.get("admin1") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN1"
        else:
            updated["location_level"] = "ADMIN0"
        return updated, note, {"method": "already_filled"}

    src_level = (location_info.get("location_level") or "Unknown").upper()

    # --- Case 1: Unknown → KB Admin1
    if src_level == "UNKNOWN" and match_level == "Admin1":
        if updated.get("admin1") in [None, "", "Unknown"] and row.get("Admin1"):
            updated["admin1"] = best_match
            did_fill = True
        if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
            updated["admin0"] = row["Admin0"]
            did_fill = True
        updated["admin2"] = updated.get("admin2", "Unknown") or "Unknown"
        note = f"Matched Unknown ({name}) → Admin1 ({best_match}) under {row.get('Admin0')} (score={best_score})" if did_fill else "No upfill needed"

    # --- Case: ADMIN1 → KB Admin2
    elif src_level == "ADMIN1" and match_level == "Admin2":
        updated["admin2"] = best_match
        if row.get("Admin1"):
            updated["admin1"] = row["Admin1"]
            did_fill = True
        if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
            updated["admin0"] = row["Admin0"]
            did_fill = True
        note = f"Matched Admin1 ({name}) → Admin2 ({best_match}) (KB Admin1: {row.get('Admin1')}) (score={best_score})"

    # --- Case: ADMIN1 → KB Admin1 (fill admin0)
    elif src_level == "ADMIN1" and match_level == "Admin1":
        if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
            updated["admin0"] = row["Admin0"]
            did_fill = True
        updated["admin2"] = updated.get("admin2", "Unknown") or "Unknown"
        note = f"Filled admin0 for {name} from KB ({row.get('Admin0')}) (score={best_score})"

    # --- Case: ADMIN2 → KB Admin2 (fill missing)
    elif src_level == "ADMIN2" and match_level == "Admin2":
        if updated.get("admin2") in [None, "", "Unknown"] and best_match:
            updated["admin2"] = best_match
            did_fill = True
        if updated.get("admin1") in [None, "", "Unknown"] and row.get("Admin1"):
            updated["admin1"] = row["Admin1"]
            did_fill = True
        if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
            updated["admin0"] = row["Admin0"]
            did_fill = True
        note = f"Filled admin2/admin1/admin0 for {name} from KB ({best_match}, {best_score})" if did_fill else "No upfill needed"

    else:
        # Universal upfill: if we matched Admin2/Admin1, fill missing hierarchy (incl. admin0) regardless of Gemini-level hints
        if match_level == "Admin2":
            if updated.get("admin2") in [None, "", "Unknown"]:
                updated["admin2"] = best_match
                did_fill = True
            if updated.get("admin1") in [None, "", "Unknown"] and row.get("Admin1"):
                updated["admin1"] = row["Admin1"]
                did_fill = True
            if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
                updated["admin0"] = row["Admin0"]
                did_fill = True
            note = f"Universal upfill via Admin2 match '{best_match}' (score={best_score})"
        elif match_level == "Admin1":
            if updated.get("admin1") in [None, "", "Unknown"]:
                updated["admin1"] = best_match
                did_fill = True
            if updated.get("admin0") in [None, "", "Unknown"] and row.get("Admin0"):
                updated["admin0"] = row["Admin0"]
                did_fill = True
            updated["admin2"] = "Unknown"
            note = f"Universal upfill via Admin1 match '{best_match}' (score={best_score})"
        else:
            note = f"No specific rule matched ({name} ↔ {best_match}, level={match_level}, score={best_score})"

    if did_fill:
        updated["location_level"] = match_level.upper()
    else:
        if updated.get("admin2") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN2"
        elif updated.get("admin1") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN1"
        elif updated.get("admin0") not in [None, "", "Unknown"]:
            updated["location_level"] = "ADMIN0"
        else:
            updated["location_level"] = "UNKNOWN"

    return updated, note, {
        "method": "fuzzy_rule_fill",
        "best": best_match,
        "score": best_score,
        "match_level": match_level,
        "iso_hint": sorted(iso_hint),
    }


# =========================
# Revisit logic (not overly sensitive)
# =========================
def compute_revisit_reason(
    loc_name: str,
    orig: Dict[str, str],
    updated: Dict[str, str],
    dbg: Dict[str, Any],
    kb_index: KBIndex,
    allowed_isos: set
) -> Optional[str]:
    """
    Revisit ONLY if:
      - country is grounded (admin0 in KB)
      - and either:
        (a) admin combo is invalid in KB, OR
        (b) Gemini/name suggests subnational, but we only got country-level.
    Never revisit POIs/entities unless they also look like admin units.
    """
    updated = sanitize_location_info(updated)

    a0 = updated.get("admin0", "Unknown")
    if a0 in {"Unknown", "", None} or not admin0_isos(a0, kb_index):
        return None  # unknown country -> unresolved, not revisit

    # If CLiFF is highly confident (single ISO), mismatch is worth a revisit.
    if allowed_isos and len(allowed_isos) == 1:
        iso = next(iter(allowed_isos))
        if admin0_isos(a0, kb_index).isdisjoint({iso}):
            return f"Admin0 '{a0}' conflicts with confident CLiFF ISO={iso}"

    # If we claim admin1/admin2 but KB says the combo doesn't exist -> revisit.
    if not kb_combo_ok(updated, kb_index):
        return "Admin combo not found in KB"

    # If Gemini expected subnational (or name looks admin-like) but we only got country.
    orig = sanitize_location_info(orig)
    expected_subnational = (
        orig.get("location_level") in {"ADMIN1", "ADMIN2"} or
        orig.get("admin1") not in {"Unknown", "", None} or
        orig.get("admin2") not in {"Unknown", "", None} or
        looks_like_admin_unit(loc_name)
    )

    if expected_subnational and updated.get("location_level") == "ADMIN0":
        # Avoid POI spam
        if looks_like_poi(loc_name) and not looks_like_admin_unit(loc_name):
            return None
        method = (dbg or {}).get("method")
        return f"Expected subnational but only country-level tagging (method={method})"

    return None


# =========================
# Doc-level: reconcile a whole gemini_locations dict
# =========================
def reconcile_gemini_dict(
    gemini_dict: Dict[str, Any],
    cliff_dict: Dict[str, Any],
    kb_index: KBIndex,
    threshold: int = DEFAULT_THRESHOLD,
    debug: bool = False
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns:
      reconciled (dict),
      debug_map (dict),
      revisit (list),
      unresolved (list),
      meta (dict)
    """
    gemini_dict = gemini_dict or {}
    cliff_dict = cliff_dict or {}

    if not isinstance(gemini_dict, dict):
        gemini_dict = {}
    if not isinstance(cliff_dict, dict):
        cliff_dict = {}

    allowed_isos = kb_index.get_iso_set(cliff_dict)

    items = [
        (k, v) for k, v in gemini_dict.items()
        if k and str(k).strip() and str(k).strip().lower() != "unknown"
    ]

    reconciled: Dict[str, Any] = {}
    debug_map: Dict[str, Any] = {}
    revisit: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []

    if len(items) == 0:
        meta = {
            "status": "NO_LOCATIONS",
            "n_input": 0,
            "n_reconciled": 0,
            "n_revisit": 0,
            "n_unresolved": 0,
            "cliff_isos": sorted(allowed_isos) if allowed_isos else [],
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        return reconciled, debug_map, revisit, unresolved, meta

    for loc_name, loc_info in items:
        orig = sanitize_location_info(loc_info)

        # Hallucination guard: if Gemini fields aren't KB-grounded, keep only potentially-valid admin0.
        candidate_info = orig.copy()
        if not is_grounded(orig, kb_index, allowed_isos=allowed_isos):
            keep_a0 = orig.get("admin0")
            if keep_a0 in {"Unknown", "", None} or not is_grounded(
                {"location_level": "ADMIN0", "admin0": keep_a0, "admin1": "Unknown", "admin2": "Unknown"},
                kb_index, allowed_isos=allowed_isos
            ):
                keep_a0 = "Unknown"
            candidate_info = {"location_level": "UNKNOWN", "admin0": keep_a0, "admin1": "Unknown", "admin2": "Unknown"}

        updated, note, dbg = reconcile_location(
            loc_name,
            candidate_info,
            kb_index,
            threshold=threshold,
            cliff_context=cliff_dict,
            debug=debug,
        )

        # If still no country, but CLiFF has a single ISO, keep country-only (best-effort)
        if updated.get("admin0") in {"Unknown", "", None} and allowed_isos and len(allowed_isos) == 1:
            iso = next(iter(allowed_isos))
            a0 = admin0_name_from_iso(iso, kb_index)
            if a0:
                updated = sanitize_location_info(updated)
                updated["admin0"] = a0
                updated["admin1"] = "Unknown"
                updated["admin2"] = "Unknown"
                updated["location_level"] = "ADMIN0"
                dbg = {**(dbg or {}), "method": "iso_only_country", "iso": iso}
                note = note + f" | ISO-only country={a0}"

        # If we still cannot ground to a KB country, treat as unresolved (not revisit)
        if updated.get("admin0") in {"Unknown", "", None} or not admin0_isos(updated.get("admin0", "Unknown"), kb_index):
            unresolved.append({
                "location": loc_name,
                "reason": "Could not ground to KB Admin0 (country)",
                "note": note,
                "original": orig,
                "candidate_input": candidate_info,
                "reconciled_attempt": updated,
            })
            continue

        reconciled[loc_name] = sanitize_location_info(updated)
        debug_map[loc_name] = {
            "note": note,
            **(dbg or {}),
            "original": orig,
            "candidate_input": candidate_info,
        }

        reason = compute_revisit_reason(loc_name, orig, updated, dbg or {}, kb_index, allowed_isos)
        if reason:
            revisit.append({
                "location": loc_name,
                "reason": reason,
                "note": note,
                "original": orig,
                "reconciled": sanitize_location_info(updated),
            })

    status = "OK"
    if len(revisit) > 0:
        status = "REVISIT"
    elif len(reconciled) == 0 and len(unresolved) > 0:
        status = "UNRESOLVED"

    meta = {
        "status": status,
        "n_input": len(items),
        "n_reconciled": len(reconciled),
        "n_revisit": len(revisit),
        "n_unresolved": len(unresolved),
        "cliff_isos": sorted(allowed_isos) if allowed_isos else [],
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return reconciled, debug_map, revisit, unresolved, meta


# =========================
# Runner: MongoDB scan/update per country/date range
# =========================
@dataclass
class ReconcileRunner:
    uri: str
    db_name: str
    countries: List[str]
    kb_path: str = "/home/diego/peace/KB_DF.csv"
    start_year: int = 2012
    end_year: int = 2025
    end_month: int = 12
    threshold: int = DEFAULT_THRESHOLD
    dry_run: bool = False
    debug_rows_per_collection: int = 3
    force: bool = False  # overwrite existing reconciled field

    def __post_init__(self):
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        kb_df = pd.read_csv(self.kb_path)
        self.kb_index = KBIndex(kb_df)

    def _source_domains_for_countries(self) -> List[str]:
        return [
            doc["source_domain"]
            for doc in self.db["sources"].find(
                {"primary_location": {"$in": self.countries}, "include": True},
                {"source_domain": 1}
            )
        ]

    def run(self):
        loc_domains = self._source_domains_for_countries()

        base_query: Dict[str, Any] = {
            "source_domain": {"$in": loc_domains},
            "environmental_binary.result": "Yes",
            "$or": [
                {"env_classifier.env_max": {"$nin": ["-999", "environmental -999", None]}},
                {"env_classifier.env_sec": {"$nin": ["-999", "environmental -999", None]}}
            ],
            "gemini_locations": {"$exists": True},
            "cliff_locations": {"$exists": True},
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

                print("\n" + "=" * 100)
                print(f"📦 {collection_name}: {total} doc(s) to reconcile | countries={self.countries}")
                print("=" * 100)

                cursor = collection.find(
                    base_query,
                    {"_id": 1, "gemini_locations": 1, "gemini_locations_normalized": 1, "cliff_locations": 1}
                )

                processed = 0
                revisit_docs = 0
                unresolved_docs = 0
                noloc_docs = 0

                for doc in cursor:
                    processed += 1
                    doc_id = doc["_id"]

                    gem = doc.get("gemini_locations_normalized") or doc.get("gemini_locations") or {}
                    cliff = doc.get("cliff_locations") or {}

                    gem = safe_load_dict(gem)
                    cliff = safe_load_dict(cliff)

                    verbose = processed <= self.debug_rows_per_collection

                    reconciled, debug_map, revisit, unresolved, meta = reconcile_gemini_dict(
                        gem, cliff, self.kb_index, threshold=self.threshold, debug=verbose
                    )

                    if meta["status"] == "REVISIT":
                        revisit_docs += 1
                    if meta["status"] == "UNRESOLVED":
                        unresolved_docs += 1
                    if meta["status"] == "NO_LOCATIONS":
                        noloc_docs += 1

                    print(f"[{collection_name}] ({processed}/{total}) _id={doc_id} "
                          f"input={meta['n_input']} reconciled={meta['n_reconciled']} "
                          f"unresolved={meta['n_unresolved']} revisit={meta['n_revisit']} "
                          f"cliff_isos={meta['cliff_isos']} status={meta['status']}")

                    if verbose and meta["n_reconciled"] > 0:
                        for loc_name, dbg in list(debug_map.items())[:10]:
                            print(f"   - {loc_name}: {dbg.get('note', '')}")

                    update_doc = {
                        RECONCILED_FIELD: reconciled,
                        RECONCILED_META_FIELD: meta,
                        RECONCILED_DEBUG_FIELD: debug_map,
                        RECONCILED_REVISIT_FIELD: revisit,
                        RECONCILED_UNRESOLVED_FIELD: unresolved,
                    }

                    if not self.dry_run:
                        collection.update_one({"_id": doc_id}, {"$set": update_doc})

                print(f"✅ Done {collection_name}: processed={processed} | "
                      f"revisit_docs={revisit_docs} | unresolved_docs={unresolved_docs} | noloc_docs={noloc_docs}")


if __name__ == "__main__":
    # Use env vars to avoid hardcoding credentials:
    #   export MONGO_URI="mongodb://..."
    #   export MONGO_DB="ml4p"
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true")
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
        force=True,
    )

    runner.run()
