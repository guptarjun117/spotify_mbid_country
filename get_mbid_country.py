import os
import re
import time
import pickle
import sqlite3
import threading
import unicodedata
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

import config
from spotify_client import get_spotify_client
from get_artists import get_unique_artists_from_playlist


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": config.USER_AGENT})


_SQL_CONN = None
_SQL_LOCK = threading.Lock()

_MEM_CACHE = {}
_WRITE_BUFFER = {}
_DIRTY = False


def _sql_connect():
    global _SQL_CONN
    if _SQL_CONN is not None:
        return
    db_path = config.resolve_path(config.DB_FILE)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    _SQL_CONN = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    cur = _SQL_CONN.cursor()

    try:
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_cache (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL,
            updated_at REAL NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_kv_cache_updated ON kv_cache(updated_at);")
    _SQL_CONN.commit()


def cache_get(key, default=None):
    if key in _WRITE_BUFFER:
        return _WRITE_BUFFER[key]
    if key in _MEM_CACHE:
        return _MEM_CACHE[key]

    _sql_connect()
    with _SQL_LOCK:
        cur = _SQL_CONN.cursor()
        cur.execute("SELECT value FROM kv_cache WHERE key = ? LIMIT 1;", (key,))
        row = cur.fetchone()

    if not row:
        return default

    try:
        val = pickle.loads(row[0])
    except Exception:
        return default

    _MEM_CACHE[key] = val
    return val


def cache_set(key, value):
    global _DIRTY
    _MEM_CACHE[key] = value
    _WRITE_BUFFER[key] = value
    _DIRTY = True


def flush_cache():
    global _DIRTY
    if not _DIRTY or not _WRITE_BUFFER:
        return

    _sql_connect()
    now = time.time()

    items = list(_WRITE_BUFFER.items())
    _WRITE_BUFFER.clear()

    payload = []
    for k, v in items:
        try:
            blob = pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
            payload.append((k, blob, now))
        except Exception:
            continue

    if not payload:
        _DIRTY = False
        return

    with _SQL_LOCK:
        cur = _SQL_CONN.cursor()
        cur.executemany(
            "INSERT INTO kv_cache(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;",
            payload,
        )
        _SQL_CONN.commit()

    _DIRTY = False


def migrate_pickle_cache_to_sqlite(pickle_file):
    if not os.path.exists(pickle_file):
        return
    try:
        with open(pickle_file, "rb") as f:
            data = pickle.load(f)
        if not isinstance(data, dict) or not data:
            return
    except Exception:
        return

    _sql_connect()
    with _SQL_LOCK:
        cur = _SQL_CONN.cursor()
        existing = set()
        cur.execute("SELECT key FROM kv_cache;")
        for (k,) in cur.fetchall():
            existing.add(k)

    to_write = []
    now = time.time()
    for k, v in data.items():
        if k in existing:
            continue
        try:
            blob = pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
            to_write.append((k, blob, now))
        except Exception:
            continue

    if not to_write:
        return

    with _SQL_LOCK:
        cur = _SQL_CONN.cursor()
        cur.executemany("INSERT INTO kv_cache(key, value, updated_at) VALUES(?,?,?);", to_write)
        _SQL_CONN.commit()

try:
    import asyncio
    from lingua import LanguageDetectorBuilder
    from googletrans import Translator

    try:
        import nest_asyncio

        nest_asyncio.apply()
        _NEST_ASYNCIO_OK = True
    except Exception:
        _NEST_ASYNCIO_OK = False

    detector = LanguageDetectorBuilder.from_all_languages().build()
    translator = Translator()

    def detect_language(text):
        if not text or not str(text).strip():
            return None, None, 0.0
        try:
            lang = detector.detect_language_of(text)
            if not lang:
                return None, None, 0.0
            code = None
            try:
                code = lang.iso_code_639_1.name.lower()
            except Exception:
                code = None
            name = str(lang).split(".")[-1]
            return code, name, 1.0
        except Exception:
            return None, None, 0.0

    async def _translate_async(text, src_code, dest_code="en"):
        return await translator.translate(text, src=src_code, dest=dest_code)

    def translate_text(text, source_code=None, target_code="en"):
        if not text or not str(text).strip():
            return None
        if source_code is None:
            source_code = "auto"
        if source_code == target_code:
            return text
        try:
            try:
                result = asyncio.run(_translate_async(text, source_code, target_code))
                return result.text
            except RuntimeError:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    if not _NEST_ASYNCIO_OK:
                        return None
                    result = loop.run_until_complete(_translate_async(text, source_code, target_code))
                    return result.text
                result = loop.run_until_complete(_translate_async(text, source_code, target_code))
                return result.text
        except Exception:
            return None

except ImportError:
    detector = None
    translator = None

try:
    import spacy

    _SPACY_OK = True
except Exception:
    spacy = None
    _SPACY_OK = False

try:
    import pycountry

    _PYCOUNTRY_OK = True
except Exception:
    pycountry = None
    _PYCOUNTRY_OK = False

try:
    import country_converter as coco

    _COCO_OK = True
except Exception:
    coco = None
    _COCO_OK = False

try:
    import geonamescache

    _GC_OK = True
except Exception:
    geonamescache = None
    _GC_OK = False

_SPACY_NLP = None
_GC = None
_GC_CITIES = None
_GC_COUNTRIES = None
_GC_CITYNAME_TO_ISO2 = None
_GC_COUNTRYNAME_TO_ISO2 = None


def _get_spacy_nlp():
    global _SPACY_NLP
    if not _SPACY_OK:
        return None
    if _SPACY_NLP is not None:
        return _SPACY_NLP
    try:
        _SPACY_NLP = spacy.load("en_core_web_sm")
        return _SPACY_NLP
    except Exception:
        return None


def _init_geonamescache():
    global _GC, _GC_CITIES, _GC_COUNTRIES, _GC_CITYNAME_TO_ISO2, _GC_COUNTRYNAME_TO_ISO2
    if not _GC_OK:
        return False
    if _GC is not None:
        return True
    try:
        _GC = geonamescache.GeonamesCache()
        _GC_CITIES = _GC.get_cities()
        _GC_COUNTRIES = _GC.get_countries()

        _GC_CITYNAME_TO_ISO2 = {}
        for _, c in _GC_CITIES.items():
            nm = (c.get("name") or "").strip().lower()
            cc = c.get("countrycode")
            if nm and cc:
                _GC_CITYNAME_TO_ISO2.setdefault(nm, cc)

        _GC_COUNTRYNAME_TO_ISO2 = {}
        for iso2, c in _GC_COUNTRIES.items():
            nm = (c.get("name") or "").strip().lower()
            if nm:
                _GC_COUNTRYNAME_TO_ISO2[nm] = iso2

        return True
    except Exception:
        return False


def _pycountry_name_to_iso2(name):
    if not _PYCOUNTRY_OK or not name:
        return None
    n = str(name).strip()
    if not n:
        return None
    try:
        c = pycountry.countries.lookup(n)
        if c and hasattr(c, "alpha_2"):
            return c.alpha_2
    except Exception:
        pass
    return None


def _co_convert_to_iso2(text):
    if not _COCO_OK or not text:
        return None
    try:
        iso2 = coco.convert(names=text, to="ISO2", not_found=None)
        if iso2 and isinstance(iso2, str) and re.fullmatch(r"[A-Z]{2}", iso2):
            return iso2
    except Exception:
        pass
    return None


def _normalize_text_simple(s):
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _norp_to_country_candidates(norp_text):
    t = _normalize_text_simple(norp_text)
    if not t:
        return []
    if "south korean" in t:
        return ["South Korea", "Korea, Republic of", "Korea"]
    if "north korean" in t:
        return ["North Korea", "Korea, Democratic People's Republic of", "Korea"]
    if t == "korean":
        return ["South Korea", "Korea, Republic of", "Korea"]
    if t == "american":
        return ["United States", "United States of America", "USA", "America"]
    if t == "british":
        return ["United Kingdom", "UK", "Great Britain", "Britain"]

    cands = [norp_text]
    suffixes = ["ese", "ish", "ian", "ean", "an"]
    for suf in suffixes:
        if t.endswith(suf) and len(t) > len(suf) + 2:
            base = t[: -len(suf)]
            cands.append(base.title())
            cands.append((base + "a").title())
            cands.append((base + "e").title())
            cands.append((base + "ia").title())
            cands.append((base + "land").title())

    out, seen = [], set()
    for x in cands:
        k = _normalize_text_simple(x)
        if k and k not in seen:
            seen.add(k)
            out.append(x)
    return out


def _city_or_place_to_country_iso2(place_text):
    if not place_text or not _init_geonamescache():
        return None
    key = _normalize_text_simple(place_text)
    if not key:
        return None

    iso2 = _GC_COUNTRYNAME_TO_ISO2.get(key)
    if iso2 and re.fullmatch(r"[A-Z]{2}", iso2):
        return iso2

    iso2 = _GC_CITYNAME_TO_ISO2.get(key)
    if iso2 and re.fullmatch(r"[A-Z]{2}", iso2):
        return iso2

    return None


def _extract_probable_demonyms_from_text(text):
    t = _normalize_text_simple(text)
    if not t:
        return []
    words = re.findall(r"\b[a-z]{3,25}\b", t)
    suffixes = ("ese", "ish", "ian", "ean", "an")
    hits = []
    for w in words:
        if w in ("korean", "american", "british"):
            hits.append(w)
        elif w.endswith(suffixes):
            hits.append(w)
    out, seen = [], set()
    for w in hits:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def _extract_place_phrases_from_text(text):
    t = str(text or "").strip()
    if not t:
        return []
    patterns = [
        r"\bfrom\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)",
        r"\bin\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)",
        r"\bbased\s+in\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)",
    ]
    out = []
    for pat in patterns:
        for m in re.finditer(pat, t):
            out.append(m.group(1).strip())
    seen, uniq = set(), []
    for x in out:
        k = _normalize_text_simple(x)
        if k and k not in seen:
            seen.add(k)
            uniq.append(x)
    return uniq


def infer_country_iso_from_text(text):
    if not text or not str(text).strip():
        return None
    t = str(text).strip()

    iso2 = _co_convert_to_iso2(t)
    if iso2:
        return iso2

    for place in _extract_place_phrases_from_text(t):
        iso2 = _city_or_place_to_country_iso2(place)
        if iso2:
            return iso2
        iso2 = _co_convert_to_iso2(place)
        if iso2:
            return iso2
        iso2 = _pycountry_name_to_iso2(place)
        if iso2:
            return iso2

    nlp = _get_spacy_nlp()
    if nlp:
        doc = nlp(t)
        gpe_loc, norp = [], []
        for ent in doc.ents:
            if ent.label_ in ("GPE", "LOC"):
                gpe_loc.append(ent.text)
            elif ent.label_ == "NORP":
                norp.append(ent.text)

        def _uniq(seq):
            seen, out2 = set(), []
            for x in seq:
                k = _normalize_text_simple(x)
                if k and k not in seen:
                    seen.add(k)
                    out2.append(x)
            return out2

        gpe_loc = _uniq(gpe_loc)
        norp = _uniq(norp)

        for place in gpe_loc:
            iso2 = _city_or_place_to_country_iso2(place)
            if iso2:
                return iso2
            iso2 = _co_convert_to_iso2(place)
            if iso2:
                return iso2
            iso2 = _pycountry_name_to_iso2(place)
            if iso2:
                return iso2

        for n in norp:
            iso2 = _co_convert_to_iso2(n)
            if iso2:
                return iso2
            iso2 = _pycountry_name_to_iso2(n)
            if iso2:
                return iso2
            for cand in _norp_to_country_candidates(n):
                iso2 = _co_convert_to_iso2(cand)
                if iso2:
                    return iso2
                iso2 = _pycountry_name_to_iso2(cand)
                if iso2:
                    return iso2

    for d in _extract_probable_demonyms_from_text(t):
        for cand in _norp_to_country_candidates(d):
            iso2 = _co_convert_to_iso2(cand)
            if iso2:
                return iso2
            iso2 = _pycountry_name_to_iso2(cand)
            if iso2:
                return iso2

    return None


_MB_LOCK = threading.Lock()
_MB_LAST_CALL_TS = 0.0

_LB_LOCK = threading.Lock()
_LB_LAST_CALL_TS = 0.0


def _rate_limit(kind: str):
    global _MB_LAST_CALL_TS, _LB_LAST_CALL_TS
    if kind == "mb":
        with _MB_LOCK:
            now = time.time()
            elapsed = now - _MB_LAST_CALL_TS
            if elapsed < config.MB_MIN_INTERVAL_SECONDS:
                time.sleep(config.MB_MIN_INTERVAL_SECONDS - elapsed)
            _MB_LAST_CALL_TS = time.time()
    elif kind == "lb":
        with _LB_LOCK:
            now = time.time()
            elapsed = now - _LB_LAST_CALL_TS
            if elapsed < config.LB_MIN_INTERVAL_SECONDS:
                time.sleep(config.LB_MIN_INTERVAL_SECONDS - elapsed)
            _LB_LAST_CALL_TS = time.time()


def make_request_with_retry(url, headers=None, params=None, timeout=10, max_retries=3):
    is_mb = "musicbrainz.org/ws/2/" in str(url)
    for attempt in range(max_retries):
        try:
            if is_mb:
                _rate_limit("mb")
            resp = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    return None

def clean_text(text):
    if text is None:
        return "None"
    if isinstance(text, float):
        if text != text:
            return "None"
        return str(text)

    text = str(text)
    if not text or text.isspace():
        return "None"
    if text.strip().lower() in ["none", "null", "nan", "na", ""]:
        return "None"

    try:
        text = unicodedata.normalize("NFKC", text)
    except Exception:
        pass

    text = " ".join(text.split())
    return text.strip()


def extract_spotify_artist_id(spotify_link):
    match = re.search(r"spotify\.com/artist/([a-zA-Z0-9]{22})", str(spotify_link))
    return match.group(1) if match else None


def normalize_name(s):
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_jaccard(a, b):
    a_toks = set(normalize_name(a).split())
    b_toks = set(normalize_name(b).split())
    if not a_toks or not b_toks:
        return 0.0
    return len(a_toks & b_toks) / max(1, len(a_toks | b_toks))

_NUM_WORDS = {
    "zero": "0",
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "ten": "10",
    "eleven": "11",
    "twelve": "12",
}


def normalize_name_strict(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = s.replace("＋", "+")
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9+\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokens_strict(s: str):
    s = normalize_name_strict(s)
    toks = s.split()
    mapped = []
    for t in toks:
        mapped.append(_NUM_WORDS.get(t, t))
    return mapped


def _has_plus_suffix(tokens):
    return any(re.fullmatch(r"\d+\+", t) for t in tokens)


def _digits_present(tokens):
    return any(re.search(r"\d", t) for t in tokens)

def get_spotify_artist_metadata(spotify_link):
    artist_id = extract_spotify_artist_id(spotify_link)
    sp = get_spotify_client()
    if not artist_id or not sp:
        return None

    cache_key = f"spotify_artist_meta_{artist_id}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return hit

    try:
        data = sp.artist(artist_id)
        meta = {
            "artist_id": artist_id,
            "name": clean_text(data.get("name", "")),
            "genres": data.get("genres", []) or [],
            "followers": (data.get("followers", {}) or {}).get("total", None),
            "popularity": data.get("popularity", None),
        }
        cache_set(cache_key, meta)
        return meta
    except Exception:
        return None


def get_artist_top_tracks_detailed(spotify_link):
    artist_id = extract_spotify_artist_id(spotify_link)
    sp = get_spotify_client()
    if not artist_id or not sp:
        return []

    cache_key = f"spotify_tracks_detailed_{artist_id}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return hit

    out = []
    try:
        results = sp.artist_top_tracks(artist_id, country="US")
        for tr in (results.get("tracks", []) or []):
            name = clean_text(tr.get("name", ""))
            isrc = None
            ext = tr.get("external_ids") or {}
            if isinstance(ext, dict):
                isrc = ext.get("isrc")
            if name and name != "None":
                out.append({"name": name, "isrc": isrc})
    except Exception:
        out = []

    cache_set(cache_key, out)
    return out

def build_mb_query(name):
    q = clean_text(name)
    toks = [t for t in normalize_name(q).split() if t]
    if len(toks) >= 2:
        sort_guess = f"{toks[-1].title()}, {toks[0].title()}"
        query = (
            f'(artist:"{q}" OR alias:"{q}" OR sortname:"{sort_guess}") '
            f"OR ((artist:{toks[0]} AND artist:{toks[-1]}) OR (alias:{toks[0]} AND alias:{toks[-1]}))"
        )
    else:
        query = f'(artist:"{q}" OR alias:"{q}")'
    return query


def musicbrainz_search_artist_by_name_paged(artist_name, limit=25, max_pages=4):
    all_artists = []
    q = clean_text(artist_name)
    query = build_mb_query(q)
    url = "https://musicbrainz.org/ws/2/artist/"
    headers = {"User-Agent": config.USER_AGENT}

    for page in range(max_pages):
        offset = page * limit
        cache_key = f"mb_search_artist_paged_{normalize_name(q)}_{normalize_name(query)}_{limit}_{offset}"
        hit = cache_get(cache_key, "__MISSING__")
        if hit != "__MISSING__":
            artists = hit
        else:
            params = {"query": query, "fmt": "json", "limit": str(limit), "offset": str(offset), "inc": "aliases"}
            resp = make_request_with_retry(url, headers=headers, params=params, timeout=15)
            if not resp or resp.status_code != 200:
                artists = []
            else:
                data = resp.json()
                artists = data.get("artists", []) or []
            cache_set(cache_key, artists)

        if not artists:
            break

        all_artists.extend(artists)

        try:
            if any(int(a.get("score", 0)) >= 95 for a in artists):
                break
        except Exception:
            pass

    return all_artists


def get_mb_name_variants(candidate):
    variants = []
    meta = {"primary_en_aliases": []}

    def add(v):
        v = clean_text(v)
        if v and v != "None":
            variants.append(v)

    add(candidate.get("name"))
    add(candidate.get("sort-name"))

    aliases = candidate.get("aliases", []) or []
    for a in aliases:
        add(a.get("name"))
        add(a.get("sort-name"))
        locale = (a.get("locale") or "").lower()
        primary = a.get("primary", False)
        if locale == "en" and primary:
            nm = a.get("name") or a.get("sort-name")
            if nm:
                meta["primary_en_aliases"].append(nm)

    seen = set()
    uniq = []
    for v in variants:
        nv = normalize_name(v)
        if nv and nv not in seen:
            seen.add(nv)
            uniq.append(v)

    return uniq, meta


def _name_sanity_gate(spotify_name: str, candidate: dict):
    sp_toks = _tokens_strict(spotify_name)
    if not sp_toks:
        return False, "empty_spotify_tokens"

    variants, _meta = get_mb_name_variants(candidate)

    sp_strict = " ".join(sp_toks)
    for v in variants:
        v_toks = _tokens_strict(v)
        if " ".join(v_toks) == sp_strict:
            return True, "exact_strict"

    sp_has_digits = _digits_present(sp_toks)
    sp_has_plus = _has_plus_suffix(sp_toks)

    sp_set = set(sp_toks)
    best_overlap = 0.0

    for v in variants:
        v_toks = _tokens_strict(v)
        v_set = set(v_toks)
        if not v_set:
            continue

        if sp_has_digits and not _digits_present(v_toks):
            continue
        if sp_has_plus and not _has_plus_suffix(v_toks):
            continue

        overlap = len(sp_set & v_set) / max(1, len(sp_set | v_set))
        if overlap > best_overlap:
            best_overlap = overlap

    if len(sp_toks) <= 2:
        if best_overlap >= 0.80:
            return True, f"short_name_overlap_{best_overlap:.2f}"
        return False, f"short_name_overlap_low_{best_overlap:.2f}"

    if best_overlap >= 0.60:
        return True, f"overlap_{best_overlap:.2f}"

    return False, f"overlap_low_{best_overlap:.2f}"


def score_mb_candidate(spotify_meta, candidate):
    sp_name = (spotify_meta or {}).get("name", "") or ""
    sp_norm = normalize_name(sp_name)
    if not sp_norm:
        return -999

    variants, meta = get_mb_name_variants(candidate)

    best_variant_score = 0
    sp_toks_strict = set(_tokens_strict(sp_name))

    for v in variants:
        v_norm = normalize_name(v)
        if not v_norm:
            continue

        if v_norm == sp_norm:
            s = 80
        else:
            jac = token_jaccard(sp_name, v)
            s = int(45 * jac)

            v_toks_strict = set(_tokens_strict(v))
            if sp_toks_strict and sp_toks_strict.issubset(v_toks_strict):
                s = max(s, 35)

        if s > best_variant_score:
            best_variant_score = s

    score = best_variant_score

    for en_alias in meta.get("primary_en_aliases", []):
        if normalize_name(en_alias) == sp_norm:
            score += 20
            break
        if token_jaccard(en_alias, sp_name) >= 0.6:
            score += 12
            break

    mb_type = (candidate.get("type") or "").lower()
    if mb_type == "group":
        score += 5

    disamb = clean_text(candidate.get("disambiguation", ""))
    iso_from_disamb = infer_country_iso_from_text(disamb)
    if iso_from_disamb:
        score += 5

    try:
        mb_search_score = int(candidate.get("score", 0))
        score += int(0.1 * mb_search_score)
    except Exception:
        pass

    return score

def _normalize_track_title(s):
    s = clean_text(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\(.*?\)|\[.*?\]|\{.*?\}", " ", s)
    s = re.sub(r"\b(remaster(ed)?|live|edit|version|mono|stereo|deluxe|feat\.?|ft\.?)\b", " ", s)
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _mb_artist_has_spotify_url(mbid, spotify_link):
    artist_id = extract_spotify_artist_id(spotify_link)
    if not artist_id:
        return False

    cache_key = f"mb_artist_urlrels_{mbid}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        rels = hit
    else:
        url = f"https://musicbrainz.org/ws/2/artist/{mbid}"
        headers = {"User-Agent": config.USER_AGENT}
        params = {"fmt": "json", "inc": "url-rels"}
        resp = make_request_with_retry(url, headers=headers, params=params, timeout=15)
        if not resp or resp.status_code != 200:
            return False
        data = resp.json()
        rels = data.get("relations", []) or []
        cache_set(cache_key, rels)

    needle1 = f"open.spotify.com/artist/{artist_id}"
    needle2 = f"spotify:artist:{artist_id}"

    for r in rels:
        u = ((r.get("url") or {}).get("resource")) or ""
        if needle1 in u or needle2 in u:
            return True
    return False


def _mb_recording_match_count(mbid, spotify_top_tracks, max_tracks=2):
    if not spotify_top_tracks:
        return 0

    headers = {"User-Agent": config.USER_AGENT}
    base_url = "https://musicbrainz.org/ws/2/recording/"

    tracks = [t for t in spotify_top_tracks if t and t != "None"][:max_tracks]
    norm_tracks = [_normalize_track_title(t) for t in tracks]
    norm_tracks = [t for t in norm_tracks if t]

    hits = 0
    for t_raw, t_norm in zip(tracks, norm_tracks):
        cache_key = f"mb_rec_title_match_{mbid}_{t_norm}"
        hit = cache_get(cache_key, "__MISSING__")
        if hit != "__MISSING__":
            ok = hit
        else:
            q = f'arid:{mbid} AND recording:"{t_raw}"'
            params = {"query": q, "fmt": "json", "limit": "1"}
            resp = make_request_with_retry(base_url, headers=headers, params=params, timeout=15)
            if not resp or resp.status_code != 200:
                ok = False
            else:
                data = resp.json()
                ok = (data.get("recording-count") or data.get("count") or 0) > 0
            cache_set(cache_key, ok)

        if ok:
            hits += 1

    return hits


def _mb_recording_isrc_match_count(mbid, spotify_top_tracks_detailed, max_tracks=2):
    if not spotify_top_tracks_detailed:
        return 0

    isrcs = []
    for t in spotify_top_tracks_detailed:
        isrc = (t or {}).get("isrc")
        if isrc and isinstance(isrc, str):
            isrcs.append(isrc.strip().upper())
    isrcs = [x for x in isrcs if re.fullmatch(r"[A-Z0-9]{12}", x)]
    if not isrcs:
        return 0

    isrcs = isrcs[:max_tracks]
    headers = {"User-Agent": config.USER_AGENT}
    base_url = "https://musicbrainz.org/ws/2/recording/"

    hits = 0
    for isrc in isrcs:
        cache_key = f"mb_rec_isrc_match_{mbid}_{isrc}"
        hit = cache_get(cache_key, "__MISSING__")
        if hit != "__MISSING__":
            ok = hit
        else:
            q = f"arid:{mbid} AND isrc:{isrc}"
            params = {"query": q, "fmt": "json", "limit": "1"}
            resp = make_request_with_retry(base_url, headers=headers, params=params, timeout=15)
            if not resp or resp.status_code != 200:
                ok = False
            else:
                data = resp.json()
                ok = (data.get("recording-count") or data.get("count") or 0) > 0
            cache_set(cache_key, ok)

        if ok:
            hits += 1

    return hits


def _is_ambiguous_spotify_name(spotify_meta, artist_name_fallback):
    sp_name = (spotify_meta or {}).get("name") or artist_name_fallback or ""
    n = normalize_name(sp_name)
    toks = n.split()
    return len(toks) <= 1


def validate_step1b_candidate(
    mbid,
    spotify_link,
    spotify_top_tracks,
    spotify_top_tracks_detailed,
    spotify_meta,
    artist_name_fallback,
    candidate_score,
    candidate,
):
    sp_name = (spotify_meta or {}).get("name") or artist_name_fallback or ""
    if not sp_name.strip():
        return False, "no_spotify_name"

    ok_name, reason = _name_sanity_gate(sp_name, candidate)
    if not ok_name:
        return False, f"name_gate_failed_{reason}"

    if candidate_score >= config.STEP1B_URLRELS_SCORE_GATE:
        if _mb_artist_has_spotify_url(mbid, spotify_link):
            return True, "spotify_url_relation"

    isrc_hits = _mb_recording_isrc_match_count(mbid, spotify_top_tracks_detailed, max_tracks=2)
    if isrc_hits >= 1:
        return True, f"isrc_match_{isrc_hits}"

    ambiguous = _is_ambiguous_spotify_name(spotify_meta, artist_name_fallback)
    title_hits = _mb_recording_match_count(mbid, spotify_top_tracks, max_tracks=2)
    required = 2 if ambiguous else 1

    if title_hits >= required:
        return True, f"title_match_{title_hits}_req_{required}"

    return False, f"no_corroboration_title_{title_hits}_isrc_{isrc_hits}_req_{required}"

def detect_primary_track_language(track_names):
    if not track_names or not detector:
        return None

    counts = {}
    names = {}

    for track in track_names[:]:
        code, name, conf = detect_language(track)
        if code and conf > 0.5:
            counts[code] = counts.get(code, 0) + 1
            names[code] = name

    if not counts:
        return None

    primary_code = max(counts.items(), key=lambda x: x[1])[0]
    primary_name = names.get(primary_code, "UNKNOWN")
    return primary_code, primary_name, counts, names


def get_translated_artist_name(artist_name, track_names):
    if not detector or not translator:
        return None

    artist_name = str(artist_name or "").strip()
    if not artist_name:
        return None

    lang_info = detect_primary_track_language(track_names)
    if not lang_info:
        return None

    track_lang_code, _, _, _ = lang_info
    if not track_lang_code or track_lang_code == "en":
        return None

    artist_code, _, _ = detect_language(artist_name)
    artist_code = artist_code or "auto"

    translated = translate_text(artist_name, source_code=artist_code, target_code=track_lang_code)
    if translated and translated.strip() and translated != artist_name:
        return translated

    translated2 = translate_text(artist_name, source_code="en", target_code=track_lang_code)
    if translated2 and translated2.strip() and translated2 != artist_name:
        return translated2

    return None

def choose_best_mbid_via_search(
    artist_name,
    spotify_link,
    spotify_top_tracks=None,
    spotify_top_tracks_detailed=None,
    track_names_for_translation=None,
):
    spotify_meta = get_spotify_artist_metadata(spotify_link)
    primary_name = (spotify_meta or {}).get("name") or artist_name

    search_name_attempts = [primary_name]

    if detector and translator and track_names_for_translation:
        translated = get_translated_artist_name(primary_name, track_names_for_translation)
        if translated and translated != primary_name:
            search_name_attempts.append(translated)

    for name_for_search in search_name_attempts:
        candidates = musicbrainz_search_artist_by_name_paged(name_for_search, limit=25, max_pages=4)
        if not candidates:
            continue

        scored = []
        for c in candidates:
            s = score_mb_candidate(spotify_meta or {"name": name_for_search}, c)
            scored.append((s, c))
        scored.sort(key=lambda x: x[0], reverse=True)

        validate_count = config.STEP1B_TOP_N
        if len(scored) >= 2 and abs(scored[0][0] - scored[1][0]) <= config.STEP1B_CLOSE_SCORE_DELTA:
            validate_count = max(validate_count, 2)

        for rank, (score, cand) in enumerate(scored[:validate_count], start=1):
            mbid = cand.get("id")
            if not mbid:
                continue
            if score < config.STEP1B_MIN_SCORE:
                continue

            ok, why = validate_step1b_candidate(
                mbid,
                spotify_link,
                (spotify_top_tracks or []),
                (spotify_top_tracks_detailed or []),
                spotify_meta,
                name_for_search,
                score,
                cand,
            )
            if ok:
                debug = {
                    "search_name": name_for_search,
                    "spotify_name": (spotify_meta or {}).get("name"),
                    "best_score": score,
                    "validation": True,
                    "validation_reason": why,
                    "rank_used": rank,
                }
                return mbid, debug

    return None, {
        "search_name": primary_name,
        "spotify_name": (spotify_meta or {}).get("name") if spotify_meta else None,
        "best_score": None,
        "validation": False,
        "validation_reason": "no_candidate_validated_any_name_attempt",
    }

def get_country_from_mbid(mbid):
    cache_key = f"country_v8_json_{mbid}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return hit

    got_valid_200 = False
    url = f"https://musicbrainz.org/ws/2/artist/{mbid}"
    headers = {"User-Agent": config.USER_AGENT}
    params = {"fmt": "json", "inc": "aliases+area-rels+url-rels"}

    for attempt in range(3):
        try:
            resp = make_request_with_retry(url, headers=headers, params=params, timeout=15)
            if not resp or resp.status_code != 200:
                time.sleep(0.2)
                continue

            got_valid_200 = True
            data = resp.json() if resp.content else {}

            country = data.get("country")
            if country and re.fullmatch(r"[A-Z]{2}", str(country).strip()):
                out = str(country).strip()
                cache_set(cache_key, out)
                return out

            disamb = clean_text(data.get("disambiguation", ""))
            iso = infer_country_iso_from_text(disamb)
            if iso:
                cache_set(cache_key, iso)
                return iso

            def area_name(obj):
                if not isinstance(obj, dict):
                    return ""
                return clean_text(obj.get("name", ""))

            for k in ("begin-area", "area"):
                nm = area_name(data.get(k))
                if nm and nm != "None":
                    iso2 = infer_country_iso_from_text(nm)
                    if iso2:
                        cache_set(cache_key, iso2)
                        return iso2
                    iso2 = _co_convert_to_iso2(nm) or _pycountry_name_to_iso2(nm)
                    if iso2:
                        cache_set(cache_key, iso2)
                        return iso2

            rels = data.get("relations", []) or []
            for r in rels:
                if not isinstance(r, dict):
                    continue
                a = r.get("area")
                if isinstance(a, dict):
                    nm = clean_text(a.get("name", ""))
                    if nm and nm != "None":
                        iso2 = infer_country_iso_from_text(nm)
                        if iso2:
                            cache_set(cache_key, iso2)
                            return iso2

            break

        except Exception:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue

    if got_valid_200:
        cache_set(cache_key, None)
    return None

def get_mbid_from_spotify_link(spotify_link):
    cache_key = f"mbid_spotify_{spotify_link}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return hit

    try:
        encoded_url = quote(spotify_link)
        url = "https://musicbrainz.org/ws/2/url/"
        params = {"query": f"url:{encoded_url}", "fmt": "json"}
        headers = {"User-Agent": config.USER_AGENT}

        response = make_request_with_retry(url, headers=headers, params=params, timeout=10)
        if not response or response.status_code != 200:
            cache_set(cache_key, [])
            return []

        data = response.json()
        mbids = []

        if data.get("count", 0) > 0 and "urls" in data:
            for url_data in data["urls"]:
                if "relation-list" in url_data:
                    for relation_list in url_data["relation-list"]:
                        if "relations" in relation_list:
                            for relation in relation_list["relations"]:
                                if "artist" in relation and "id" in relation["artist"]:
                                    mbids.append(
                                        {
                                            "mbid": relation["artist"]["id"],
                                            "name": relation["artist"].get("name", ""),
                                        }
                                    )

        cache_set(cache_key, mbids)
        return mbids

    except Exception:
        cache_set(cache_key, [])
        return []

def get_mbid_from_listenbrainz_simple(artist_name, track_name):
    if not track_name:
        cache_key = f"listenbrainz_simple_{artist_name}"
    else:
        clean_track = re.sub(r"[^\w\s]", "", track_name)
        cache_key = f"listenbrainz_simple_{artist_name}_{clean_track}"

    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return None if hit == "NOT_FOUND" else hit

    for attempt in range(3):
        try:
            _rate_limit("lb")
            params = {"artist_name": artist_name, "recording_name": track_name, "metadata": "true"}
            headers = {"Authorization": f"Token {config.LISTENBRAINZ_TOKEN}", "User-Agent": config.USER_AGENT}

            response = SESSION.get(
                "https://api.listenbrainz.org/1/metadata/lookup/",
                headers=headers,
                params=params,
                timeout=10,
            )

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict):
                    if "artist_mbids" in data and data["artist_mbids"]:
                        mbid = data["artist_mbids"][0]
                        cache_set(cache_key, mbid)
                        return mbid

                    if "recording" in data and isinstance(data["recording"], dict):
                        rec_data = data["recording"]
                        if "artist-credit" in rec_data and rec_data["artist-credit"]:
                            for artist in rec_data["artist-credit"]:
                                if isinstance(artist, dict) and "artist" in artist:
                                    if "id" in artist["artist"]:
                                        mbid = artist["artist"]["id"]
                                        cache_set(cache_key, mbid)
                                        return mbid

        except Exception:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue

    cache_set(cache_key, "NOT_FOUND")
    return None

def _mb_search_exact_name_candidates(artist_name, limit=10):
    q = clean_text(artist_name)
    if not q or q == "None":
        return []

    cache_key = f"mb_exact_name_{normalize_name(q)}_{limit}"
    hit = cache_get(cache_key, "__MISSING__")
    if hit != "__MISSING__":
        return hit

    url = "https://musicbrainz.org/ws/2/artist/"
    headers = {"User-Agent": config.USER_AGENT}
    params = {"query": f'artist:"{q}"', "fmt": "json", "limit": str(limit), "inc": "aliases"}

    resp = make_request_with_retry(url, headers=headers, params=params, timeout=15)
    if not resp or resp.status_code != 200:
        cache_set(cache_key, [])
        return []

    data = resp.json()
    artists = data.get("artists", []) or []
    cache_set(cache_key, artists)
    return artists


def _candidate_matches_exact_name(candidate, target_name):
    t = normalize_name_strict(target_name)
    if not t:
        return False

    if normalize_name_strict(candidate.get("name", "")) == t:
        return True

    for a in (candidate.get("aliases", []) or []):
        if normalize_name_strict(a.get("name", "")) == t:
            return True
        if normalize_name_strict(a.get("sort-name", "")) == t:
            return True

    return False


def unique_exact_name_country_fallback(artist_name):
    candidates = _mb_search_exact_name_candidates(artist_name, limit=15)
    if not candidates:
        return None, None

    matches = []
    for c in candidates:
        if _candidate_matches_exact_name(c, artist_name):
            mbid = c.get("id")
            if mbid:
                matches.append(mbid)

    matches = list(dict.fromkeys(matches))
    if len(matches) != 1:
        return None, None

    mbid = matches[0]
    country = get_country_from_mbid(mbid)
    if not country:
        return mbid, None
    return mbid, country

def process_artist(artist_name, spotify_link):
    original_artist = clean_text(artist_name)
    spotify_link = clean_text(spotify_link)

    top_tracks_detailed = get_artist_top_tracks_detailed(spotify_link)
    top_tracks = [t.get("name") for t in top_tracks_detailed if t.get("name")]
    top_tracks = [t for t in top_tracks if t and t != "None"]

    mbids = get_mbid_from_spotify_link(spotify_link)
    if len(mbids) == 1:
        mbid = mbids[0]["mbid"]
        country = get_country_from_mbid(mbid)
        if country:
            return {
                "artist_name": original_artist,
                "spotify_link": spotify_link,
                "mbid": mbid,
                "country": country,
                "method": "step16a_mb_url_single",
            }

    method = "step16_fallback_multiple" if len(mbids) > 1 else "step16_fallback_none"
    step2_result = None

    if top_tracks:
        mbid = None
        successful_track = None
        tracks_tried = 0
        successful_phase = 1

        for track_name in top_tracks[:]:
            tracks_tried += 1
            mbid = get_mbid_from_listenbrainz_simple(original_artist, track_name)
            if mbid:
                successful_track = track_name
                break

        if not mbid and detector and translator:
            translated_artist = get_translated_artist_name(original_artist, top_tracks)
            if translated_artist and translated_artist != original_artist:
                for track_name in top_tracks[:]:
                    mbid = get_mbid_from_listenbrainz_simple(translated_artist, track_name)
                    if mbid:
                        successful_track = track_name
                        successful_phase = 2
                        break

        if mbid:
            country = get_country_from_mbid(mbid)
            step2_result = {
                "artist_name": original_artist,
                "spotify_link": spotify_link,
                "mbid": mbid,
                "country": country,
                "method": method,
                "track_used": successful_track,
                "tracks_tried": tracks_tried,
                "total_tracks_available": len(top_tracks),
                "phase_used": successful_phase,
            }
            if country:
                return step2_result

    mbid_1b, _dbg = choose_best_mbid_via_search(
        original_artist,
        spotify_link,
        spotify_top_tracks=top_tracks or [],
        spotify_top_tracks_detailed=top_tracks_detailed or [],
        track_names_for_translation=top_tracks or [],
    )

    if mbid_1b:
        country_1b = get_country_from_mbid(mbid_1b)
        if country_1b:
            return {
                "artist_name": original_artist,
                "spotify_link": spotify_link,
                "mbid": mbid_1b,
                "country": country_1b,
                "method": "step16c_mb_search_name_alias_paged",
            }

    # Step 16D. Unique exact-name fallback
    mbid_u, country_u = unique_exact_name_country_fallback(original_artist)
    if country_u:
        return {
            "artist_name": original_artist,
            "spotify_link": spotify_link,
            "mbid": mbid_u,
            "country": country_u,
            "method": "step16d_unique_exact_name_country_only",
        }

    # Step 16E. Return best partials if available
    if step2_result is not None:
        return step2_result

    if mbid_1b:
        return {
            "artist_name": original_artist,
            "spotify_link": spotify_link,
            "mbid": mbid_1b,
            "country": None,
            "method": "step16c_mb_search_name_alias_paged_no_country",
        }

    if len(mbids) == 1:
        mbid = mbids[0]["mbid"]
        return {
            "artist_name": original_artist,
            "spotify_link": spotify_link,
            "mbid": mbid,
            "country": None,
            "method": "step16a_mb_url_single_no_country",
        }

    return {
        "artist_name": original_artist,
        "spotify_link": spotify_link,
        "mbid": None,
        "country": None,
        "method": method,
    }


def check_dependencies():
    required = ["spotipy", "pandas", "requests"]
    missing_required = []
    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing_required.append(package)
    if missing_required:
        print("❌ Missing required dependencies:")
        for p in missing_required:
            print(f"   - {p}")
        print("\nInstall with: pip install spotipy pandas requests")
        return False
    return True


def build_countries_csv(playlist_url, output_csv_path):
    sp = get_spotify_client()
    if not sp:
        print("❌ Cannot initialize Spotify client")
        return None

    _sql_connect()

    if config.MIGRATE_PICKLE_TO_SQLITE:
        legacy_path = config.resolve_path(config.LEGACY_PICKLE_CACHE_FILE)
        if os.path.exists(legacy_path):
            migrate_pickle_cache_to_sqlite(legacy_path)

    artists = get_unique_artists_from_playlist(playlist_url, max_tracks=None)
    df = pd.DataFrame(artists)

    if df is None or len(df) == 0:
        print("❌ No artists found in playlist.")
        return None

    if "artist_name" not in df.columns or "spotify_link" not in df.columns:
        print("❌ Internal error: playlist extraction must yield 'artist_name' and 'spotify_link'")
        return None

    df["artist_name"] = df["artist_name"].apply(clean_text)
    df["spotify_link"] = df["spotify_link"].astype(str).apply(clean_text)

    existing = None
    if os.path.exists(output_csv_path):
        try:
            existing = pd.read_csv(output_csv_path, keep_default_na=False, na_filter=False)
        except Exception:
            existing = None

    done_keys = set()
    if existing is not None and len(existing) > 0:
        for _, r in existing.iterrows():
            k = (clean_text(r.get("artist_name")), clean_text(r.get("spotify_link")))
            done_keys.add(k)

    results = []
    results_lock = threading.Lock()

    def _worker(idx, artist_name, spotify_link):
        k = (clean_text(artist_name), clean_text(spotify_link))
        if k in done_keys:
            if existing is not None:
                rows = existing[(existing["artist_name"] == k[0]) & (existing["spotify_link"] == k[1])]
                if len(rows) > 0:
                    return idx, rows.iloc[0].to_dict()
            return idx, None
        return idx, process_artist(artist_name, spotify_link)

    tasks = []
    start = time.time()
    total = len(df)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as ex:
        for i, row in df.iterrows():
            tasks.append(ex.submit(_worker, i, row["artist_name"], row["spotify_link"]))

        completed = 0
        buffer = {}

        for fut in as_completed(tasks):
            idx, res = fut.result()
            completed += 1
            if res is not None:
                buffer[idx] = res

            if completed % config.SAVE_EVERY_N_ARTISTS == 0 or completed == total:
                new_rows = [buffer[k] for k in sorted(buffer.keys()) if buffer[k] is not None]
                buffer.clear()

                with results_lock:
                    results.extend(new_rows)

                try:
                    cur = pd.DataFrame(results)
                    if existing is not None and len(existing) > 0:
                        out_df = pd.concat([existing, cur], ignore_index=True)
                    else:
                        out_df = cur

                    out_df["artist_name"] = out_df["artist_name"].apply(clean_text)
                    out_df["spotify_link"] = out_df["spotify_link"].apply(clean_text)
                    out_df = out_df.drop_duplicates(subset=["artist_name", "spotify_link"], keep="last")
                    out_df.to_csv(output_csv_path, index=False)
                    existing = out_df
                except Exception:
                    pass

                flush_cache()

            if completed % 50 == 0 or completed == total:
                elapsed = time.time() - start
                print(f"Progress: {completed}/{total} | elapsed {elapsed:.1f}s")

    flush_cache()

    final_df = existing if existing is not None else pd.DataFrame(results)
    if final_df is None or len(final_df) == 0:
        print("No results produced.")
        return None

    total = len(final_df)
    mbids = final_df["mbid"].notna().sum() if "mbid" in final_df.columns else 0
    countries = final_df["country"].notna().sum() if "country" in final_df.columns else 0

    print("\nDone.")
    print(f"Artists: {total}")
    if total > 0:
        print(f"MBIDs: {mbids} ({mbids / total * 100:.1f}%)")
        print(f"Countries: {countries} ({countries / total * 100:.1f}%)")

    return final_df
