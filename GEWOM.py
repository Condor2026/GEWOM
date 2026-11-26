# ============================================================
# GLOBAL EARLY WARNING OSINT MONITOR — v2.4
# -By: Condor2026
# Fuentes (sin API / públicas): Tzeva Adom, Oref (oficial), alerts.com.ua,
#          Ubilling UA, Kyiv Digital, Neptun, USGS, NASA EONET,
#          GDELT GEO, RSS noticias, Telegram (canales públicos)
# Fuentes (con API opcional): NASA FIRMS, GDELT Cloud, World Monitor,
#          alerts.in.ua (token)
# Servidor: http://127.0.0.1:5000
#
# Dependencias:   pip install flask requests beautifulsoup4
# Opcional:       pip install telethon   (búsqueda nativa de Telegram)
#
# Variables de entorno (todas opcionales):
#   NASA_FIRMS_KEY, GDELT_API_KEY, WORLD_MONITOR_KEY, ALERTS_UA_TOKEN
#   TG_API_ID, TG_API_HASH, TG_SESSION      -> Telethon (python este.py --tg-login)
#   TG_MESSAGES_PER_CHANNEL (12), TG_MAX_AGE_HOURS (72), FIRMS_MIN_FRP (50)
#   EXTRA_RSS_FEEDS="Nombre|https://url/feed,Otro|https://url2/rss"
#   MONITOR_HOST (127.0.0.1), MONITOR_PORT (5000)
#   Si el puerto está ocupado se busca automáticamente el siguiente libre.
#   DEEPL_API_KEY / LIBRETRANSLATE_URL (+LIBRETRANSLATE_KEY) -> traductores opcionales
#       (sin claves se usa Google gtx + MyMemory, ambos públicos)
#   EXTRA_WAR_FEEDS / EXTRA_STATEMENT_FEEDS / EXTRA_STATE_FEEDS="Nombre|URL|A" separados por coma (A/B/C/D = credibilidad)
# ============================================================

import os
import re
import io
import sys
import csv
import json
import time
import base64
import random
import hashlib
import logging
import math
import socket
import sqlite3
import threading
import xml.etree.ElementTree as ET
from collections import Counter, OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, request, Response
from bs4 import BeautifulSoup

try:  # Telethon es opcional: solo se usa si hay TG_API_ID / TG_API_HASH
    import telethon  # noqa: F401
    TELETHON_OK = True
except Exception:
    TELETHON_OK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

APP_VERSION = "2.4"
APP_START = time.time()
FETCH_LOG = deque(maxlen=120)
_req_count = {"n": 0}
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "")
LIBRETRANSLATE_URL = os.getenv("LIBRETRANSLATE_URL", "").rstrip("/")
LIBRETRANSLATE_KEY = os.getenv("LIBRETRANSLATE_KEY", "")
WAR_MAX_AGE_H = float(os.getenv("WAR_MAX_AGE_H", "72"))
STATEMENT_MAX_AGE_H = float(os.getenv("STATEMENT_MAX_AGE_H", "72"))

NASA_FIRMS_KEY = os.getenv("NASA_FIRMS_KEY", "TU_MAP_KEY_AQUI")
GDELT_API_KEY = os.getenv("GDELT_API_KEY", "TU_GDELT_KEY_AQUI")
WORLD_MONITOR_KEY = os.getenv("WORLD_MONITOR_KEY", "TU_WM_KEY_AQUI")
ALERTS_UA_TOKEN = os.getenv("ALERTS_UA_TOKEN", "")

TG_API_ID = os.getenv("TG_API_ID", "")
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_SESSION = os.getenv("TG_SESSION", "osint_monitor_tg")
TG_CHANNELS_FILE = os.getenv("TG_CHANNELS_FILE", "telegram_channels.json")
TG_MESSAGES_PER_CHANNEL = int(os.getenv("TG_MESSAGES_PER_CHANNEL", "12"))
TG_MAX_AGE_HOURS = float(os.getenv("TG_MAX_AGE_HOURS", "72"))
FIRMS_MIN_FRP = float(os.getenv("FIRMS_MIN_FRP", "50"))

HOST = os.getenv("MONITOR_HOST", "127.0.0.1")
PORT = int(os.getenv("MONITOR_PORT", "5000"))

DB_PATH = "alerts_history.db"
CACHE_TTL_SECONDS = 15

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]


# ============================================================
# UTILIDADES GENERALES
# ============================================================

def get_headers(html: bool = False) -> Dict[str, str]:
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if html:
        h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    return h


def create_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=1, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def normalize_dt(value) -> str:
    """Convierte epoch (s/ms) o ISO a ISO UTC. Si falla, devuelve 'ahora'."""
    try:
        if value is None or value == "":
            return now_iso()
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
            v = float(value)
            if v > 1e12:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc).isoformat()
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return now_iso()


def age_seconds(iso: str) -> float:
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return 0.0


def run_parallel(fn, items, workers: int = 6):
    """Ejecuta fn(item) en paralelo. Devuelve lista de (item, resultado, error)."""
    results = []
    if not items:
        return results
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(items)))) as ex:
        futs = [(it, ex.submit(fn, it)) for it in items]
        for it, f in futs:
            try:
                results.append((it, f.result(), None))
            except Exception as e:
                results.append((it, [], e))
    return results


# ------------------------------------------------------------
# Estado de fuentes (para la pestaña ERRORES y /api/sources)
# ------------------------------------------------------------
_status_lock = threading.Lock()
SOURCE_STATUS: Dict[str, Dict[str, Any]] = {}


def _set_status(name: str, status: str, count: int = 0, error: str = ""):
    with _status_lock:
        prev = SOURCE_STATUS.get(name, {})
        SOURCE_STATUS[name] = {"status": status, "count": count, "error": str(error)[:200],
                               "ts": now_iso(), "ms": prev.get("ms", 0)}


def _ok(name: str, alerts: List[dict]) -> List[dict]:
    logger.info(f"[{name}] {len(alerts)}")
    _set_status(name, "ok", len(alerts))
    return alerts


def _warn(name: str, alerts: List[dict], msg: str) -> List[dict]:
    logger.info(f"[{name}] {len(alerts)} (aviso: {msg})")
    _set_status(name, "warn", len(alerts), msg)
    return alerts


def _skip(name: str, why: str) -> List[dict]:
    logger.info(f"[{name}] {why}. Saltando.")
    _set_status(name, "skipped", 0, why)
    return []


def _fail(name: str, e) -> List[dict]:
    logger.warning(f"[{name}] {e}")
    _set_status(name, "error", 0, str(e))
    return []


# ============================================================
# GEOLOCALIZACIÓN APROXIMADA POR TEXTO (RU / UK / EN / HE-ES)
# ============================================================
PLACES: List[Tuple[str, float, float, Tuple[str, ...]]] = [
    ("Kyiv", 50.45, 30.52, ("kyiv", "kiev", "киев", "київ")),
    ("Kharkiv", 49.99, 36.23, ("kharkiv", "kharkov", "харьк", "харків")),
    ("Odesa", 46.48, 30.73, ("odesa", "odessa", "одесс", "одеса", "одесі", "одеськ")),
    ("Dnipro", 48.46, 35.05, ("dnipro", "днепр", "дніпр")),
    ("Zaporizhzhia", 47.84, 35.14, ("zaporizhzhia", "zaporozhye", "запорож", "запоріж", "запорізьк")),
    ("Lviv", 49.84, 24.03, ("lviv", "lvov", "львов", "львів")),
    ("Kherson", 46.64, 32.62, ("kherson", "херсон")),
    ("Mykolaiv", 46.97, 32.00, ("mykolaiv", "nikolaev", "николаев", "миколаїв")),
    ("Sumy", 50.91, 34.80, ("sumy", "суми", "сумы", "сумськ", "сумщин")),
    ("Chernihiv", 51.49, 31.29, ("chernihiv", "chernigov", "чернигов", "чернігів")),
    ("Poltava", 49.59, 34.55, ("poltava", "полтав")),
    ("Donetsk", 48.00, 37.80, ("donetsk", "донецк", "донецьк")),
    ("Luhansk", 48.57, 39.31, ("luhansk", "lugansk", "луганск", "луганськ")),
    ("Mariupol", 47.10, 37.54, ("mariupol", "мариупол", "маріупол")),
    ("Crimea", 44.95, 34.10, ("crimea", "крым", "криму", "кримськ")),
    ("Sevastopol", 44.62, 33.52, ("sevastopol", "севастопол")),
    ("Kramatorsk", 48.72, 37.56, ("kramatorsk", "краматорск")),
    ("Sloviansk", 48.87, 37.62, ("sloviansk", "славянск", "слов'янськ")),
    ("Bakhmut", 48.59, 38.00, ("bakhmut", "бахмут")),
    ("Pokrovsk", 48.28, 37.18, ("pokrovsk", "покровск")),
    ("Avdiivka", 48.14, 37.75, ("avdiivka", "авдеевк", "авдіївк")),
    ("Kupiansk", 49.71, 37.62, ("kupiansk", "купянск", "куп'янськ")),
    ("Izium", 49.21, 37.26, ("izium", "изюм", "ізюм")),
    ("Kryvyi Rih", 47.91, 33.39, ("kryvyi rih", "кривой рог", "кривий ріг")),
    ("Nikopol", 47.57, 34.40, ("nikopol", "никополь", "нікополь")),
    ("Vinnytsia", 49.23, 28.47, ("vinnytsia", "винниц", "вінниц")),
    ("Zhytomyr", 50.25, 28.66, ("zhytomyr", "житомир")),
    ("Rivne", 50.62, 26.25, ("rivne", "ривне", "рівне")),
    ("Lutsk", 50.75, 25.34, ("lutsk", "луцк", "луцьк", "волин")),
    ("Ternopil", 49.55, 25.59, ("ternopil", "тернопол", "тернопіл")),
    ("Chernivtsi", 48.29, 25.94, ("chernivtsi", "черновц", "чернівц", "чернівец")),
    ("Ivano-Frankivsk", 48.92, 24.71, ("ivano-frankivsk", "ивано-франк", "івано-франк")),
    ("Uzhhorod", 48.62, 22.29, ("uzhhorod", "ужгород", "закарпат")),
    ("Cherkasy", 49.44, 32.06, ("cherkasy", "черкас")),
    ("Kropyvnytskyi", 48.51, 32.26, ("kropyvnytskyi", "кропивниц", "кировоград", "кіровоград")),
    ("Khmelnytskyi", 49.42, 26.99, ("khmelnytskyi", "хмельницк", "хмельницьк")),
    ("Belgorod", 50.60, 36.59, ("belgorod", "белгород")),
    ("Kursk", 51.73, 36.19, ("kursk", "курск")),
    ("Bryansk", 53.24, 34.37, ("bryansk", "брянск")),
    ("Voronezh", 51.67, 39.18, ("voronezh", "воронеж")),
    ("Moscow", 55.75, 37.62, ("moscow", "москв", "москов")),
    ("St Petersburg", 59.93, 30.34, ("st petersburg", "saint petersburg", "санкт-петербург", "петербург")),
    ("Rostov", 47.22, 39.72, ("rostov", "ростов")),
    ("Krasnodar", 45.04, 38.98, ("krasnodar", "краснодар")),
    ("Novorossiysk", 44.72, 37.77, ("novorossiysk", "новороссийск")),
    ("Tuapse", 44.10, 39.07, ("tuapse", "туапсе")),
    ("Tel Aviv", 32.08, 34.78, ("tel aviv", "tel-aviv", "тель-авив")),
    ("Jerusalem", 31.77, 35.21, ("jerusalem", "иерусалим")),
    ("Haifa", 32.79, 34.99, ("haifa", "хайф")),
    ("Gaza", 31.50, 34.47, ("gaza", "сектор газа")),
    ("Ashdod", 31.80, 34.65, ("ashdod", "ашдод")),
    ("Ashkelon", 31.67, 34.57, ("ashkelon", "ашкелон")),
    ("Beersheba", 31.25, 34.79, ("beersheba", "beer sheva", "беэр-шев")),
    ("Sderot", 31.52, 34.60, ("sderot", "сдерот")),
    ("Eilat", 29.56, 34.95, ("eilat", "эйлат")),
    ("Beirut", 33.89, 35.50, ("beirut", "бейрут")),
    ("Damascus", 33.51, 36.29, ("damascus", "дамаск")),
    ("Tehran", 35.69, 51.39, ("tehran", "тегеран")),
    ("Isfahan", 32.65, 51.67, ("isfahan", "исфахан")),
    ("Sanaa", 15.37, 44.19, ("sanaa",)),
]


COUNTRIES: List[Tuple[str, float, float, str]] = [
    ("Ukraine", 49.0, 32.0, r"\b(?:ukrain\w*|украин\w*|україн\w*|ucrania\w*)"),
    ("Russia", 56.0, 40.0, r"\b(?:russia\w*|росси\w*|рф\b|rusia\w*)"),
    ("Belarus", 53.7, 27.9, r"\b(?:belarus\w*|беларус\w*|білорус\w*|bielorrusia)"),
    ("Poland", 52.0, 19.4, r"\b(?:poland|polish|польш\w*|польск\w*|polonia)"),
    ("Romania", 45.9, 25.0, r"\b(?:romania\w*|румын\w*|румун\w*|rumania)"),
    ("Moldova", 47.2, 28.4, r"\b(?:moldova\w*|молдов\w*|moldavia)"),
    ("Lithuania", 55.2, 23.9, r"\b(?:lithuania\w*|литв\w*|lituania)"),
    ("Latvia", 56.9, 24.6, r"\b(?:latvia\w*|латви\w*|letonia)"),
    ("Estonia", 58.7, 25.5, r"\b(?:estonia\w*|эстони\w*)"),
    ("Finland", 64.0, 26.0, r"\b(?:finland|finnish|финлянд\w*|finlandia)"),
    ("Armenia", 40.2, 44.5, r"\b(?:armenia\w*|армени\w*)"),
    ("Azerbaijan", 40.3, 47.7, r"\b(?:azerbaijan\w*|азербайджан\w*)"),
    ("Israel", 31.5, 34.9, r"\b(?:israel\w*|израил\w*|ізраїл\w*)"),
    ("Lebanon", 33.9, 35.9, r"\b(?:leban\w*|ливан\w*|líbano|libano)"),
    ("Syria", 34.8, 38.9, r"\b(?:syria\w*|сири\w*|siria)"),
    ("Iraq", 33.0, 43.7, r"\b(?:iraq\w*|ирак\w*|irak)"),
    ("Iran", 32.4, 53.7, r"\b(?:iran\w*|иран\w*|irán)"),
    ("Yemen", 15.6, 48.0, r"\b(?:yemen\w*|йемен\w*|houthi\w*|хуси\w*)"),
    ("Saudi Arabia", 24.0, 45.0, r"\b(?:saudi\w*|саудов\w*|arabia saudita)"),
    ("Turkey", 39.0, 35.2, r"\b(?:turkey|turkish|türkiye|турци\w*|turquía)"),
    ("Egypt", 26.8, 30.8, r"\b(?:egypt\w*|египт\w*|egipto)"),
    ("Jordan", 31.2, 36.5, r"\b(?:jordan\w*|иордан\w*|jordania)"),
    ("West Bank", 31.9, 35.3, r"\b(?:west bank|cisjordania|западн\w* берег\w*)"),
    ("Afghanistan", 33.9, 67.7, r"\b(?:afghan\w*|афганистан\w*)"),
    ("Pakistan", 30.4, 69.3, r"\b(?:pakistan\w*|пакистан\w*)"),
    ("India", 21.0, 78.0, r"\b(?:india\b|indian\b|индия\b|индии\b|индийск\w*)"),
    ("China", 35.0, 103.0, r"\b(?:china\b|chinese\b|китай\w*|китае\b|китая\b)"),
    ("Taiwan", 23.7, 121.0, r"\b(?:taiwan\w*|тайван\w*)"),
    ("North Korea", 40.3, 127.5, r"\b(?:north korea\w*|dprk|северн\w* коре\w*|corea del norte|кндр)"),
    ("South Korea", 36.5, 127.9, r"\b(?:south korea\w*|южн\w* коре\w*|corea del sur)"),
    ("Japan", 36.2, 138.3, r"\b(?:japan\w*|япони\w*|japón)"),
    ("Sudan", 15.6, 30.0, r"\b(?:sudan\w*|судан\w*|sudán)"),
    ("Somalia", 5.2, 46.2, r"\bsomali\w*"),
    ("Libya", 27.0, 17.0, r"\b(?:libya\w*|ливи\w*|libia)"),
    ("Myanmar", 21.9, 95.9, r"\b(?:myanmar|burma|мьянм\w*)"),
    ("Venezuela", 7.0, -66.0, r"\b(?:venezuela\w*|венесуэл\w*)"),
    ("Haiti", 19.0, -72.4, r"\b(?:haiti\w*|гаити)"),
    ("DR Congo", -2.9, 23.7, r"\b(?:congo\w*|конго)"),
    ("Serbia", 44.0, 21.0, r"\b(?:serbia\w*|серби\w*)"),
    ("Kosovo", 42.6, 20.9, r"\b(?:kosovo\w*|косов\w*)"),
]
_COUNTRY_RE = [(n, la, lo, re.compile(p)) for n, la, lo, p in COUNTRIES]


def geolocate_text(text: str) -> Optional[Tuple[float, float, str]]:
    """Devuelve (lat, lon, nombre) del primer lugar conocido que aparece en el texto."""
    if not text:
        return None
    low = text.lower()
    best = None
    for name, lat, lon, aliases in PLACES:
        for al in aliases:
            idx = low.find(al)
            if idx != -1 and (best is None or idx < best[0]):
                best = (idx, name, lat, lon)
    if best:
        return (best[2], best[3], best[1])
    cbest = None
    for name, lat, lon, rx in _COUNTRY_RE:
        m = rx.search(low)
        if m and (cbest is None or m.start() < cbest[0]):
            cbest = (m.start(), name, lat, lon)
    return (cbest[2], cbest[3], cbest[1]) if cbest else None


# ============================================================
# BASE DE DATOS
# ============================================================
_db_lock = threading.Lock()


def init_db():
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT, alert_type TEXT, region TEXT,
            lat REAL, lon REAL, severity TEXT, description TEXT,
            raw_json TEXT, detected_at TEXT,
            fingerprint TEXT, link TEXT, cred TEXT, tags TEXT,
            UNIQUE(source, region, detected_at)
        )
        """)
        # Migración de bases antiguas (v2.1) sin romper datos existentes
        cols = [r[1] for r in conn.execute("PRAGMA table_info(alerts)")]
        if "fingerprint" not in cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN fingerprint TEXT")
        if "link" not in cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN link TEXT")
        if "cred" not in cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN cred TEXT")
        if "tags" not in cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN tags TEXT")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_fp ON alerts(fingerprint)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_detected ON alerts(detected_at)")
        conn.commit()
        conn.close()


def make_fingerprint(alert: dict) -> str:
    base = "|".join([
        str(alert.get("source")), str(alert.get("type")), str(alert.get("region")),
        str(alert.get("description"))[:160], str(alert.get("detected_at", ""))[:13],
    ])
    return hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()


_SAVE_SQL = """
INSERT OR IGNORE INTO alerts (source, alert_type, region, lat, lon, severity, description, raw_json, detected_at, fingerprint, link, cred, tags)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _alert_row(alert: dict):
    return (
        alert.get("source"), alert.get("type"), alert.get("region"),
        alert.get("lat"), alert.get("lon"), alert.get("severity"),
        alert.get("description"), json.dumps(alert.get("raw", {}), ensure_ascii=False, default=str),
        alert.get("detected_at") or now_iso(), make_fingerprint(alert), alert.get("link"),
        alert.get("cred"), ",".join(alert.get("tags") or []),
    )


def save_alerts(alerts: List[dict]):
    if not alerts:
        return
    try:
        rows = [_alert_row(a) for a in alerts]
        with _db_lock:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            try:
                conn.executemany(_SAVE_SQL, rows)
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        logger.warning(f"DB: {e}")


def save_alert(alert: dict):
    save_alerts([alert])


def query_history(hours: float = 24, source: str = "", q: str = "", limit: int = 500) -> List[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    sql = ("SELECT source, alert_type, region, lat, lon, severity, description, detected_at, link, cred, tags "
           "FROM alerts WHERE detected_at >= ?")
    params: List[Any] = [since]
    if source:
        sql += " AND source = ?"
        params.append(source)
    if q:
        sql += " AND (description LIKE ? OR region LIKE ? OR source LIKE ?)"
        params += [f"%{q}%"] * 3
    sql += " ORDER BY detected_at DESC LIMIT ?"
    params.append(max(1, min(int(limit), 5000)))
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
    return [{
        "source": r[0], "type": r[1], "region": r[2], "lat": r[3], "lon": r[4],
        "severity": r[5], "description": r[6], "detected_at": r[7], "link": r[8],
        "cred": r[9] or "D", "tags": [t for t in (r[10] or "").split(",") if t],
    } for r in rows]


# ============================================================
# UTILIDADES v2.3: distancias, credibilidad de fuentes, comunicados
# ============================================================
_LEVEL_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def epoch_of(iso) -> float:
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return time.time()


# Credibilidad: A oficial/instrumental, B prensa/análisis/OSINT verificable,
# C parte implicada (afirmación oficial sin verificar), D no verificado.
# OJO: ninguna fuente garantiza que algo sea cierto en una guerra; el nivel solo indica
# de qué tipo de fuente viene el dato. Contrasta siempre con 2+ fuentes independientes.
CRED_LABELS = {"A": "Oficial / instrumental", "B": "Prensa / análisis / OSINT verificable",
               "C": "Parte implicada (afirmación)", "D": "No verificado"}
CRED_RULES = [
    ("estatal —", "C"), ("centcom", "C"), ("pentágono", "C"), ("casa blanca", "C"), ("ue acción exterior", "C"), ("ue consejo", "C"),
    ("(viajes)", "A"), ("(noticias)", "C"), ("(prensa)", "C"), ("reino unido mod", "C"), ("kremlin", "C"), ("otan", "C"),
    ("usgs", "A"), ("emsc", "A"), ("geofon", "A"), ("eonet", "A"), ("firms", "A"), ("oref", "A"), ("tzeva", "A"),
    ("kyiv digital", "A"), ("alerts.com.ua", "A"), ("alerts.in.ua", "A"), ("ubilling", "A"),
    ("onu", "A"), ("oiea", "A"),
    ("bellingcat", "B"), ("war on the rocks", "B"), ("breaking defense", "B"), ("meduza", "B"),
    ("moscow times", "B"), ("kyiv independent", "B"), ("ukrainska", "B"), ("euromaidan", "B"), ("oryx", "B"),
    ("critical threats", "B"), ("isw", "B"), ("rss —", "B"), ("gdelt", "B"), ("deepstate", "B"),
    ("neptun", "D"),
]


def cred_for(source: str) -> str:
    low = (source or "").lower()
    for token, tier in CRED_RULES:
        if token in low:
            return tier
    return "D"


# Detección de comunicados especiales (multi-idioma). Cada regla: (nivel, etiqueta, patrones)
STATEMENT_RULES = [
    ("critical", "guerra", ["declaration of war", "declares war", "declared war", "state of war", "объявил войну",
                            "объявила войну", "объявление войны", "оголосив війну", "declaración de guerra",
                            "declara la guerra", "declaró la guerra"]),
    ("critical", "nuclear", ["nuclear weapon", "nuclear strike", "nuclear threat", "nuclear alert", "nuclear doctrine",
                             "nuclear test", "nuclear attack", "tactical nuclear", "nuclear forces", "ядерное оружие",
                             "ядерный удар", "ядерного оружия", "ядерных сил", "ядерна зброя", "ядерного удару",
                             "arma nuclear", "armas nucleares", "ataque nuclear", "radiation leak", "fuga radiactiva"]),
    ("critical", "ley marcial", ["martial law", "военное положение", "военного положения", "воєнний стан",
                                 "воєнного стану", "ley marcial"]),
    ("critical", "movilización", ["general mobilization", "general mobilisation", "movilización general",
                                  "общая мобилизация", "загальна мобілізація"]),
    ("critical", "otan", ["article 5", "artículo 5", "статья 5"]),
    ("critical", "embajada", ["ordered departure"]),
    ("high", "movilización", ["mobilization", "mobilisation", "movilización", "мобилизац", "мобілізац"]),
    ("high", "embajada", ["authorized departure", "evacuate embassy", "embassy evacuat", "embassy closed",
                          "embassy suspend", "suspends embassy", "closes embassy", "closing its embassy",
                          "cierra su embajada", "cierre de la embajada", "shelter in place", "shelter-in-place"]),
    ("high", "viaje", ["advise against all travel", "advises against all travel", "level 4", "do not travel",
                       "no viajar", "nivel 4"]),
    ("high", "espacio aéreo", ["airspace closure", "airspace closed", "closes airspace", "closed its airspace",
                               "no-fly zone", "no fly zone", "закрыт воздушн", "закрытие воздушного пространства",
                               "закрыли воздушное пространство", "cierre del espacio aéreo", "cierra su espacio aéreo"]),
    ("high", "emergencia", ["state of emergency", "emergency decree", "режим чрезвычайн", "чрезвычайное положение",
                            "estado de emergencia", "estado de excepción", "emergency meeting", "emergency session"]),
    ("high", "ultimátum", ["ultimatum", "red line", "ультиматум", "красная линия", "красные линии"]),
    ("high", "evacuación", ["evacuation order", "evacuate", "evacuation of", "эвакуац", "евакуац", "evacuación"]),
    ("high", "otan", ["article 4", "artículo 4"]),
    ("medium", "viaje", ["advise against all but essential travel", "level 3", "reconsider travel", "nivel 3"]),
    ("medium", "onu", ["security council", "consejo de seguridad", "совет безопасности оон", "совбез оон"]),
    ("medium", "sanciones", ["sanctions", "sanciones", "санкци"]),
    ("medium", "despliegue", ["troop deployment", "deploys troops", "deployment of troops", "snap exercise",
                              "military exercises", "ejercicio militar", "despliegue de tropas"]),
    ("medium", "diplomacia", ["ceasefire", "cease-fire", "alto el fuego", "peace talks", "peace deal", "truce",
                              "tregua", "перемирие", "режим тишины"]),
]
WARNING_TAGS = {"guerra", "nuclear", "ley marcial", "movilización", "otan", "espacio aéreo", "embajada",
                "ultimátum", "evacuación"}


def classify_statement(text: str) -> Tuple[str, List[str]]:
    """Devuelve (nivel, etiquetas) según palabras clave de comunicados de crisis."""
    low = (text or "").lower()
    level, tags = "low", []
    for lvl, tag, pats in STATEMENT_RULES:
        if any(p in low for p in pats):
            if tag not in tags:
                tags.append(tag)
            if _LEVEL_ORDER[lvl] > _LEVEL_ORDER[level]:
                level = lvl
    return level, tags


_STOPWORDS = set("""this that with from have been were will their about after which also said over into more than when
there what would could other some only amid says para como pero este esta desde entre sobre также этого этот было были
после который которые через между более очень только есть свои сказал що які для після його вона було були цього також
дуже того если чтобы когда уже ещё они него нее была будет можно будут которая которого""".split())


# ============================================================
# FUENTES — ISRAEL
# ============================================================
_tzeva_cities: Dict[str, Any] = {"data": {}, "ts": 0.0}
_tzeva_lock = threading.Lock()


def load_tzeva_cities() -> Dict[str, Tuple[float, float]]:
    """Carga (una vez/hora) coordenadas de ciudades israelíes de tzevaadom.co.il."""
    with _tzeva_lock:
        if _tzeva_cities["data"] or (time.time() - _tzeva_cities["ts"] < 3600):
            return _tzeva_cities["data"]
        _tzeva_cities["ts"] = time.time()
    out: Dict[str, Tuple[float, float]] = {}
    try:
        r = create_session().get("https://www.tzevaadom.co.il/static/cities.json", headers=get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json()
        cities = data.get("cities", data) if isinstance(data, dict) else {}
        for k, v in cities.items():
            if isinstance(v, dict):
                lat = to_float(v.get("lat"))
                lon = to_float(v.get("lng", v.get("lon")))
                if lat is not None and lon is not None:
                    out[k] = (lat, lon)
                    for alt in (v.get("he"), v.get("en")):
                        if alt:
                            out[alt] = (lat, lon)
    except Exception as e:
        logger.info(f"[Tzeva Adom] cities.json no disponible: {e}")
    with _tzeva_lock:
        _tzeva_cities["data"] = out
    return out


def _coords_for_cities(cities: List[str]) -> Optional[Tuple[float, float]]:
    db = load_tzeva_cities()
    pts = [db[c] for c in cities if c in db]
    if not pts:
        return None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def _iter_tzeva_items(data):
    rows = data if isinstance(data, list) else []
    if isinstance(data, dict):
        rows = data.get("alerts") or data.get("data") or []
    for item in rows:
        if not isinstance(item, dict):
            continue
        subs = item.get("alerts")
        if isinstance(subs, list) and subs:
            for s in subs:
                if isinstance(s, dict):
                    yield s
        else:
            yield item


def fetch_tzeva_adom() -> List[dict]:
    name = "Tzeva Adom"
    url = "https://api.tzevaadom.co.il/alerts-history"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        for item in _iter_tzeva_items(data):
            if len(alerts) >= 40:
                break
            if item.get("isDrill"):
                continue
            ts = item.get("time") or item.get("timestamp")
            detected = normalize_dt(ts) if ts else now_iso()
            if ts and age_seconds(detected) > 7200:
                continue
            cities = item.get("cities", [])
            region = ", ".join(cities) if isinstance(cities, list) else str(cities)[:80]
            region = region[:200]
            threat = int(to_float(item.get("threat"), 0) or 0)
            if threat == 5:
                emoji, kind = "🛩", "de aeronave hostil / dron"
            elif threat == 0:
                emoji, kind = "🚀", "de cohetes / misiles"
            else:
                emoji, kind = "⚠", f"especial (código {threat})"
            coords = _coords_for_cities(cities if isinstance(cities, list) else [])
            if coords:
                lat, lon, approx = coords[0], coords[1], False
            else:
                lat, lon, approx = 31.5 + (random.random() - 0.5) * 2, 34.9 + (random.random() - 0.5) * 1.5, True
            alerts.append({
                "source": "Tzeva Adom (Israel)", "type": "missile_alert",
                "region": region or "Israel",
                "lat": lat, "lon": lon, "geo_approx": approx,
                "severity": "critical",
                "description": f"{emoji} Alerta {kind}: {region or 'zona no especificada'}",
                "emoji": emoji,
                "detected_at": detected, "raw": item
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_oref() -> List[dict]:
    """Oref (Mando del Frente Interno, oficial). Puede estar bloqueado fuera de Israel."""
    name = "Oref (oficial)"
    url = "https://www.oref.org.il/WarningMessages/alert/alerts.json"
    headers = get_headers()
    headers.update({"Referer": "https://www.oref.org.il/", "X-Requested-With": "XMLHttpRequest"})
    try:
        resp = create_session().get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig", errors="ignore").strip()
        alerts = []
        if text:
            data = json.loads(text)
            cities = data.get("data", []) if isinstance(data, dict) else []
            if isinstance(cities, str):
                cities = [cities]
            if cities:
                title = data.get("title", "Alerta") if isinstance(data, dict) else "Alerta"
                cat = str(data.get("cat", "")) if isinstance(data, dict) else ""
                emoji = "🛩" if cat == "6" else "🚀"
                region = ", ".join(cities)[:200]
                coords = _coords_for_cities(cities)
                if coords:
                    lat, lon, approx = coords[0], coords[1], False
                else:
                    lat, lon, approx = 31.5 + (random.random() - 0.5) * 2, 34.9 + (random.random() - 0.5) * 1.5, True
                alerts.append({
                    "source": "Oref (Israel, oficial)", "type": "missile_alert",
                    "region": region, "lat": lat, "lon": lon, "geo_approx": approx,
                    "severity": "critical",
                    "description": f"{emoji} Oref: {title} — {region}",
                    "emoji": emoji,
                    "detected_at": now_iso(), "raw": data
                })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ============================================================
# FUENTES — UCRANIA
# ============================================================
def fetch_alerts_ua() -> List[dict]:
    name = "alerts.com.ua"
    url = "https://alerts.com.ua/api/states"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        states = data.get("states", []) if isinstance(data, dict) else data
        alerts = []
        for st in states:
            if not isinstance(st, dict):
                continue
            if st.get("alert") or st.get("alert_type"):
                lat, lon = to_float(st.get("lat")), to_float(st.get("lon"))
                if lat is None or lon is None:
                    loc = geolocate_text(str(st.get("name", "")))
                    lat, lon = (loc[0], loc[1]) if loc else (49.0, 32.0)
                alerts.append({
                    "source": "alerts.com.ua (Ucrania)", "type": "air_raid",
                    "region": st.get("name", "Ucrania"),
                    "lat": lat, "lon": lon,
                    "severity": "high",
                    "description": f"Alerta aérea en {st.get('name', 'óblast')}",
                    "detected_at": now_iso(), "raw": st
                })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_ubilling() -> List[dict]:
    """Mapa público de alertas aéreas de Ucrania (sin API key)."""
    name = "Ubilling UA"
    url = "https://ubilling.net.ua/aerialalerts/"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        states = data.get("states", data) if isinstance(data, dict) else {}
        alerts = []
        for oblast, st in states.items():
            if not isinstance(st, dict):
                continue
            if not (st.get("alertnow") or st.get("alert")):
                continue
            loc = geolocate_text(str(oblast))
            lat, lon = (loc[0], loc[1]) if loc else (49.0, 32.0)
            alerts.append({
                "source": "Ubilling (Ucrania)", "type": "air_raid",
                "region": str(oblast), "lat": lat, "lon": lon, "geo_approx": True,
                "severity": "high",
                "description": f"Alerta aérea en {oblast}",
                "detected_at": now_iso(), "raw": st
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_alerts_in_ua() -> List[dict]:
    """alerts.in.ua (requiere token gratuito ALERTS_UA_TOKEN)."""
    name = "Alerts.in.ua"
    if not ALERTS_UA_TOKEN:
        return _skip(name, "Sin token")
    url = "https://api.alerts.in.ua/v1/alerts/active.json"
    headers = get_headers()
    headers["Authorization"] = f"Bearer {ALERTS_UA_TOKEN}"
    try:
        resp = create_session().get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        for al in data.get("alerts", []):
            title = al.get("location_title") or al.get("location_oblast") or "Ucrania"
            loc = geolocate_text(str(title) + " " + str(al.get("location_oblast", "")))
            lat, lon = (loc[0], loc[1]) if loc else (49.0, 32.0)
            atype = al.get("alert_type", "air_raid")
            alerts.append({
                "source": "alerts.in.ua (Ucrania)", "type": "air_raid",
                "region": str(title), "lat": lat, "lon": lon, "geo_approx": True,
                "severity": "high",
                "description": f"Alerta {atype} en {title}",
                "detected_at": normalize_dt(al.get("started_at")), "raw": al
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_kyiv_digital() -> List[dict]:
    name = "Kyiv Digital"
    url = "https://kyiv.digital/open-api/air-alert/state"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            if it.get("state", 0) in (1, True):
                causes = it.get("causes", []) or it.get("cause", [])
                if isinstance(causes, str):
                    causes = [causes]
                cause_str = ", ".join(causes) if causes else "unknown"
                sev = "critical" if any(c in ("missile", "ballistic", "massive-drone") for c in causes) else "high"
                alerts.append({
                    "source": "Kyiv Digital (oficial)", "type": "kyiv_alert",
                    "region": "Kyiv", "lat": 50.45, "lon": 30.52,
                    "severity": sev,
                    "description": f"Alerta Kyiv — amenaza: {cause_str}",
                    "detected_at": normalize_dt(it.get("created_at")),
                    "raw": it
                })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_neptun() -> List[dict]:
    name = "Neptun"
    url = "https://neptun.in.ua/api/tracks"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        tracks = data if isinstance(data, list) else data.get("tracks", [])
        alerts = []
        for tr in tracks[:20]:
            if not isinstance(tr, dict):
                continue
            lat, lon = to_float(tr.get("lat")), to_float(tr.get("lon"))
            if lat is None or lon is None:
                loc = geolocate_text(str(tr.get("region", "")))
                lat, lon = (loc[0], loc[1]) if loc else (49.0, 32.0)
            alerts.append({
                "source": "Neptun (Ucrania)", "type": "drone_missile_track",
                "region": tr.get("region", "Ucrania"),
                "lat": lat, "lon": lon,
                "severity": "high",
                "description": f"Track: {tr.get('type', 'aéreo')} — {tr.get('direction', 'dirección desconocida')}",
                "detected_at": now_iso(), "raw": tr
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ============================================================
# FUENTES — SATÉLITE / GLOBALES
# ============================================================
def fetch_nasa_firms() -> List[dict]:
    name = "NASA FIRMS"
    if NASA_FIRMS_KEY == "TU_MAP_KEY_AQUI":
        return _skip(name, "Sin MAP_KEY")
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_FIRMS_KEY}/VIIRS_SNPP_NRT/world/1"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "latitude" not in text.split("\n", 1)[0]:
            return _fail(name, f"Respuesta no CSV: {text[:100]}")
        rows = []
        for row in csv.DictReader(io.StringIO(text)):
            frp = to_float(row.get("frp"), 0.0)
            lat, lon = to_float(row.get("latitude")), to_float(row.get("longitude"))
            if frp is None or frp <= FIRMS_MIN_FRP or lat is None or lon is None:
                continue
            rows.append((frp, lat, lon, row))
        rows.sort(key=lambda r: r[0], reverse=True)
        alerts = []
        for frp, lat, lon, row in rows[:150]:
            detected = now_iso()
            try:
                dt = datetime.strptime(f"{row.get('acq_date')} {str(row.get('acq_time', '0')).zfill(4)}", "%Y-%m-%d %H%M")
                detected = dt.replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                pass
            alerts.append({
                "source": "NASA FIRMS", "type": "thermal_anomaly",
                "region": f"Lat {row.get('latitude')}, Lon {row.get('longitude')}",
                "lat": lat, "lon": lon,
                "severity": "medium",
                "description": f"Anomalía térmica (FRP={frp:.0f} MW)",
                "detected_at": detected, "raw": row
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_gdelt() -> List[dict]:
    name = "GDELT"
    if GDELT_API_KEY == "TU_GDELT_KEY_AQUI":
        return _skip(name, "Sin key")
    url = "https://gdeltcloud.com/api/v2/events"
    headers = {"Authorization": f"Bearer {GDELT_API_KEY}", "Accept": "application/json"}
    params = {"family": "conflict", "category": "Explosions/Remote violence", "limit": 20, "sort": "event_date:desc"}
    try:
        resp = create_session().get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("data", []) if isinstance(data, dict) else data
        alerts = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            geo = ev.get("geo") or {}
            alerts.append({
                "source": "GDELT Cloud", "type": "conflict_event",
                "region": geo.get("location", geo.get("country", "Global")),
                "lat": geo.get("latitude", 0), "lon": geo.get("longitude", 0),
                "severity": "high" if ev.get("has_fatalities") else "medium",
                "description": str(ev.get("title") or "Evento de violencia remota")[:150],
                "detected_at": normalize_dt(ev.get("event_date")), "raw": ev
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_gdelt_geo() -> List[dict]:
    """GDELT GEO API pública (sin key): menciones geolocalizadas de ataques."""
    name = "GDELT GEO"
    url = "https://api.gdeltproject.org/api/v2/geo/geo"
    params = {
        "query": "(missile OR airstrike OR drone OR shelling OR explosion)",
        "format": "GeoJSON", "timespan": "6h", "maxpoints": 150,
    }
    try:
        resp = create_session().get(url, params=params, headers=get_headers(), timeout=20)
        resp.raise_for_status()
        data = resp.json()
        feats = data.get("features", []) if isinstance(data, dict) else []
        pts = []
        for f in feats:
            try:
                props = f.get("properties", {}) or {}
                coords = (f.get("geometry") or {}).get("coordinates") or []
                lon, lat = float(coords[0]), float(coords[1])
                count = to_float(props.get("count"), 1.0) or 1.0
                pts.append((count, lat, lon, str(props.get("name", "Global")), props))
            except Exception:
                continue
        pts.sort(key=lambda p: p[0], reverse=True)
        alerts = []
        for count, lat, lon, place, props in pts[:40]:
            alerts.append({
                "source": "GDELT GEO (público)", "type": "news_geo",
                "region": place[:120], "lat": lat, "lon": lon,
                "severity": "medium" if count >= 5 else "low",
                "description": f"{place[:100]} — {int(count)} menciones de ataque/explosión (6h)",
                "detected_at": now_iso(), "raw": {"name": place, "count": count}
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


def fetch_world_monitor() -> List[dict]:
    name = "World Monitor"
    if WORLD_MONITOR_KEY == "TU_WM_KEY_AQUI":
        return _skip(name, "Sin key")
    url = "https://api.worldmonitor.app/api/conflict/v1/list-acled-events"
    headers = {"X-WorldMonitor-Key": WORLD_MONITOR_KEY, "Accept": "application/json"}
    try:
        resp = create_session().get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", data.get("data", []))
        alerts = []
        for ev in events[:20]:
            if not isinstance(ev, dict):
                continue
            alerts.append({
                "source": "World Monitor (ACLED)", "type": "acled_conflict",
                "region": ev.get("location", ev.get("country", "Global")),
                "lat": ev.get("latitude", ev.get("lat", 0)),
                "lon": ev.get("longitude", ev.get("lon", 0)),
                "severity": "high" if (to_float(ev.get("fatalities"), 0) or 0) > 0 else "medium",
                "description": str(ev.get("event_type") or ev.get("title") or "Evento de conflicto")[:150],
                "detected_at": normalize_dt(ev.get("event_date")), "raw": ev
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ------------------------------------------------------------
# SENSORES SÍSMICOS PÚBLICOS (USGS + EMSC + GEOFON) — detección de explosiones
# Un sismo muy superficial, pequeño y dentro de una zona de conflicto es un CANDIDATO a
# explosión (heurístico, no una prueba). Si además coincide con alertas/reportes de ataque
# cercanos en tiempo y espacio, se marca como "correlado".
# ------------------------------------------------------------
SEISMIC_TYPES = ("earthquake", "seismic_explosion", "explosion_candidate", "nuclear_test_candidate")
EXPLOSION_TYPES = ("seismic_explosion", "explosion_candidate", "nuclear_test_candidate")
STRIKE_TYPES = ("air_raid", "drone_missile_track", "telegram_signal", "missile_alert", "kyiv_alert",
                "acled_conflict", "conflict_event")
CONFLICT_ZONES = [  # (nombre, lat_min, lat_max, lon_min, lon_max)
    ("Ucrania / frontera rusa", 44.0, 54.0, 22.0, 42.5),
    ("Israel / Gaza / Líbano", 29.0, 34.8, 33.5, 36.6),
]
TEST_SITES = [
    ("Punggye-ri (Corea del Norte)", 40.8, 41.8, 128.3, 129.8),
    ("Novaya Zemlya (Rusia)", 70.0, 77.5, 48.0, 62.0),
    ("Lop Nur (China)", 40.8, 42.5, 87.0, 89.5),
]


def zone_of(lat: float, lon: float, zones) -> Optional[str]:
    for name, la0, la1, lo0, lo1 in zones:
        if la0 <= lat <= la1 and lo0 <= lon <= lo1:
            return name
    return None


def build_seismic_alert(source: str, lat: float, lon: float, mag: float, depth: Optional[float],
                        place, when: str, link, evtype) -> dict:
    ev = str(evtype or "earthquake").lower()
    mag = float(mag or 0.0)
    shallow = depth is not None and depth <= 10
    zone = zone_of(lat, lon, CONFLICT_ZONES)
    site = zone_of(lat, lon, TEST_SITES)
    a_type = "earthquake"
    sev = "high" if mag >= 6 else ("medium" if mag >= 5 else "low")
    note = ""
    if "nuclear" in ev:
        a_type, sev, note = "seismic_explosion", "critical", " — catalogado como EXPLOSIÓN NUCLEAR"
    elif any(k in ev for k in ("explosion", "blast", "mining")):
        a_type, sev, note = "seismic_explosion", "high", " — catalogado como explosión"
    elif site and shallow and mag >= 3.0:
        a_type, sev, note = "nuclear_test_candidate", "high", f" — posible prueba nuclear en {site} (heurístico)"
    elif zone and shallow and 2.0 <= mag <= 5.5:
        a_type, sev = "explosion_candidate", "medium"
        note = f" — candidato a explosión en {zone}, prof. {depth:.0f} km (heurístico)"
    d_txt = f", prof. {depth:.0f} km" if (depth is not None and not note) else ""
    return {
        "source": source, "type": a_type, "region": str(place or "Global")[:120],
        "lat": lat, "lon": lon, "severity": sev,
        "description": f"Sismo M{mag:.1f} — {place or ''}{d_txt}{note}"[:220],
        "detected_at": when, "link": link,
        "raw": {"mag": mag, "depth": depth, "place": place, "evtype": ev, "url": link},
    }


def fetch_usgs() -> List[dict]:
    """USGS (sin key): sismos M2.5+ 24h. Conserva M4.5+, explosiones catalogadas y candidatos."""
    name = "USGS"
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
    try:
        resp = create_session().get(url, headers=get_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        for f in data.get("features", []):
            props = f.get("properties", {}) or {}
            coords = (f.get("geometry") or {}).get("coordinates") or []
            if len(coords) < 2:
                continue
            mag = to_float(props.get("mag"), 0.0) or 0.0
            depth = to_float(coords[2]) if len(coords) > 2 else None
            a = build_seismic_alert("USGS", float(coords[1]), float(coords[0]), mag, depth, props.get("place"),
                                    normalize_dt(props.get("time")), props.get("url"), props.get("type"))
            if a["type"] == "earthquake" and mag < 4.5:
                continue
            alerts.append(a)
        return _ok(name, alerts[:120])
    except Exception as e:
        return _fail(name, e)


def fetch_emsc() -> List[dict]:
    """EMSC / SeismicPortal (sin key): excelente cobertura de Europa y Mediterráneo, M2.0+."""
    name = "EMSC"
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {"format": "json", "limit": 300, "minmagnitude": 2.0, "orderby": "time", "starttime": since}
    try:
        resp = create_session().get("https://www.seismicportal.eu/fdsnws/event/1/query", params=params,
                                    headers=get_headers(), timeout=20)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        for f in data.get("features", []):
            p = f.get("properties", {}) or {}
            lat, lon = to_float(p.get("lat")), to_float(p.get("lon"))
            if lat is None or lon is None:
                co = (f.get("geometry") or {}).get("coordinates") or []
                if len(co) < 2:
                    continue
                lon, lat = float(co[0]), float(co[1])
            mag = to_float(p.get("mag"), 0.0) or 0.0
            depth = to_float(p.get("depth"))
            if depth is not None:
                depth = abs(depth)
            unid = p.get("unid") or f.get("id")
            link = f"https://www.seismicportal.eu/eventdetails.html?unid={unid}" if unid else None
            a = build_seismic_alert("EMSC", lat, lon, mag, depth, p.get("flynn_region"),
                                    normalize_dt(p.get("time")), link, p.get("evtype"))
            if a["type"] == "earthquake" and mag < 4.5:
                continue
            alerts.append(a)
        return _ok(name, alerts[:120])
    except Exception as e:
        return _fail(name, e)


def fetch_geofon() -> List[dict]:
    """GEOFON / GFZ Potsdam (sin key): catálogo FDSN en texto con tipo de evento."""
    name = "GEOFON"
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {"format": "text", "starttime": since, "minmagnitude": 2.5, "limit": 300, "orderby": "time"}
    try:
        resp = create_session().get("https://geofon.gfz-potsdam.de/fdsnws/event/1/query", params=params,
                                    headers=get_headers(), timeout=20)
        resp.raise_for_status()
        alerts = []
        for line in resp.text.splitlines():
            if not line or line.startswith("#"):
                continue
            c = line.split("|")
            if len(c) < 13:
                continue
            lat, lon = to_float(c[2]), to_float(c[3])
            if lat is None or lon is None:
                continue
            depth = to_float(c[4])
            mag = to_float(c[10], 0.0) or 0.0
            evtype = c[13] if len(c) > 13 else "earthquake"
            a = build_seismic_alert("GEOFON", lat, lon, mag, depth, c[12], normalize_dt(c[1]),
                                    f"https://geofon.gfz-potsdam.de/eqinfo/event.php?id={c[0]}", evtype)
            if a["type"] == "earthquake" and mag < 4.5:
                continue
            alerts.append(a)
        return _ok(name, alerts[:120])
    except Exception as e:
        return _fail(name, e)


def merge_seismic(alerts: List[dict]) -> List[dict]:
    """Une el mismo evento reportado por varias redes (±120 s, ≤100 km) conservando la clasificación más grave."""
    seismic = [a for a in alerts if a.get("type") in SEISMIC_TYPES]
    others = [a for a in alerts if a.get("type") not in SEISMIC_TYPES]
    rank = {"EMSC": 0, "USGS": 1, "GEOFON": 2}

    def r(a):
        return min([v for k, v in rank.items() if k in a.get("source", "")] or [9])

    seismic.sort(key=r)
    kept: List[dict] = []
    times: Dict[int, float] = {}
    for a in seismic:
        t = epoch_of(a["detected_at"])
        dup = None
        for k in kept:
            if abs(times[id(k)] - t) <= 120 and haversine_km(k["lat"], k["lon"], a["lat"], a["lon"]) <= 100:
                dup = k
                break
        if dup is None:
            kept.append(a)
            times[id(a)] = t
            continue
        dup["raw"].setdefault("also", []).append(a["source"])
        if (_LEVEL_ORDER.get(a["severity"], 0) > _LEVEL_ORDER.get(dup["severity"], 0)
                or (dup["type"] == "earthquake" and a["type"] != "earthquake")):
            for key in ("type", "severity", "description"):
                dup[key] = a[key]
    for k in kept:
        n = len(k["raw"].get("also", []))
        if n:
            k["description"] = (k["description"] + f" [{n + 1} redes sísmicas]")[:240]
    return others + kept


def correlate_explosions(alerts: List[dict]):
    """Marca candidatos a explosión que coinciden (≤150 km, ±30 min) con alertas/reportes de ataque."""
    cands = [a for a in alerts if a.get("type") in EXPLOSION_TYPES]
    if not cands:
        return
    strikes = [a for a in alerts if a.get("type") in STRIKE_TYPES and a.get("geo_known", True)
               and a.get("lat") is not None and a.get("lon") is not None]
    for c in cands:
        tc = epoch_of(c["detected_at"])
        n = 0
        for st in strikes:
            try:
                if (abs(epoch_of(st["detected_at"]) - tc) <= 1800
                        and haversine_km(c["lat"], c["lon"], float(st["lat"]), float(st["lon"])) <= 150):
                    n += 1
            except Exception:
                continue
        if n:
            if _LEVEL_ORDER.get(c["severity"], 0) < 2:
                c["severity"] = "high"
            c["description"] = (c["description"] + f" ⚡ correlaciona con {n} alerta(s)/reporte(s) de ataque (≤150 km, ±30 min)")[:300]
            c.setdefault("tags", []).append("correlado")


def fetch_eonet() -> List[dict]:
    """NASA EONET (sin key): incendios, volcanes, tormentas severas abiertas."""
    name = "NASA EONET"
    url = "https://eonet.gsfc.nasa.gov/api/v3/events"
    try:
        resp = create_session().get(url, params={"status": "open", "limit": 60, "days": 3},
                                    headers=get_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        alerts = []
        for ev in data.get("events", []):
            geoms = ev.get("geometry") or []
            if not geoms:
                continue
            g = geoms[-1]
            coords = g.get("coordinates")
            try:
                while isinstance(coords, list) and coords and isinstance(coords[0], list):
                    coords = coords[0]
                lon, lat = float(coords[0]), float(coords[1])
            except Exception:
                continue
            cats = ev.get("categories") or [{}]
            cat = cats[0].get("title", "Evento natural")
            alerts.append({
                "source": "NASA EONET", "type": "natural_event",
                "region": str(ev.get("title", ""))[:120], "lat": lat, "lon": lon,
                "severity": "medium" if "volcan" in cat.lower() else "low",
                "description": f"{cat}: {ev.get('title', '')}"[:150],
                "detected_at": normalize_dt(g.get("date")),
                "link": (ev.get("sources") or [{}])[0].get("url") or ev.get("link"),
                "raw": {"id": ev.get("id"), "category": cat}
            })
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ------------------------------------------------------------
# RSS de noticias (filtradas por palabras clave de ataque)
# ------------------------------------------------------------
RSS_FEEDS: List[Tuple[str, str]] = [
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Times of Israel", "https://www.timesofisrael.com/feed/"),
]
for _extra in os.getenv("EXTRA_RSS_FEEDS", "").split(","):
    if "|" in _extra:
        _n, _u = _extra.split("|", 1)
        if _n.strip() and _u.strip():
            RSS_FEEDS.append((_n.strip(), _u.strip()))

RSS_KEYWORDS = ["missile", "drone", "airstrike", "air strike", "explosion", "shelling", "rocket",
                "intercept", "sirens", "ballistic", "launch", "bombard", "attack"]


def _fetch_one_rss(feed: Tuple[str, str]) -> List[dict]:
    fname, furl = feed
    out = []
    now = datetime.now(timezone.utc)
    resp = create_session().get(furl, headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = re.sub(r"<[^>]+>", " ", item.findtext("description") or "")
        blob = f"{title} {desc}".lower()
        if not any(k in blob for k in RSS_KEYWORDS):
            continue
        try:
            dt = parsedate_to_datetime(item.findtext("pubDate") or "")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = now
        if (now - dt).total_seconds() > 6 * 3600:
            continue
        loc = geolocate_text(f"{title} {desc}")
        out.append({
            "source": f"RSS — {fname}", "type": "news",
            "region": loc[2] if loc else fname,
            "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
            "severity": "low",
            "description": title[:200],
            "detected_at": dt.astimezone(timezone.utc).isoformat(),
            "link": link, "raw": {"feed": fname, "title": title}
        })
        if len(out) >= 12:
            break
    return out


def fetch_rss_news() -> List[dict]:
    name = "RSS Noticias"
    try:
        results = run_parallel(tracked("Noticias RSS", _fetch_one_rss), RSS_FEEDS, workers=4)
        alerts, failed = [], []
        for feed, res, err in results:
            if err:
                failed.append(feed[0])
                logger.warning(f"[RSS/{feed[0]}] {err}")
            else:
                alerts.extend(res)
        if failed and len(failed) == len(RSS_FEEDS):
            return _fail(name, f"todos los feeds fallaron ({', '.join(failed)})")
        if failed:
            return _warn(name, alerts, f"{len(failed)}/{len(RSS_FEEDS)} feeds sin respuesta: {', '.join(failed)}")
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ============================================================
# FUENTES DE GUERRA (prensa/OSINT/organismos) Y COMUNICADOS OFICIALES
# ============================================================
def _ok_note(name: str, alerts: List[dict], note: str) -> List[dict]:
    logger.info(f"[{name}] {len(alerts)} ({note})")
    _set_status(name, "ok", len(alerts), note)
    return alerts


def _parse_dt_any(v) -> Optional[datetime]:
    if not v:
        return None
    v = str(v).strip()
    try:
        dt = parsedate_to_datetime(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_feed_items(content: bytes) -> List[dict]:
    """Parsea RSS 2.0 y Atom -> [{title, link, desc, dt}]."""
    root = ET.fromstring(content)
    out = []

    def ltag(el):
        return el.tag.split("}")[-1] if isinstance(el.tag, str) else ""

    def child_text(el, *names):
        for c in el:
            if ltag(c) in names and (c.text or "").strip():
                return c.text.strip()
        return ""

    for el in root.iter():
        t = ltag(el)
        if t == "item":
            title, link = child_text(el, "title"), child_text(el, "link")
            desc, when = child_text(el, "description", "encoded"), child_text(el, "pubDate", "date")
        elif t == "entry":
            title, link = child_text(el, "title"), ""
            for c in el:
                if ltag(c) == "link" and c.get("href") and c.get("rel", "alternate") == "alternate":
                    link = c.get("href")
                    break
            desc, when = child_text(el, "summary", "content"), child_text(el, "updated", "published")
        else:
            continue
        desc = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", desc))).strip()
        out.append({"title": unescape(title), "link": link, "desc": desc, "dt": _parse_dt_any(when)})
    return out


def _env_feeds(var: str) -> List[Tuple[str, str, str]]:
    out = []
    for chunk in os.getenv(var, "").split(","):
        parts = [p.strip() for p in chunk.split("|")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            tier = parts[2].upper() if len(parts) > 2 and parts[2].upper() in ("A", "B", "C", "D") else "B"
            out.append((parts[0], parts[1], tier))
    return out


# (nombre, url, credibilidad, filtrar_por_palabras_de_guerra)
WAR_FEEDS: List[Tuple[str, str, str, bool]] = [
    ("ISW (análisis)", "https://www.understandingwar.org/rss.xml", "B", False),
    ("Bellingcat (OSINT)", "https://www.bellingcat.com/feed/", "B", True),
    ("War on the Rocks", "https://warontherocks.com/feed/", "B", True),
    ("Breaking Defense", "https://breakingdefense.com/feed/", "B", True),
    ("Oryx (pérdidas verificadas)", "https://www.oryxspioenkop.com/feeds/posts/default", "B", False),
    ("Kyiv Independent", "https://kyivindependent.com/news-archive/rss/", "B", False),
    ("Ukrainska Pravda EN", "https://www.pravda.com.ua/eng/rss/view_news/", "B", False),
    ("Euromaidan Press", "https://euromaidanpress.com/feed/", "B", True),
    ("Meduza EN", "https://meduza.io/rss/en/all", "B", True),
    ("The Moscow Times", "https://www.themoscowtimes.com/rss/news", "B", True),
    ("Critical Threats", "https://www.criticalthreats.org/feed", "B", True),
    ("ONU Noticias — Paz y seguridad", "https://news.un.org/feed/subscribe/en/news/topic/peace-and-security/feed/rss.xml", "A", False),
    ("Long War Journal", "https://www.longwarjournal.org/feed", "B", False),
    ("The War Zone", "https://www.twz.com/feed", "B", True),
    ("Defense One", "https://www.defenseone.com/rss/all/", "B", True),
    ("Naval News", "https://www.navalnews.com/feed/", "B", True),
    ("Kyiv Post", "https://www.kyivpost.com/feed", "B", True),
    ("Al-Monitor", "https://www.al-monitor.com/rss", "B", True),
    ("Atlantic Council UkraineAlert", "https://www.atlanticcouncil.org/category/blogs/ukrainealert/feed/", "B", False),
]
WAR_FEEDS += [(n, u, c, False) for n, u, c in _env_feeds("EXTRA_WAR_FEEDS")]
WAR_KEYWORDS = ["war", "ukrain", "russia", "missile", "drone", "strike", "attack", "front", "offensive", "troops",
                "military", "nato", "gaza", "israel", "hamas", "hezbollah", "iran", "houthi", "ceasefire", "nuclear",
                "sanction", "mobiliz", "shell", "kremlin", "artillery", "invasion", "airstrike", "killed", "defen"]


def _one_war_feed(feed) -> List[dict]:
    fname, furl, cred, use_filter = feed
    out = []
    now = datetime.now(timezone.utc)
    resp = create_session().get(furl, headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    for it in parse_feed_items(resp.content):
        if not it["dt"] or (now - it["dt"]).total_seconds() > WAR_MAX_AGE_H * 3600:
            continue
        blob = f"{it['title']} {it['desc']}"
        if use_filter and not any(k in blob.lower() for k in WAR_KEYWORDS):
            continue
        lvl, tags = classify_statement(blob)
        sev = "high" if _LEVEL_ORDER[lvl] >= 2 else "low"  # la prensa nunca dispara la alarma crítica
        loc = geolocate_text(blob)
        out.append({
            "source": f"Guerra — {fname}", "type": "war_news",
            "region": loc[2] if loc else fname,
            "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
            "severity": sev, "description": it["title"][:220],
            "detected_at": it["dt"].isoformat(), "link": it["link"], "cred": cred, "tags": tags,
            "raw": {"feed": fname, "title": it["title"][:200]},
        })
        if len(out) >= 10:
            break
    return out


def fetch_war_news() -> List[dict]:
    name = "Guerra (prensa/OSINT)"
    try:
        results = run_parallel(tracked("Prensa de guerra", _one_war_feed), WAR_FEEDS, workers=6)
        alerts, failed = [], []
        for feed, res, err in results:
            if err:
                failed.append(feed[0])
                logger.info(f"[Guerra/{feed[0]}] {err}")
            else:
                alerts.extend(res)
        if failed and len(failed) == len(WAR_FEEDS):
            return _fail(name, f"todos los feeds fallaron ({', '.join(failed)[:150]})")
        if failed:
            return _ok_note(name, alerts, f"{len(failed)}/{len(WAR_FEEDS)} feeds sin respuesta: {', '.join(failed)}"[:200])
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# (nombre, url, credibilidad, modo)  modo: "travel" (avisos de viaje) | "gov" (comunicados de gobierno/organismo)
STATEMENT_FEEDS: List[Tuple[str, str, str, str]] = [
    ("EE.UU. Dpto. Estado (viajes)", "https://travel.state.gov/_res/rss/TAsTWs.xml", "A", "travel"),
    ("Reino Unido FCDO (viajes)", "https://www.gov.uk/foreign-travel-advice.atom", "A", "travel"),
    ("EE.UU. Dpto. Estado (prensa)", "https://www.state.gov/rss-feed/press-releases/feed/", "C", "gov"),
    ("Reino Unido FCDO (noticias)", "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office.atom", "C", "gov"),
    ("Reino Unido MoD", "https://www.gov.uk/government/organisations/ministry-of-defence.atom", "C", "gov"),
    ("Kremlin (presidente)", "http://en.kremlin.ru/events/president/news/feed", "C", "gov"),
    ("OTAN (NATO)", "https://www.nato.int/cps/en/natohq/news.xml", "C", "gov"),
    ("OIEA (IAEA)", "https://www.iaea.org/feeds/topnews", "A", "gov"),
    ("ONU Noticias", "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "A", "gov"),
    ("EE.UU. CENTCOM (prensa)", "https://www.centcom.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=808&max=10", "C", "gov"),
    ("EE.UU. Pentágono (noticias)", "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=945&max=10", "C", "gov"),
    ("EE.UU. Pentágono (comunicados)", "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=9&Site=945&max=10", "C", "gov"),
    ("EE.UU. Casa Blanca (prensa)", "https://www.whitehouse.gov/news/feed/", "C", "gov"),
    ("UE Acción Exterior (prensa)", "https://www.eeas.europa.eu/rss.xml", "C", "gov"),
    ("UE Consejo (prensa)", "https://www.consilium.europa.eu/en/rss/pressreleases.ashx", "C", "gov"),
]
STATEMENT_FEEDS += [(n, u, c, "gov") for n, u, c in _env_feeds("EXTRA_STATEMENT_FEEDS")]


def _one_statement_feed(feed) -> List[dict]:
    fname, furl, cred, mode = feed
    out = []
    now = datetime.now(timezone.utc)
    max_age = (7 * 24 if mode == "travel" else STATEMENT_MAX_AGE_H) * 3600
    resp = create_session().get(furl, headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    for it in parse_feed_items(resp.content):
        dt = it["dt"]
        if not dt or (now - dt).total_seconds() > max_age:
            continue
        title = it["title"]
        text = f"{title}. {it['desc']}"[:700]
        lvl, tags = classify_statement(text)
        if mode == "travel":
            m = re.search(r"level\s*(\d)", title.lower())
            if m:
                n = int(m.group(1))
                if n >= 4 and _LEVEL_ORDER[lvl] < 2:
                    lvl = "high"
                    if "viaje" not in tags:
                        tags.append("viaje")
                elif n == 3 and _LEVEL_ORDER[lvl] < 1:
                    lvl = "medium"
                    if "viaje" not in tags:
                        tags.append("viaje")
        if _LEVEL_ORDER[lvl] < 1:
            continue
        tags.insert(0, "oficial")
        loc = geolocate_text(text)
        out.append({
            "source": f"Comunicado — {fname}", "type": "official_statement",
            "region": loc[2] if loc else fname,
            "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
            "severity": lvl, "description": title[:240],
            "detected_at": dt.isoformat(), "link": it["link"], "cred": cred, "tags": tags,
            "raw": {"feed": fname, "title": title[:200], "mode": mode},
        })
        if len(out) >= 15:
            break
    return out


def fetch_statements() -> List[dict]:
    """Comunicados especiales de gobiernos y organismos (avisos de viaje, movilización, ley marcial,
    cierre de espacio aéreo, ultimátums, declaraciones nucleares, evacuaciones, etc.)."""
    name = "Comunicados oficiales"
    try:
        results = run_parallel(tracked("Comunicados oficiales", _one_statement_feed), STATEMENT_FEEDS, workers=6)
        alerts, failed = [], []
        for feed, res, err in results:
            if err:
                failed.append(feed[0])
                logger.info(f"[Comunicados/{feed[0]}] {err}")
            else:
                alerts.extend(res)
        if failed and len(failed) == len(STATEMENT_FEEDS):
            return _fail(name, f"todos los feeds fallaron ({', '.join(failed)[:150]})")
        if failed:
            return _ok_note(name, alerts, f"{len(failed)}/{len(STATEMENT_FEEDS)} feeds sin respuesta: {', '.join(failed)}"[:200])
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# Medios estatales: cuentan la versión de cada gobierno, NO lo que ha pasado (etiqueta "estatal", credibilidad C).
STATE_MEDIA_FEEDS: List[Tuple[str, str]] = [
    ("TASS (Rusia)", "https://tass.com/rss/v2.xml"),
    ("RT (Rusia)", "https://www.rt.com/rss/news/"),
    ("Sputnik (Rusia)", "https://sputnikglobe.com/export/rss2/archive/index.xml"),
    ("IRNA (Irán)", "https://en.irna.ir/rss"),
    ("Tasnim (Irán)", "https://www.tasnimnews.com/en/rss/feed/0/8/0/all-stories"),
    ("Press TV (Irán)", "https://www.presstv.ir/rss.xml"),
    ("Xinhua (China)", "http://www.xinhuanet.com/english/rss/worldrss.xml"),
    ("CGTN (China)", "https://www.cgtn.com/subscribe/rss/section/world.xml"),
    ("WAFA (Palestina)", "https://english.wafa.ps/rss.aspx"),
    ("SANA (Siria)", "https://sana.sy/en/?feed=rss2"),
    ("Anadolu (Turquía)", "https://www.aa.com.tr/en/rss/default?cat=world"),
    ("Ukrinform (Ucrania)", "https://www.ukrinform.net/rss/block-lastnews"),
]
STATE_MEDIA_FEEDS += [(n, u) for n, u, _c in _env_feeds("EXTRA_STATE_FEEDS")]


def _one_state_feed(feed) -> List[dict]:
    fname, furl = feed
    out = []
    now = datetime.now(timezone.utc)
    resp = create_session().get(furl, headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    for it in parse_feed_items(resp.content):
        if not it["dt"] or (now - it["dt"]).total_seconds() > WAR_MAX_AGE_H * 3600:
            continue
        blob = f"{it['title']} {it['desc']}"
        if not any(k in blob.lower() for k in WAR_KEYWORDS):
            continue
        lvl, tags = classify_statement(blob)
        loc = geolocate_text(blob)
        out.append({
            "source": f"Estatal — {fname}", "type": "state_media",
            "region": loc[2] if loc else fname,
            "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
            "severity": "medium" if _LEVEL_ORDER[lvl] >= 2 else "low",  # nunca escala por sí solo
            "description": it["title"][:220],
            "detected_at": it["dt"].isoformat(), "link": it["link"], "cred": "C", "tags": ["estatal"] + tags,
            "raw": {"feed": fname, "title": it["title"][:200]},
        })
        if len(out) >= 8:
            break
    return out


def fetch_state_media() -> List[dict]:
    name = "Medios estatales"
    try:
        results = run_parallel(tracked("Medios estatales", _one_state_feed), STATE_MEDIA_FEEDS, workers=6)
        alerts, failed = [], []
        for feed, res, err in results:
            if err:
                failed.append(feed[0])
                logger.info(f"[Estatales/{feed[0]}] {err}")
            else:
                alerts.extend(res)
        if failed and len(failed) == len(STATE_MEDIA_FEEDS):
            return _fail(name, f"todos los feeds fallaron ({', '.join(failed)[:150]})")
        if failed:
            return _ok_note(name, alerts, f"{len(failed)}/{len(STATE_MEDIA_FEEDS)} feeds sin respuesta: {', '.join(failed)}"[:200])
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ============================================================
# TELEGRAM — CANALES MONITORIZADOS
# ============================================================
TELEGRAM_CHANNELS = [
    {"id": "rybar", "name": "Rybar (RU)", "lang": "ru"},
    {"id": "readovkanews", "name": "Readovka (RU)", "lang": "ru"},
    {"id": "wargonzo", "name": "WarGonzo (RU)", "lang": "ru"},
    {"id": "DeepStateUA", "name": "DeepState UA", "lang": "uk"},
    {"id": "kpszsu", "name": "UA Air Force", "lang": "uk"},
    # Añadidos en v2.2 (puedes quitarlos desde la pestaña BUSCAR)
    {"id": "air_alert_ua", "name": "Повітряна тривога (UA)", "lang": "uk"},
    {"id": "operativnoZSU", "name": "Оперативний ЗСУ (UA)", "lang": "uk"},
    {"id": "mod_russia", "name": "Минобороны РФ (RU)", "lang": "ru"},
    {"id": "boris_rozhin", "name": "Colonelcassad (RU)", "lang": "ru"},
    {"id": "militarysummary", "name": "Military Summary (EN)", "lang": "en"},
    {"id": "intelslava", "name": "Intel Slava (EN)", "lang": "en"},
    # v2.3: comunicados oficiales (afirmaciones de cada parte; contrastar)
    {"id": "GeneralStaffZSU", "name": "Estado Mayor UA (oficial)", "lang": "uk"},
    {"id": "ukrainenowenglish", "name": "Ukraine NOW (gobierno UA)", "lang": "en"},
    {"id": "idfofficial", "name": "IDF (oficial)", "lang": "en"},
    {"id": "SBUkr", "name": "SBU (oficial UA)", "lang": "uk"},
    {"id": "MID_Russia", "name": "MAE Rusia (oficial)", "lang": "ru"},
]

TG_KEYWORDS = ["ракета", "ракеты", "БПЛА", "дрон", "взрыв", "пуск", "обстрел",
               "missile", "drone", "explosion", "launch", "strike",
               "ракетна", "безпілотник", "вибух",
               # v2.2
               "авиаудар", "ракетн", "шахед", "сбит", "пво", "iskander", "искандер", "калибр",
               "kalibr", "shahed", "airstrike", "air strike", "shelling", "ballistic", "балістич",
               "тривога", "тревога", "обстріл", "приліт", "прилёт"]
TG_STRONG = ["ballistic", "балістич", "баллистич", "launch", "пуск", "взрыв", "вибух", "explosion", "искандер", "iskander"]

TG_CRED = {"deepstateua": "B", "kpszsu": "C", "mod_russia": "C", "generalstaffzsu": "C", "idfofficial": "C",
           "ukrainenowenglish": "C", "sbukr": "C", "mid_russia": "C"}  # el resto de canales = "D" (no verificado)
_tg_lock = threading.Lock()
_TG_USER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
TG_RESERVED = {"joinchat", "share", "addstickers", "proxy", "socks", "setlanguage", "login", "iv", "s", "c",
               "addlist", "boost", "contact", "addemoji", "giftcode", "invoice", "telegram", "username"}


def clean_username(value) -> Optional[str]:
    """Extrae un @usuario válido desde @user, user, t.me/user, https://t.me/s/user..."""
    if not value:
        return None
    s = str(value).strip()
    s = re.sub(r"^(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog)/", "", s, flags=re.I)
    s = s.lstrip("@")
    if s.lower().startswith("s/"):
        s = s[2:]
    s = re.split(r"[/?#\s]", s)[0]
    if not s or s.lower() in TG_RESERVED:
        return None
    return s if _TG_USER_RE.match(s) else None


def username_from_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
    except Exception:
        return None
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host not in ("t.me", "telegram.me", "telegram.dog"):
        return None
    parts = [x for x in p.path.split("/") if x]
    if not parts:
        return None
    cand = parts[1] if (parts[0].lower() == "s" and len(parts) > 1) else parts[0]
    return clean_username(cand)


def _tg_read_cfg() -> Dict[str, Any]:
    try:
        with open(TG_CHANNELS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    custom = d.get("custom", []) if isinstance(d, dict) else []
    disabled = d.get("disabled", []) if isinstance(d, dict) else []
    return {"custom": custom if isinstance(custom, list) else [], "disabled": disabled if isinstance(disabled, list) else []}


def _tg_write_cfg(cfg: Dict[str, Any]):
    tmp = TG_CHANNELS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, TG_CHANNELS_FILE)


def get_telegram_channels(include_disabled: bool = False) -> List[dict]:
    with _tg_lock:
        cfg = _tg_read_cfg()
    disabled = {str(x).lower() for x in cfg["disabled"]}
    out, seen = [], set()
    for ch in TELEGRAM_CHANNELS:
        item = dict(ch)
        item.update({"custom": False, "keywords_only": True, "enabled": ch["id"].lower() not in disabled})
        out.append(item)
        seen.add(ch["id"].lower())
    for ch in cfg["custom"]:
        if not isinstance(ch, dict) or not clean_username(ch.get("id")):
            continue
        if ch["id"].lower() in seen:
            continue
        item = {"id": ch["id"], "name": ch.get("name") or ch["id"], "lang": ch.get("lang", "xx"),
                "custom": True, "keywords_only": bool(ch.get("keywords_only", True)), "enabled": True}
        out.append(item)
        seen.add(ch["id"].lower())
    return out if include_disabled else [c for c in out if c["enabled"]]


def add_telegram_channel(username: str, name: str, lang: str = "xx", keywords_only: bool = True):
    with _tg_lock:
        cfg = _tg_read_cfg()
        low = username.lower()
        cfg["disabled"] = [d for d in cfg["disabled"] if str(d).lower() != low]
        if low not in {c["id"].lower() for c in TELEGRAM_CHANNELS}:
            cfg["custom"] = [c for c in cfg["custom"] if str(c.get("id", "")).lower() != low]
            cfg["custom"].append({"id": username, "name": name or username, "lang": lang or "xx",
                                  "keywords_only": bool(keywords_only)})
        _tg_write_cfg(cfg)


def remove_telegram_channel(username: str) -> bool:
    with _tg_lock:
        cfg = _tg_read_cfg()
        low = username.lower()
        if low in {c["id"].lower() for c in TELEGRAM_CHANNELS}:
            if low not in {str(d).lower() for d in cfg["disabled"]}:
                cfg["disabled"].append(username)
            _tg_write_cfg(cfg)
            return True
        before = len(cfg["custom"])
        cfg["custom"] = [c for c in cfg["custom"] if str(c.get("id", "")).lower() != low]
        _tg_write_cfg(cfg)
        return len(cfg["custom"]) != before


def parse_tg_html(html: str) -> List[dict]:
    """Parsea t.me/s/<canal> -> lista de mensajes en orden cronológico."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for w in soup.select(".tgme_widget_message"):
        txt_el = w.select_one(".tgme_widget_message_text")
        if not txt_el:
            continue
        text = txt_el.get_text(separator=" ", strip=True)
        if not text:
            continue
        time_el = w.select_one(".tgme_widget_message_date time") or w.select_one("time")
        dt = time_el.get("datetime") if time_el and time_el.get("datetime") else None
        post = w.get("data-post") or ""
        views_el = w.select_one(".tgme_widget_message_views")
        out.append({
            "text": text,
            "datetime": normalize_dt(dt) if dt else now_iso(),
            "link": f"https://t.me/{post}" if post else "",
            "views": views_el.get_text(strip=True) if views_el else "",
        })
    return out


def fetch_telegram_channel(ch: dict) -> List[dict]:
    url = f"https://t.me/s/{ch['id']}"
    resp = create_session().get(url, headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    msgs = parse_tg_html(resp.text)[-TG_MESSAGES_PER_CHANNEL:]
    keywords_only = ch.get("keywords_only", True)
    min_len = 25 if keywords_only else 10
    out = []
    for m in msgs:
        text = m["text"]
        low = text.lower()
        if len(text) < min_len:
            continue
        if keywords_only and not any(kw.lower() in low for kw in TG_KEYWORDS):
            continue
        if TG_MAX_AGE_HOURS > 0 and age_seconds(m["datetime"]) > TG_MAX_AGE_HOURS * 3600:
            continue
        loc = geolocate_text(text)
        if loc:
            lat, lon, approx = loc[0], loc[1], True
        else:
            lat, lon, approx = (48.0 if ch.get("lang") == "uk" else 55.0), 37.0, True
        sev = "high" if any(s in low for s in TG_STRONG) else "medium"
        lvl, st_tags = classify_statement(text)
        if _LEVEL_ORDER.get(lvl, 0) >= 2:
            sev = "high"
        out.append({
            "source": f"Telegram — {ch['name']}", "type": "telegram_signal",
            "region": ch["name"],
            "lat": lat, "lon": lon, "geo_approx": approx,
            "severity": sev,
            "description": text[:200],
            "detected_at": m["datetime"], "link": m["link"],
            "cred": TG_CRED.get(ch["id"].lower(), "D"), "tags": ["telegram", "sin-verificar"] + st_tags, "geo_known": bool(loc),
            "raw": {"channel": ch["id"], "text": text[:500], "views": m["views"]}
        })
    return out


def fetch_telegram() -> List[dict]:
    name = "Telegram"
    try:
        channels = get_telegram_channels()
        if not channels:
            return _skip(name, "Sin canales activos")
        results = run_parallel(tracked("Telegram", fetch_telegram_channel), channels, workers=6)
        alerts, failed = [], []
        for ch, res, err in results:
            if err:
                failed.append(ch["id"])
                logger.warning(f"[Telegram/{ch['id']}] {err}")
            else:
                alerts.extend(res)
        if failed and len(failed) == len(channels):
            return _fail(name, f"todos los canales fallaron ({', '.join(failed)})")
        if failed:
            return _warn(name, alerts, f"{len(failed)}/{len(channels)} canales sin respuesta: {', '.join(failed)}")
        return _ok(name, alerts)
    except Exception as e:
        return _fail(name, e)


# ============================================================
# TELEGRAM — BUSCADOR DE CANALES / GRUPOS
# ============================================================
def tg_channel_info(username: str) -> Dict[str, Any]:
    u = clean_username(username)
    if not u:
        return {"exists": False, "username": username}
    resp = create_session().get(f"https://t.me/{u}", headers=get_headers(html=True), timeout=12)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    title_el = soup.select_one(".tgme_page_title")
    desc_el = soup.select_one(".tgme_page_description")
    extra_el = soup.select_one(".tgme_page_extra")
    title = title_el.get_text(" ", strip=True) if title_el else ""
    extra = extra_el.get_text(" ", strip=True) if extra_el else ""
    low = extra.lower()
    if any(k in low for k in ("subscriber", "подписчик", "підписник")):
        kind = "channel"
    elif any(k in low for k in ("member", "участник", "учасник")):
        kind = "group"
    elif extra.startswith("@") or u.lower().endswith("bot"):
        kind = "user/bot"
    else:
        kind = "unknown"
    return {
        "exists": bool(title) and title.lower() != "telegram",
        "username": u, "title": title,
        "description": desc_el.get_text(" ", strip=True)[:300] if desc_el else "",
        "extra": extra, "kind": kind, "url": f"https://t.me/{u}",
    }


def tg_preview(username: str, q: str = "", limit: int = 25) -> Dict[str, Any]:
    u = clean_username(username)
    if not u:
        raise ValueError("Usuario de Telegram no válido")
    params = {"q": q} if q else None
    resp = create_session().get(f"https://t.me/s/{u}", params=params, headers=get_headers(html=True), timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    title_el = soup.select_one(".tgme_channel_info_header_title")
    desc_el = soup.select_one(".tgme_channel_info_description")
    counters = {}
    for c in soup.select(".tgme_channel_info_counter"):
        v = c.select_one(".counter_value")
        t = c.select_one(".counter_type")
        if v and t:
            counters[t.get_text(strip=True)] = v.get_text(strip=True)
    msgs = parse_tg_html(resp.text)
    msgs.reverse()
    title = title_el.get_text(" ", strip=True) if title_el else ""
    desc = desc_el.get_text(" ", strip=True)[:400] if desc_el else ""
    note = ""
    if not msgs and not title:
        info = tg_channel_info(u)
        title = info.get("title", "")
        desc = desc or info.get("description", "")
        note = "Sin vista previa pública (grupo privado, canal sin vista web o usuario inexistente)."
    elif not msgs:
        note = "Sin mensajes" + (" para esa búsqueda." if q else " visibles.")
    return {"username": u, "title": title or u, "description": desc, "counters": counters,
            "messages": msgs[:limit], "url": f"https://t.me/{u}", "note": note}


def _search_ddg(query: str) -> List[dict]:
    r = create_session().get("https://html.duckduckgo.com/html/", params={"q": query},
                             headers=get_headers(html=True), timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for res in soup.select(".result"):
        a = res.select_one("a.result__a")
        if not a:
            continue
        href = a.get("href", "")
        if "uddg=" in href:
            full = href if href.startswith("http") else "https:" + href
            href = unquote(parse_qs(urlparse(full).query).get("uddg", [href])[0])
        sn = res.select_one(".result__snippet")
        out.append({"url": href, "title": a.get_text(" ", strip=True),
                    "snippet": sn.get_text(" ", strip=True) if sn else ""})
    return out


def _decode_bing_url(href: str) -> str:
    if "bing.com/ck/a" in href:
        try:
            u = parse_qs(urlparse(href).query).get("u", [""])[0]
            if u.startswith("a1"):
                u = u[2:]
            u += "=" * (-len(u) % 4)
            return base64.urlsafe_b64decode(u).decode("utf-8", "ignore")
        except Exception:
            return href
    return href


def _search_bing(query: str) -> List[dict]:
    r = create_session().get("https://www.bing.com/search", params={"q": query},
                             headers=get_headers(html=True), timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for li in soup.select("li.b_algo"):
        a = li.select_one("h2 a")
        if not a:
            continue
        p = li.select_one(".b_caption p")
        out.append({"url": _decode_bing_url(a.get("href", "")), "title": a.get_text(" ", strip=True),
                    "snippet": p.get_text(" ", strip=True) if p else ""})
    return out


def tg_search_telethon(query: str, limit: int = 20) -> Optional[List[dict]]:
    """Búsqueda nativa de Telegram (canales y grupos públicos). None = sesión no iniciada."""
    if not (TELETHON_OK and TG_API_ID and TG_API_HASH):
        return []
    import asyncio
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import SearchRequest

    async def _run():
        client = TelegramClient(TG_SESSION, int(TG_API_ID), TG_API_HASH)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                return None
            res = await client(SearchRequest(q=query, limit=limit))
            out = []
            for chat in res.chats:
                uname = getattr(chat, "username", None)
                if not uname:
                    continue
                out.append({
                    "username": uname, "title": getattr(chat, "title", "") or uname,
                    "kind": "channel" if getattr(chat, "broadcast", False) else "group",
                    "members": getattr(chat, "participants_count", None),
                })
            return out
        finally:
            await client.disconnect()

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run())
    finally:
        loop.close()


def tg_login_cli():
    """python este_archivo.py --tg-login  -> inicia sesión de Telethon una sola vez."""
    if not (TELETHON_OK and TG_API_ID and TG_API_HASH):
        print("Necesitas: pip install telethon  y variables TG_API_ID / TG_API_HASH (my.telegram.org)")
        return
    from telethon.sync import TelegramClient
    with TelegramClient(TG_SESSION, int(TG_API_ID), TG_API_HASH) as client:
        me = client.get_me()
        print(f"Sesión Telethon lista: {getattr(me, 'first_name', '')}")


_tg_search_lock = threading.Lock()


def tg_search(query: str, limit: int = 30) -> Dict[str, Any]:
    q = query.strip()
    results: Dict[str, Dict[str, Any]] = {}
    notes: List[str] = []

    def add(username, title="", snippet="", via="web", extra=None):
        if not username:
            return
        key = username.lower()
        r = results.setdefault(key, {"username": username, "title": "", "snippet": "", "kind": "unknown",
                                     "members": None, "url": f"https://t.me/{username}", "via": [], "score": 0})
        if title and not r["title"]:
            r["title"] = title
        if snippet and len(snippet) > len(r["snippet"]):
            r["snippet"] = snippet[:240]
        if via not in r["via"]:
            r["via"].append(via)
        r["score"] += 1
        if extra:
            r.update({k: v for k, v in extra.items() if v})

    def clean_title(t: str) -> str:
        t = re.sub(r"\s*[–\-|]\s*Telegram.*$", "", t or "").strip()
        if re.match(r"^Telegram:\s*(Contact|View|Join)", t, re.I):
            return ""
        return t

    # 1) Búsqueda directa si parece @usuario / enlace t.me / nombre exacto
    direct = clean_username(q)
    if direct and (q.startswith("@") or "t.me" in q.lower() or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", q)):
        try:
            info = tg_channel_info(direct)
            if info.get("exists"):
                add(direct, info.get("title", ""), info.get("description", ""), "directo",
                    {"kind": info.get("kind"), "extra_info": info.get("extra")})
                results[direct.lower()]["score"] += 100
        except Exception as e:
            notes.append(f"Directo: {e}")

    # 2) Telethon (opcional)
    if TELETHON_OK and TG_API_ID and TG_API_HASH:
        try:
            with _tg_search_lock:
                tl = tg_search_telethon(q)
            if tl is None:
                notes.append("Telethon: sesión no iniciada (ejecuta el script con --tg-login)")
            else:
                for r in tl:
                    add(r["username"], r.get("title", ""), "", "telegram",
                        {"kind": r.get("kind"), "members": r.get("members")})
                    results[r["username"].lower()]["score"] += 50
        except Exception as e:
            notes.append(f"Telethon: {e}")

    # 3) Motores web públicos (sin API)
    jobs = [("ddg", f"site:t.me {q}"), ("ddg", f"{q} telegram канал t.me"), ("bing", f"site:t.me {q}")]

    def job(j):
        eng, qq = j
        return (_search_ddg if eng == "ddg" else _search_bing)(qq)

    engine_errors = []
    for j, items, err in run_parallel(job, jobs, workers=3):
        if err:
            engine_errors.append(f"{j[0]}: {str(err)[:80]}")
            continue
        for it in items:
            url, title, snip = it.get("url", ""), it.get("title", ""), it.get("snippet", "")
            u = username_from_url(url)
            if u:
                add(u, clean_title(title), snip, j[0])
            m = re.search(r"/(?:channel|chat)/@([A-Za-z][A-Za-z0-9_]{3,31})", url)  # directorios tipo tgstat
            if m and clean_username(m.group(1)):
                add(m.group(1), clean_title(title), snip, "directorio")
            for mm in re.findall(r"(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z][A-Za-z0-9_]{3,31})", f"{title} {snip}"):
                cu = clean_username(mm)
                if cu:
                    add(cu, "", snip, j[0])
    if engine_errors and len(engine_errors) == len(jobs):
        notes.append("Motores web no disponibles o bloqueados (" + "; ".join(engine_errors) + ")")
    if not results and not notes:
        notes.append("Sin resultados. Prueba con otro término, un @usuario o un enlace t.me.")
    if not results and not (TELETHON_OK and TG_API_ID and TG_API_HASH):
        notes.append("Para búsqueda nativa en Telegram configura Telethon (TG_API_ID/TG_API_HASH).")

    out_list = sorted(results.values(), key=lambda r: (-r["score"], not r["title"]))[:limit]
    for r in out_list:
        if not r["title"]:
            r["title"] = "@" + r["username"]
    return {"query": q, "results": out_list, "notes": notes,
            "telethon": bool(TELETHON_OK and TG_API_ID and TG_API_HASH)}


# ============================================================
# AGREGADOR
# ============================================================
SOURCES = [
    ("Tzeva Adom", fetch_tzeva_adom),
    ("Oref (oficial)", fetch_oref),
    ("alerts.com.ua", fetch_alerts_ua),
    ("Ubilling UA", fetch_ubilling),
    ("Alerts.in.ua", fetch_alerts_in_ua),
    ("Kyiv Digital", fetch_kyiv_digital),
    ("Neptun", fetch_neptun),
    ("NASA FIRMS", fetch_nasa_firms),
    ("GDELT", fetch_gdelt),
    ("GDELT GEO", fetch_gdelt_geo),
    ("World Monitor", fetch_world_monitor),
    ("USGS", fetch_usgs),
    ("EMSC", fetch_emsc),
    ("GEOFON", fetch_geofon),
    ("NASA EONET", fetch_eonet),
    ("RSS Noticias", fetch_rss_news),
    ("Guerra (prensa/OSINT)", fetch_war_news),
    ("Comunicados oficiales", fetch_statements),
    ("Medios estatales", fetch_state_media),
    ("Telegram", fetch_telegram),
]


SOURCES_CONFIG_FILE = os.getenv("SOURCES_CONFIG_FILE", "sources_config.json")
_cfg_lock = threading.Lock()


def _load_disabled() -> set:
    try:
        with open(SOURCES_CONFIG_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return {str(x) for x in d.get("disabled", [])}
    except Exception:
        return set()


DISABLED_SOURCES = _load_disabled()


def set_source_enabled(name: str, enabled: bool):
    with _cfg_lock:
        if enabled:
            DISABLED_SOURCES.discard(name)
        else:
            DISABLED_SOURCES.add(name)
        tmp = SOURCES_CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"disabled": sorted(DISABLED_SOURCES)}, f, ensure_ascii=False, indent=2)
        os.replace(tmp, SOURCES_CONFIG_FILE)


def fetch_all() -> Dict[str, Any]:
    t_start = time.time()
    all_alerts: List[dict] = []
    errors: List[dict] = []

    def run(src):
        name, fn = src
        if name in DISABLED_SOURCES:
            _set_status(name, "skipped", 0, "desactivada por el usuario")
            return []
        t0 = time.time()
        try:
            res = fn() or []
        except Exception as e:
            _fail(name, e)
            res = []
        with _status_lock:
            if name in SOURCE_STATUS:
                SOURCE_STATUS[name]["ms"] = int((time.time() - t0) * 1000)
        return res

    results = run_parallel(run, SOURCES, workers=len(SOURCES))
    seen = set()
    for src, res, _err in results:
        for a in res:
            a.setdefault("cred", cred_for(a.get("source", "")))
            a.setdefault("tags", [])
            if not a.get("place"):
                loc_p = geolocate_text(f"{a.get('region', '')} {a.get('description', '')}")
                a["place"] = loc_p[2] if loc_p else str(a.get("region", ""))[:40]
            fp = make_fingerprint(a)
            if fp in seen:
                continue
            seen.add(fp)
            all_alerts.append(a)

    all_alerts = merge_seismic(all_alerts)
    correlate_explosions(all_alerts)

    with _status_lock:
        status_snapshot = json.loads(json.dumps(SOURCE_STATUS))
    for _dn in DISABLED_SOURCES:
        if _dn in status_snapshot:
            status_snapshot[_dn]["disabled"] = True
    for name, _fn in SOURCES:
        st = status_snapshot.get(name)
        if st and st.get("status") in ("error", "warn"):
            errors.append({
                "source": name, "error": st.get("error", "")[:100],
                "message": (f"ERROR: no se pudo obtener {name}" if st["status"] == "error" else f"AVISO: {name}")
                           + (f" — {st.get('error', '')[:100]}" if st.get("error") else ""),
                "timestamp": st.get("ts", now_iso())
            })

    save_alerts(all_alerts)
    all_alerts.sort(key=lambda x: x.get("detected_at", ""), reverse=True)

    special = []
    for a in all_alerts:
        if (a.get("type") == "official_statement" and a.get("severity") in ("critical", "high")
                and set(a.get("tags") or []) & WARNING_TAGS and age_seconds(a.get("detected_at", "")) <= 6 * 3600):
            special.append({"source": a["source"], "description": a["description"][:240], "severity": a["severity"],
                            "detected_at": a["detected_at"], "link": a.get("link"), "tags": a.get("tags")})
    special.sort(key=lambda x: (_LEVEL_ORDER.get(x["severity"], 0), x["detected_at"]), reverse=True)

    stats = {
        "total": len(all_alerts), "by_source": {}, "by_type": {},
        "by_severity": {}, "by_cred": {},
        "critical": sum(1 for a in all_alerts if a.get("severity") == "critical"),
        "critical_recent": sum(1 for a in all_alerts
                               if a.get("severity") == "critical" and age_seconds(a.get("detected_at", "")) <= 1800),
        "explosions": sum(1 for a in all_alerts
                          if a.get("type") in EXPLOSION_TYPES and age_seconds(a.get("detected_at", "")) <= 6 * 3600),
        "special_statements": special[:5],
        "source_status": status_snapshot,
        "generated_at": now_iso(),
    }
    for a in all_alerts:
        stats["by_source"][a["source"]] = stats["by_source"].get(a["source"], 0) + 1
        stats["by_type"][a["type"]] = stats["by_type"].get(a["type"], 0) + 1
        stats["by_severity"][a.get("severity", "low")] = stats["by_severity"].get(a.get("severity", "low"), 0) + 1
        stats["by_cred"][a.get("cred", "D")] = stats["by_cred"].get(a.get("cred", "D"), 0) + 1
    FETCH_LOG.append({"ts": now_iso(), "ms": int((time.time() - t_start) * 1000),
                      "total": len(all_alerts), "errors": len(errors)})
    return {"alerts": all_alerts, "errors": errors, "stats": stats}


# ============================================================
# FLASK
# ============================================================
app = Flask(__name__)
try:
    app.json.ensure_ascii = False
except Exception:
    app.config["JSON_AS_ASCII"] = False

_cache = {"data": None, "last_fetch": None}
_cache_lock = threading.Lock()
_fetch_lock = threading.Lock()


def invalidate_cache():
    with _cache_lock:
        _cache["data"] = None
        _cache["last_fetch"] = None


def _cached_fresh():
    with _cache_lock:
        if _cache["data"] and _cache["last_fetch"]:
            age = (datetime.now(timezone.utc) - _cache["last_fetch"]).total_seconds()
            if age < CACHE_TTL_SECONDS:
                return _cache["data"], int(age)
    return None, 0


@app.route("/api/alerts")
def get_alerts():
    force = request.args.get("force", "0") == "1"
    if not force:
        data, age = _cached_fresh()
        if data:
            return jsonify({"cached": True, "age_seconds": age, "data": data})
    with _fetch_lock:
        if not force:  # otro hilo pudo refrescar mientras esperábamos
            data, age = _cached_fresh()
            if data:
                return jsonify({"cached": True, "age_seconds": age, "data": data})
        result = fetch_all()
        with _cache_lock:
            _cache["data"] = result
            _cache["last_fetch"] = datetime.now(timezone.utc)
    return jsonify({"cached": False, "age_seconds": 0, "data": result})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "version": APP_VERSION, "time": now_iso()})


@app.route("/api/sources")
def api_sources():
    with _status_lock:
        return jsonify({"sources": json.loads(json.dumps(SOURCE_STATUS))})


@app.route("/api/history")
def api_history():
    hours = request.args.get("hours", 24.0, type=float)
    limit = request.args.get("limit", 500, type=int)
    rows = query_history(hours=hours, source=request.args.get("source", ""), q=request.args.get("q", ""), limit=limit)
    return jsonify({"count": len(rows), "alerts": rows})


@app.route("/api/export")
def api_export():
    fmt = request.args.get("format", "csv").lower()
    hours = request.args.get("hours", 24.0, type=float)
    rows = query_history(hours=hours, limit=5000)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if fmt == "json":
        body = json.dumps(rows, ensure_ascii=False, indent=2)
        return Response(body, mimetype="application/json",
                        headers={"Content-Disposition": f"attachment; filename=alerts_{stamp}.json"})
    buf = io.StringIO()
    w = csv.writer(buf)
    cols = ["detected_at", "source", "type", "severity", "region", "lat", "lon", "description", "link"]
    w.writerow(cols)
    for r in rows:
        w.writerow([r.get(c, "") for c in cols])
    return Response("\ufeff" + buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=alerts_{stamp}.csv"})


@app.route("/api/telegram/channels", methods=["GET", "POST"])
def api_tg_channels():
    if request.method == "GET":
        return jsonify({"channels": get_telegram_channels(include_disabled=True)})
    body = request.get_json(silent=True) or {}
    u = clean_username(body.get("id") or body.get("url") or body.get("username") or "")
    if not u:
        return jsonify({"error": "Usuario o enlace de Telegram no válido"}), 400
    name = str(body.get("name") or "").strip()[:60]
    warning = ""
    try:
        info = tg_channel_info(u)
        if not info.get("exists"):
            return jsonify({"error": f"@{u} no existe o no es público"}), 404
        name = name or info.get("title") or u
    except Exception as e:
        warning = f"No se pudo verificar ({str(e)[:80]}); añadido igualmente"
        name = name or u
    add_telegram_channel(u, name, str(body.get("lang") or "xx"), bool(body.get("keywords_only", True)))
    invalidate_cache()
    return jsonify({"ok": True, "id": u, "name": name, "warning": warning})


@app.route("/api/telegram/channels/<cid>", methods=["DELETE"])
def api_tg_channel_delete(cid):
    u = clean_username(cid)
    if not u or not remove_telegram_channel(u):
        return jsonify({"error": "Canal no encontrado"}), 404
    invalidate_cache()
    return jsonify({"ok": True})


@app.route("/api/telegram/search")
def api_tg_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"error": "Consulta demasiado corta"}), 400
    try:
        out = tg_search(q)
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 502
    following = {c["id"].lower() for c in get_telegram_channels()}
    for r in out["results"]:
        r["following"] = (r.get("username") or "").lower() in following
    return jsonify(out)


@app.route("/api/telegram/preview")
def api_tg_preview():
    ch = request.args.get("channel", "")
    q = (request.args.get("q") or "").strip()
    try:
        return jsonify(tg_preview(ch, q))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 502


# ============================================================
# TRADUCTOR (sin claves: Google gtx público + MyMemory; opcional DeepL / LibreTranslate)
# ============================================================
_tr_cache: "OrderedDict[str, dict]" = OrderedDict()
_tr_lock = threading.Lock()
TR_MAX_CACHE = 3000


def _tr_deepl(text: str, target: str):
    r = requests.post("https://api-free.deepl.com/v2/translate",
                      headers={"Authorization": f"DeepL-Auth-Key {DEEPL_API_KEY}"},
                      data={"text": text, "target_lang": target.upper()}, timeout=12)
    r.raise_for_status()
    t = (r.json().get("translations") or [{}])[0]
    return t.get("text", ""), (t.get("detected_source_language") or "").lower()


def _tr_libre(text: str, target: str):
    payload = {"q": text, "source": "auto", "target": target, "format": "text"}
    if LIBRETRANSLATE_KEY:
        payload["api_key"] = LIBRETRANSLATE_KEY
    r = requests.post(LIBRETRANSLATE_URL + "/translate", json=payload, timeout=15)
    r.raise_for_status()
    j = r.json()
    return j.get("translatedText", ""), ((j.get("detectedLanguage") or {}).get("language") or "")


def _tr_google(text: str, target: str):
    r = create_session().get("https://translate.googleapis.com/translate_a/single",
                             params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text},
                             headers=get_headers(), timeout=10)
    r.raise_for_status()
    data = r.json()
    out = "".join(seg[0] for seg in (data[0] or []) if seg and seg[0])
    return out, (data[2] if len(data) > 2 and isinstance(data[2], str) else "")


def _tr_mymemory(text: str, target: str):
    r = create_session().get("https://api.mymemory.translated.net/get",
                             params={"q": text[:450], "langpair": f"Autodetect|{target}"},
                             headers=get_headers(), timeout=10)
    r.raise_for_status()
    return ((r.json().get("responseData") or {}).get("translatedText") or ""), ""


def translation_providers() -> List[Tuple[str, Any]]:
    provs: List[Tuple[str, Any]] = []
    if DEEPL_API_KEY:
        provs.append(("deepl", _tr_deepl))
    if LIBRETRANSLATE_URL:
        provs.append(("libretranslate", _tr_libre))
    provs.append(("google", _tr_google))
    provs.append(("mymemory", _tr_mymemory))
    return provs


def translate_text(text: str, target: str = "es") -> Dict[str, Any]:
    text = (text or "").strip()[:1800]
    if not text:
        return {"text": "", "lang": "", "provider": "", "same": True}
    key = f"{target}:{hashlib.sha1(text.encode('utf-8', 'ignore')).hexdigest()}"
    with _tr_lock:
        if key in _tr_cache:
            _tr_cache.move_to_end(key)
            return _tr_cache[key]
    errors = []
    for pname, fn in translation_providers():
        try:
            out, lang = fn(text, target)
            if out:
                res = {"text": out, "lang": lang or "", "provider": pname,
                       "same": (lang or "").lower()[:2] == target.lower()[:2] or out.strip() == text}
                with _tr_lock:
                    _tr_cache[key] = res
                    while len(_tr_cache) > TR_MAX_CACHE:
                        _tr_cache.popitem(last=False)
                return res
        except Exception as e:
            errors.append(f"{pname}: {str(e)[:60]}")
    raise RuntimeError("; ".join(errors) or "sin traductor disponible")


# ============================================================
# ESTADÍSTICAS Y TELEMETRÍA
# ============================================================
def build_telemetry(total_rows: int) -> Dict[str, Any]:
    with _status_lock:
        src = {k: {"status": v.get("status"), "count": v.get("count", 0), "ms": v.get("ms", 0),
                   "error": v.get("error", ""), "disabled": k in DISABLED_SOURCES} for k, v in SOURCE_STATUS.items()}
    try:
        size = os.path.getsize(DB_PATH)
    except OSError:
        size = 0
    with _cache_lock:
        lf = _cache["last_fetch"]
    return {
        "version": APP_VERSION, "uptime_s": int(time.time() - APP_START), "requests": _req_count["n"],
        "db_rows": total_rows, "db_kb": round(size / 1024, 1),
        "cache_age_s": int((datetime.now(timezone.utc) - lf).total_seconds()) if lf else None,
        "fetches": list(FETCH_LOG)[-40:], "sources": src,
        "telethon": bool(TELETHON_OK and TG_API_ID and TG_API_HASH),
        "translators": [n for n, _ in translation_providers()],
    }


def compute_stats(hours: float = 24.0) -> Dict[str, Any]:
    hours = max(1.0, min(float(hours), 168.0))
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=hours)).isoformat()
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            rows = conn.execute(
                "SELECT source, alert_type, region, severity, description, detected_at, cred, raw_json "
                "FROM alerts WHERE detected_at >= ? ORDER BY detected_at DESC LIMIT 20000", (since,)).fetchall()
            total_rows = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        finally:
            conn.close()
    nb = int(min(168, max(1, round(hours))))
    keys = [(now - timedelta(hours=i)).strftime("%Y-%m-%dT%H") for i in range(nb - 1, -1, -1)]
    tl = {k: {"critical": 0, "high": 0, "medium": 0, "low": 0} for k in keys}
    by_sev, by_type, by_src, by_cred = Counter(), Counter(), Counter(), Counter()
    regions, words, mags = Counter(), Counter(), Counter()
    explosions: List[dict] = []
    c30 = h30 = ex2 = st6 = 0
    cut30 = (now - timedelta(minutes=30)).isoformat()
    cut2h = (now - timedelta(hours=2)).isoformat()
    cut6h = (now - timedelta(hours=6)).isoformat()
    textual = {"telegram_signal", "news", "war_news", "official_statement", "state_media"}
    ground = {"air_raid", "missile_alert", "kyiv_alert", "drone_missile_track", "acled_conflict", "conflict_event"}
    for src, typ, region, sev, desc, det, cred, raw in rows:
        sev = sev if sev in ("critical", "high", "medium", "low") else "low"
        det = det or ""
        if det[:13] in tl:
            tl[det[:13]][sev] += 1
        by_sev[sev] += 1
        by_type[typ or "?"] += 1
        by_src[src or "?"] += 1
        by_cred[cred or "D"] += 1
        if det >= cut30:
            if sev == "critical":
                c30 += 1
            elif sev == "high":
                h30 += 1
        if typ in EXPLOSION_TYPES:
            if det >= cut2h:
                ex2 += 1
            if len(explosions) < 10:
                explosions.append({"detected_at": det, "description": desc, "severity": sev})
        if typ == "official_statement" and sev in ("critical", "high") and det >= cut6h:
            st6 += 1
        if typ in textual:
            loc = geolocate_text(desc or "")
            if loc:
                regions[loc[2]] += 1
            for w in re.findall(r"[^\W\d_]{4,}", (desc or "").lower()):
                if w not in _STOPWORDS:
                    words[w] += 1
        elif typ in ground and region:
            regions[str(region).split(",")[0].strip()[:30]] += 1
        if typ in SEISMIC_TYPES and raw:
            try:
                m = json.loads(raw).get("mag")
                if m is not None:
                    mags[int(float(m))] += 1
            except Exception:
                pass
    idx = min(100, c30 * 8 + h30 * 3 + ex2 * 12 + st6 * 15)
    return {
        "hours": hours, "rows": len(rows),
        "timeline": [{"h": k, **tl[k]} for k in keys],
        "by_severity": dict(by_sev), "by_type": dict(by_type.most_common(12)),
        "by_source": dict(by_src.most_common(12)), "by_cred": dict(by_cred),
        "regions": regions.most_common(10), "words": words.most_common(18),
        "magnitudes": [{"bin": f"M{k}–{k + 1}", "n": mags[k]} for k in sorted(mags)],
        "explosions": explosions,
        "activity": {"index": idx, "critical_30m": c30, "high_30m": h30, "explosions_2h": ex2, "statements_6h": st6},
        "telemetry": build_telemetry(total_rows),
    }


@app.before_request
def _count_requests():
    _req_count["n"] += 1


@app.route("/api/translate", methods=["POST"])
def api_translate():
    body = request.get_json(silent=True) or {}
    target = str(body.get("target") or "es").lower()
    if not re.fullmatch(r"[a-z]{2}(-[a-z]{2})?", target):
        target = "es"
    texts = body.get("texts")
    if texts is None and body.get("text") is not None:
        texts = [body.get("text")]
    if not isinstance(texts, list) or not texts:
        return jsonify({"error": "texts requerido"}), 400
    texts = [str(t) for t in texts[:25]]

    def one(t):
        try:
            r = dict(translate_text(t, target))
            r["ok"] = True
            return r
        except Exception as e:
            return {"text": "", "lang": "", "provider": "", "ok": False, "same": False, "error": str(e)[:120]}

    out = [r for _t, r, _e in run_parallel(one, texts, workers=4)]
    if not any(r.get("ok") for r in out):
        return jsonify({"error": out[0].get("error", "traductor no disponible"), "translations": out}), 502
    return jsonify({"target": target, "translations": out})


@app.route("/api/stats")
def api_stats():
    hours = request.args.get("hours", 24.0, type=float)
    try:
        return jsonify(compute_stats(hours))
    except Exception as e:
        logger.warning(f"stats: {e}")
        return jsonify({"error": str(e)[:200]}), 500


# ============================================================
# DIAGNÓSTICO DE FEEDS + SALUD + INTERRUPTORES
# ============================================================
FEED_HEALTH: Dict[str, Dict[str, Any]] = {}
_feed_lock = threading.Lock()
FEED_COOLDOWN_FAILS = 3
FEED_COOLDOWN_SECONDS = 1800
LAST_DIAG: Dict[str, Any] = {"data": None}


def _feed_name(feed) -> str:
    return str(feed.get("id")) if isinstance(feed, dict) else str(feed[0])


def tracked(group: str, fn):
    """Envuelve el fetch de un feed: registra salud y lo 'enfría' 30 min tras 3 fallos seguidos."""
    def wrapper(feed):
        key = f"{group}/{_feed_name(feed)}"
        with _feed_lock:
            h = FEED_HEALTH.get(key)
            if h and h["fails"] >= FEED_COOLDOWN_FAILS and time.time() - h["last_try"] < FEED_COOLDOWN_SECONDS:
                return []
        t0 = time.time()
        try:
            res = fn(feed)
        except Exception as e:
            _feed_record(key, False, 0, e, int((time.time() - t0) * 1000))
            raise
        _feed_record(key, True, len(res), "", int((time.time() - t0) * 1000))
        return res
    return wrapper


def _feed_record(key: str, ok: bool, items: int, err, ms: int):
    with _feed_lock:
        h = FEED_HEALTH.setdefault(key, {"fails": 0, "ok_count": 0, "last_ok": None, "last_try": 0.0,
                                         "last_err": "", "items": 0, "ms": 0})
        h["last_try"], h["ms"] = time.time(), ms
        if ok:
            h.update({"fails": 0, "last_ok": now_iso(), "items": items, "last_err": ""})
            h["ok_count"] += 1
        else:
            h["fails"] += 1
            h["last_err"] = str(err)[:120]


def probe_url(group: str, name: str, url: str, kind: str = "feed") -> Dict[str, Any]:
    t0 = time.time()
    res: Dict[str, Any] = {"group": group, "name": name, "url": url, "ok": False, "status": None,
                           "ms": 0, "items": None, "newest": None, "error": ""}
    try:
        headers = get_headers(html=(kind in ("feed", "tg")))
        if kind == "oref":
            headers.update({"Referer": "https://www.oref.org.il/", "X-Requested-With": "XMLHttpRequest"})
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        res["status"] = resp.status_code
        if resp.status_code >= 400:
            res["error"] = f"HTTP {resp.status_code}"
        elif kind == "feed":
            items = parse_feed_items(resp.content)
            res["items"] = len(items)
            dts = [i["dt"] for i in items if i["dt"]]
            if dts:
                res["newest"] = max(dts).isoformat()
            res["ok"] = bool(items)
            if not items:
                res["error"] = "sin items (¿no es RSS/Atom?)"
        elif kind == "tg":
            msgs = parse_tg_html(resp.text)
            res["items"] = len(msgs)
            res["ok"] = bool(msgs)
            if not msgs:
                res["error"] = "sin vista previa pública"
        else:
            res["ok"] = True
    except ET.ParseError:
        res["error"] = "respuesta no es XML válido (¿página HTML o bloqueo?)"
    except Exception as e:
        res["error"] = str(e)[:140]
    res["ms"] = int((time.time() - t0) * 1000)
    return res


def diag_targets() -> List[Tuple[str, str, str, str]]:
    t: List[Tuple[str, str, str, str]] = []
    endpoints = [
        ("Tzeva Adom", "https://api.tzevaadom.co.il/alerts-history", "json"),
        ("Oref (oficial)", "https://www.oref.org.il/WarningMessages/alert/alerts.json", "oref"),
        ("alerts.com.ua", "https://alerts.com.ua/api/states", "json"),
        ("Ubilling UA", "https://ubilling.net.ua/aerialalerts/", "json"),
        ("Kyiv Digital", "https://kyiv.digital/open-api/air-alert/state", "json"),
        ("Neptun", "https://neptun.in.ua/api/tracks", "json"),
        ("USGS", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson", "json"),
        ("EMSC", "https://www.seismicportal.eu/fdsnws/event/1/query?format=json&limit=1", "json"),
        ("GEOFON", "https://geofon.gfz-potsdam.de/fdsnws/event/1/query?format=text&limit=1", "json"),
        ("NASA EONET", "https://eonet.gsfc.nasa.gov/api/v3/events?limit=1", "json"),
        ("GDELT GEO", "https://api.gdeltproject.org/api/v2/geo/geo?query=missile&format=GeoJSON&maxpoints=5", "json"),
    ]
    t += [("Alertas y sensores", n, u, k) for n, u, k in endpoints]
    t += [("Noticias RSS", n, u, "feed") for n, u in RSS_FEEDS]
    t += [("Prensa de guerra", n, u, "feed") for n, u, _c, _f in WAR_FEEDS]
    t += [("Comunicados oficiales", n, u, "feed") for n, u, _c, _m in STATEMENT_FEEDS]
    t += [("Medios estatales", n, u, "feed") for n, u in STATE_MEDIA_FEEDS]
    t += [("Telegram", ch["id"], f"https://t.me/s/{ch['id']}", "tg") for ch in get_telegram_channels()]
    return t


def run_diagnostics() -> Dict[str, Any]:
    targets = diag_targets()
    res = [r for _t, r, _e in run_parallel(lambda tg: probe_url(*tg), targets, workers=16) if isinstance(r, dict)]
    out = {"ts": now_iso(), "results": res, "summary": {"ok": sum(1 for r in res if r["ok"]), "total": len(res)}}
    LAST_DIAG["data"] = out
    return out


def diag_report_text(d: Dict[str, Any]) -> str:
    lines = [f"INFORME DE DIAGNÓSTICO — {d['ts']} — {d['summary']['ok']}/{d['summary']['total']} fuentes responden", ""]
    for r in d["results"]:
        flag = "OK " if r["ok"] else "ERR"
        detail = (f"{r['items']} items" if r.get("items") is not None else "") if r["ok"] else r.get("error", "")
        lines.append(f"[{flag}] {r['group']} | {r['name']} | {detail} | {r['ms']}ms | {r['url']}")
    return "\n".join(lines)


def _startup_diag():
    time.sleep(3)
    try:
        d = run_diagnostics()
        bad = [f"{r['name']} ({r['error']})" for r in d["results"] if not r["ok"]]
        print(f"  [DIAG] {d['summary']['ok']}/{d['summary']['total']} fuentes responden")
        if bad:
            print("  [DIAG] No responden: " + "; ".join(bad)[:900])
        print("  [DIAG] Detalle completo en el panel: pestaña ERRORES > DIAGNÓSTICO")
    except Exception as e:
        print(f"  [DIAG] fallo: {e}")


@app.route("/api/diagnostics")
def api_diagnostics():
    d = LAST_DIAG["data"]
    if request.args.get("run", "0") == "1" or d is None:
        d = run_diagnostics()
    if request.args.get("format") == "txt":
        return Response(diag_report_text(d), mimetype="text/plain; charset=utf-8",
                        headers={"Content-Disposition": "attachment; filename=diagnostico_feeds.txt"})
    with _feed_lock:
        health = json.loads(json.dumps(FEED_HEALTH))
    d = dict(d)
    d["health"] = health
    return jsonify(d)


@app.route("/api/sources/toggle", methods=["POST"])
def api_source_toggle():
    body = request.get_json(silent=True) or {}
    name = str(body.get("name") or "")
    if name not in {n for n, _ in SOURCES}:
        return jsonify({"error": "Fuente desconocida"}), 404
    set_source_enabled(name, bool(body.get("enabled")))
    invalidate_cache()
    return jsonify({"ok": True, "disabled": sorted(DISABLED_SOURCES)})


HTML_PANEL = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GLOBAL EARLY WARNING OSINT</title>
<script src="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css" rel="stylesheet" />
<script src="https://unpkg.com/deck.gl@9.0.0/dist.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {
--bg: #0a0a0a; --bg-panel: #111; --bg-card: #1a1a1a; --border: #2a2a2a;
--text: #e0e0e0; --dim: #888;
--red: #ff3344; --green: #00ff88; --orange: #ff8800;
--purple: #aa66ff; --yellow: #ffcc00; --blue: #4488ff;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Courier New', monospace; background: var(--bg); color: var(--text); height: 100vh; overflow: hidden; display: flex; flex-direction: column; }
.alert-banner { display: none; background: var(--red); color: #fff; padding: 10px 20px; text-align: center; font-weight: bold; font-size: 14px; letter-spacing: 2px; animation: blink 1s infinite; flex-shrink: 0; z-index: 200; }
@keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
.alert-banner.show { display: block; }
.header { display: flex; align-items: center; justify-content: space-between; padding: 10px 20px; background: var(--bg-panel); border-bottom: 1px solid var(--border); flex-shrink: 0; flex-wrap: wrap; gap: 8px; }
.header h1 { font-size: 16px; color: var(--green); letter-spacing: 2px; }
.header .status { display: flex; gap: 10px; align-items: center; font-size: 12px; flex-wrap: wrap; }
.badge { background: var(--bg-card); border: 1px solid var(--border); padding: 4px 8px; border-radius: 3px; font-size: 10px; }
.badge.critical { border-color: var(--red); color: var(--red); }
.badge.total { border-color: var(--green); color: var(--green); }
.dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
.dot.live { background: var(--green); box-shadow: 0 0 8px var(--green); animation: pulse 2s infinite; }
.dot.error { background: var(--red); box-shadow: 0 0 8px var(--red); }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
.main { display: flex; flex: 1; overflow: hidden; position: relative; }
#map { flex: 1; background: #000; }
.map-controls {
position: absolute; top: 12px; left: 12px; z-index: 10;
display: flex; flex-direction: column; gap: 6px;
}
.btn { background: var(--bg-panel); border: 1px solid var(--border); color: var(--text); padding: 6px 10px; font-family: inherit; font-size: 10px; cursor: pointer; border-radius: 3px; text-transform: uppercase; transition: all 0.2s; text-decoration: none; display: inline-block; }
.btn:hover { border-color: var(--green); color: var(--green); }
.btn.active { border-color: var(--green); color: var(--green); background: rgba(0,255,136,0.1); }
.btn-red:hover { border-color: var(--red); color: var(--red); }
.btn-orange:hover { border-color: var(--orange); color: var(--orange); }
.btn-blue:hover { border-color: var(--blue); color: var(--blue); }
.zoom-controls {
position: absolute; top: 12px; right: 462px; z-index: 10;
display: flex; flex-direction: column; gap: 0; background: var(--bg-panel);
border: 1px solid var(--border); border-radius: 3px; overflow: hidden;
}
.zoom-controls button {
background: var(--bg-panel); border: none; color: var(--text); width: 36px; height: 36px;
font-size: 18px; cursor: pointer; font-family: inherit; border-bottom: 1px solid var(--border);
}
.zoom-controls button:last-child { border-bottom: none; }
.zoom-controls button:hover { background: var(--bg-card); color: var(--green); }
.sidebar { width: 450px; background: var(--bg-panel); border-left: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.tabs { display: flex; border-bottom: 1px solid var(--border); flex-shrink: 0; }
.tab { flex: 1; padding: 10px; text-align: center; font-size: 10px; cursor: pointer; color: var(--dim); border-bottom: 2px solid transparent; text-transform: uppercase; }
.tab.active { color: var(--green); border-bottom-color: var(--green); }
.content { flex: 1; overflow-y: auto; padding: 12px; display: none; }
.content.active { display: block; }
.alert-item { background: var(--bg-card); border: 1px solid var(--border); border-left: 3px solid var(--red); padding: 10px; margin-bottom: 8px; border-radius: 3px; cursor: pointer; }
.alert-item:hover { border-left-color: var(--green); }
.alert-item .src { font-size: 10px; color: var(--blue); font-weight: bold; }
.alert-item .time { font-size: 10px; color: var(--orange); float: right; }
.alert-item .desc { font-size: 11px; margin-top: 6px; line-height: 1.4; word-break: break-word; }
.sevtag { font-size: 9px; color: var(--dim); border: 1px solid var(--border); padding: 0 4px; border-radius: 2px; margin-left: 4px; text-transform: uppercase; }
.lnk { color: var(--green); text-decoration: none; font-size: 11px; margin-left: 6px; }
.error-banner { background: rgba(255,51,68,0.1); border: 1px solid var(--red); border-left: 3px solid var(--red); padding: 8px; margin-bottom: 8px; border-radius: 3px; font-size: 11px; color: var(--red); }
.chart-box { background: var(--bg-card); border: 1px solid var(--border); border-radius: 3px; padding: 12px; margin-bottom: 12px; }
.chart-box h3 { font-size: 10px; color: var(--yellow); margin-bottom: 8px; text-transform: uppercase; }
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.loading { text-align: center; padding: 40px; color: var(--dim); font-size: 12px; }
.spinner { width: 20px; height: 20px; border: 2px solid var(--border); border-top-color: var(--green); border-radius: 50%; animation: spin 0.8s linear infinite; margin: 0 auto 12px; }
@keyframes spin { to { transform: rotate(360deg); } }
/* --- v2.2: filtros, buscador Telegram, toast --- */
.filterbar { margin-bottom: 10px; display: flex; flex-direction: column; gap: 6px; }
.frow { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
.fi { background: var(--bg-card); border: 1px solid var(--border); color: var(--text); padding: 6px 8px; font-family: inherit; font-size: 11px; border-radius: 3px; flex: 1; min-width: 0; }
.fi:focus { outline: none; border-color: var(--green); }
select.fi { flex: 1 1 30%; }
.st-row { display: flex; gap: 8px; font-size: 11px; padding: 4px 0; border-bottom: 1px solid var(--border); align-items: center; }
.st-row .nm { flex: 1; }
.st-row .meta { color: var(--dim); font-size: 10px; }
.tg-item { background: var(--bg-card); border: 1px solid var(--border); border-left: 3px solid var(--blue); padding: 10px; margin-bottom: 8px; border-radius: 3px; }
.tg-item .tg-title { font-size: 12px; font-weight: bold; word-break: break-word; }
.tg-item .tg-user { font-size: 10px; color: var(--blue); }
.tg-item .tg-desc { font-size: 11px; color: var(--dim); margin: 6px 0; line-height: 1.4; word-break: break-word; }
.tg-btns { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
.tg-msg { border-bottom: 1px solid var(--border); padding: 6px 0; font-size: 11px; line-height: 1.4; word-break: break-word; }
.tg-msg .tm { color: var(--orange); font-size: 10px; }
.hint { font-size: 10px; color: var(--dim); margin: 6px 0 10px; line-height: 1.4; }
#toast { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); background: var(--bg-panel); border: 1px solid var(--green); color: var(--green); padding: 8px 16px; border-radius: 3px; font-size: 12px; z-index: 999; display: none; max-width: 80vw; }
/* --- v2.3: comunicados, credibilidad, traducción, estadísticas --- */
.alert-banner.stmt { background: var(--yellow); color: #000; animation: none; cursor: pointer; font-size: 13px; letter-spacing: 1px; }
.trans { font-size: 11px; color: var(--green); margin-top: 4px; line-height: 1.4; word-break: break-word; border-left: 2px solid var(--green); padding-left: 6px; }
.credtag { font-size: 9px; font-weight: bold; padding: 0 5px; border-radius: 2px; margin-left: 4px; color: #000; }
.cred-A { background: var(--green); } .cred-B { background: var(--blue); } .cred-C { background: var(--orange); } .cred-D { background: #666; color: #fff; }
.sevtag.tagw { color: var(--yellow); border-color: var(--yellow); }
.kpis { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 12px; }
.kpi { background: var(--bg-card); border: 1px solid var(--border); border-radius: 3px; padding: 8px; text-align: center; }
.kpi .kv { font-size: 22px; font-weight: bold; }
.kpi .kl { font-size: 9px; color: var(--dim); margin-top: 4px; text-transform: uppercase; }
.ex-item { font-size: 11px; border-bottom: 1px solid var(--border); padding: 5px 0; color: var(--purple); word-break: break-word; }
.tag-oficial { color: var(--green); border-color: var(--green); }
.tag-estatal { color: var(--orange); border-color: var(--orange); }
.tag-telegram { color: var(--blue); border-color: var(--blue); }
.tag-sin-verificar { color: #999; border-color: #666; }
</style>
</head>
<body>
<div class="alert-banner" id="alertBanner">⚠ ALERTA ACTIVA — MISIL / DRON DETECTADO ⚠</div>
<div class="alert-banner stmt" id="stmtBanner" onclick="openStatements()" title="Ver comunicados oficiales"></div>
<div class="header">
<h1>▶ GLOBAL EARLY WARNING OSINT</h1>
<div class="status">
<span><span class="dot live" id="statusDot"></span><span id="statusText">INIT</span></span>
<span class="badge total" id="badgeTotal">0 alertas</span>
<span class="badge critical" id="badgeCritical">0 críticas</span>
<span id="lastUpdate" style="color:var(--dim)">--:--:--</span>
<button class="btn" onclick="refresh(true)" title="Fuerza recarga sin caché">⟳ REFRESH</button>
<button class="btn" onclick="toggleAutoRefresh()" id="autoBtn" title="Pausa/activa auto-refresh cada 15s">⏸ AUTO</button>
<button class="btn" onclick="toggleSound()" id="soundBtn">🔇 SONIDO OFF</button>
<button class="btn" onclick="toggleNotify()" id="notifyBtn" title="Avisos de escritorio ante alertas críticas, comunicados y explosiones">🔔 AVISOS OFF</button>
</div>
</div>
<div class="main">
<div id="map"></div>
<div class="map-controls">
<button class="btn active" id="btn2D" onclick="setMapMode('2d')" title="Vista plana">2D</button>
<button class="btn" id="btn3D" onclick="setMapMode('3d')" title="Vista inclinada">3D</button>
<button class="btn btn-orange" onclick="toggleBasemap()" title="Alternar Dark/Light">DARK/LIGHT</button>
<button class="btn btn-blue" onclick="fitAlerts()" title="Encuadrar todas las alertas">FIT ALERTS</button>
<button class="btn btn-red" onclick="resetView()" title="Resetear vista inicial">RESET</button>
</div>
<div class="zoom-controls">
<button onclick="map.zoomIn()" title="Zoom in">+</button>
<button onclick="map.zoomOut()" title="Zoom out">−</button>
</div>
<div class="sidebar">
<div class="tabs">
<div class="tab active" data-tab="feed">📡 ALERTAS</div>
<div class="tab" data-tab="search">🔎 BUSCAR</div>
<div class="tab" data-tab="charts">📊 STATS</div>
<div class="tab" data-tab="errors">⚠ ERRORES</div>
</div>
<div class="content active" id="tab-feed">
<div class="chart-box" id="summaryBox" style="margin-bottom:10px"><h3>📋 RESUMEN DE SITUACIÓN</h3><div id="summaryBody" class="hint">Calculando...</div></div>
<div class="filterbar">
<input id="fSearch" class="fi" type="text" placeholder="🔎 filtrar alertas (texto, fuente, región)..." oninput="onFilterChange()">
<div class="frow">
<select id="fSev" class="fi" onchange="onFilterChange()">
<option value="">Severidad: todas</option><option value="critical">critical</option><option value="high">high</option><option value="medium">medium</option><option value="low">low</option>
</select>
<select id="fSrc" class="fi" onchange="onFilterChange()"><option value="">Fuente: todas</option></select>
<select id="fRange" class="fi" onchange="onRangeChange()">
<option value="live">LIVE</option><option value="1">Hist. 1h</option><option value="6">Hist. 6h</option><option value="24">Hist. 24h</option><option value="168">Hist. 7d</option>
</select>
</div>
<div class="frow">
<select id="fType" class="fi" onchange="onFilterChange()">
<option value="">Tipo: todos</option><option value="alerta">Alertas misil/dron</option><option value="explosion">Posibles explosiones</option><option value="sismo">Sismos</option><option value="comunicado">Comunicados oficiales</option><option value="estatal">Medios estatales</option><option value="noticia">Noticias de guerra</option><option value="telegram">Telegram</option>
</select>
<select id="fCred" class="fi" onchange="onFilterChange()">
<option value="">Credibilidad: todas</option><option value="A">A · Oficial/instrumental</option><option value="B">B · Prensa/OSINT</option><option value="C">C · Parte implicada</option><option value="D">D · No verificado</option>
</select>
</div>
<div class="frow">
<button class="btn" id="tradBtn" onclick="toggleTrad()" title="Traducir automáticamente (ruso, ucraniano, hebreo...)">🌐 TRAD: OFF</button>
<select id="tradLang" class="fi" onchange="onTradLang()" style="flex:0 0 auto"><option value="es">→ Español</option><option value="en">→ English</option></select>
</div>
<div class="frow">
<span class="badge" id="fCount">0/0</span>
<button class="btn" onclick="exportData('csv')" title="Descargar historial CSV">⬇ CSV</button>
<button class="btn" onclick="exportData('json')" title="Descargar historial JSON">⬇ JSON</button>
</div>
</div>
<div id="feedList"><div class="loading"><div class="spinner"></div>CARGANDO FUENTES...</div></div>
</div>
<div class="content" id="tab-search">
<div class="frow">
<input id="tgQuery" class="fi" type="text" placeholder="Canal/grupo Telegram: nombre, @usuario, t.me/..., tema" onkeydown="if(event.key==='Enter')tgSearch()">
<button class="btn" style="flex:0 0 auto" onclick="tgSearch()">BUSCAR</button>
</div>
<div class="hint">Busca canales y grupos públicos de Telegram (motores web sin API; con Telethon también búsqueda nativa). Pulsa VER para leer los últimos mensajes y SEGUIR para añadirlo al monitor.</div>
<div id="tgResults"></div>
<div class="chart-box" id="tgPreviewBox" style="display:none"></div>
<div class="chart-box"><h3>CANALES MONITORIZADOS</h3><div id="tgChannels"><div class="loading">Cargando...</div></div></div>
</div>
<div class="content" id="tab-charts">
<div class="frow" style="margin-bottom:10px">
<select id="stRange" class="fi" onchange="loadStats()"><option value="6">Últimas 6h</option><option value="24" selected>Últimas 24h</option><option value="72">Últimas 72h</option><option value="168">Últimos 7 días</option></select>
<button class="btn" style="flex:0 0 auto" onclick="loadStats()" title="Actualizar estadísticas">⟳</button>
</div>
<div class="kpis" id="kpis"></div>
<div class="chart-box"><h3>LÍNEA DE TIEMPO (alertas por hora y severidad, UTC)</h3><canvas id="chartTimeline"></canvas></div>
<div class="chart-box"><h3>ALERTAS POR FUENTE (en vivo)</h3><canvas id="chartSources"></canvas></div>
<div class="chart-box"><h3>TOTAL ALERTAS</h3><div style="font-size:40px;color:var(--red);text-align:center;padding:20px 0;" id="totalAlerts">0</div></div>
<div class="chart-box"><h3>CRÍTICAS</h3><div style="font-size:40px;color:var(--orange);text-align:center;padding:20px 0;" id="criticalAlerts">0</div></div>
<div class="chart-box"><h3>POR SEVERIDAD</h3><canvas id="chartSeverity"></canvas></div>
<div class="chart-box"><h3>POR TIPO</h3><canvas id="chartTypes"></canvas></div>
<div class="chart-box"><h3>CREDIBILIDAD DE LA FUENTE (A oficial · B prensa/OSINT · C parte implicada · D no verificado)</h3><canvas id="chartCred"></canvas></div>
<div class="chart-box"><h3>SISMOS — MAGNITUDES Y POSIBLES EXPLOSIONES</h3><canvas id="chartMag"></canvas><div id="exList" style="margin-top:8px"></div></div>
<div class="chart-box"><h3>ZONAS MÁS MENCIONADAS</h3><canvas id="chartRegions"></canvas></div>
<div class="chart-box"><h3>PALABRAS CLAVE (Telegram / noticias / comunicados)</h3><div id="wordCloud"></div></div>
<div class="chart-box"><h3>TELEMETRÍA — LATENCIA POR FUENTE (ms)</h3><canvas id="chartLatency"></canvas></div>
<div class="chart-box"><h3>TELEMETRÍA — CICLOS DE CONSULTA</h3><canvas id="chartFetch"></canvas></div>
<div class="chart-box"><h3>TELEMETRÍA DEL SISTEMA</h3><div id="sysTel"></div></div>
</div>
<div class="content" id="tab-errors">
<div class="frow" style="margin-bottom:10px">
<button class="btn btn-blue" onclick="runDiagnostics()" title="Prueba todos los feeds y muestra cuáles responden">🩺 DIAGNÓSTICO DE FEEDS</button>
<button class="btn" onclick="exportDiag()" title="Descargar informe de texto">⬇ INFORME</button>
</div>
<div id="diagBox"></div>
<div id="errList"><div class="loading">Sin errores</div></div>
</div>
</div>
</div>
<div id="toast"></div>
<script>
let allAlerts = [], histAlerts = [], allErrors = [], allStatus = {}, visibleList = [];
let map, overlay = null, overlayFailed = false, mapReady = false, mapPending = false, fbMarkers = [];
let soundOn = false, audioCtx = null, lastCritical = 0;
let autoRefreshTimer = null, autoRefreshOn = true, isDark = true, loadingNow = false;
let tgFollowing = new Set(), tgLastResults = [], tgPvUser = '';
const CARTO_DARK = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const CARTO_LIGHT = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

/* ---------- utilidades ---------- */
function esc(s) {
return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }
function fmtTime(iso) { const d = new Date(iso); return isNaN(d) ? '--:--' : d.toLocaleTimeString(); }
function fmtDateTime(iso) { const d = new Date(iso); return isNaN(d) ? '' : d.toLocaleString(); }
function hasGeo(a) {
const la = Number(a.lat), lo = Number(a.lon);
return a.lat != null && a.lon != null && isFinite(la) && isFinite(lo) && !(la === 0 && lo === 0);
}
function toast(msg) {
const t = document.getElementById('toast');
t.textContent = msg; t.style.display = 'block';
clearTimeout(toast._t); toast._t = setTimeout(() => { t.style.display = 'none'; }, 3500);
}

/* ---------- mapa ---------- */
map = new maplibregl.Map({
container: 'map',
style: CARTO_DARK,
center: [30, 48], zoom: 3.5, pitch: 0,
scrollZoom: true,
doubleClickZoom: true,
dragRotate: true,
touchZoomRotate: true
});
map.addControl(new maplibregl.NavigationControl({ showCompass: true, showZoom: false }), 'bottom-left');
map.on('load', () => { mapReady = true; console.log('[MAP] Ready'); if (mapPending) { mapPending = false; updateMap(); } });

function setMapMode(mode) {
document.getElementById('btn2D').classList.toggle('active', mode === '2d');
document.getElementById('btn3D').classList.toggle('active', mode === '3d');
map.easeTo({ pitch: mode === '3d' ? 55 : 0, bearing: mode === '3d' ? -15 : 0, duration: 800 });
}

function toggleBasemap() {
isDark = !isDark;
map.setStyle(isDark ? CARTO_DARK : CARTO_LIGHT);
toast(isDark ? 'Mapa: oscuro' : 'Mapa: claro');
}

function fitAlerts() {
const pts = getVisible().filter(hasGeo);
if (pts.length === 0) { toast('No hay alertas con coordenadas válidas'); return; }
const lons = pts.map(a => Number(a.lon)), lats = pts.map(a => Number(a.lat));
const minLon = Math.min(...lons), maxLon = Math.max(...lons), minLat = Math.min(...lats), maxLat = Math.max(...lats);
if (minLon === maxLon && minLat === maxLat) { map.flyTo({ center: [minLon, minLat], zoom: 7, duration: 1000 }); return; }
map.fitBounds([[minLon, minLat], [maxLon, maxLat]], { padding: 80, duration: 1000, maxZoom: 8 });
}

function resetView() {
map.easeTo({ center: [30, 48], zoom: 3.5, pitch: 0, bearing: 0, duration: 800 });
document.getElementById('btn2D').classList.add('active');
document.getElementById('btn3D').classList.remove('active');
}

/* ---------- sonido ---------- */
function toggleSound() {
soundOn = !soundOn;
document.getElementById('soundBtn').textContent = soundOn ? '🔊 SONIDO ON' : '🔇 SONIDO OFF';
if (soundOn) {
if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
if (audioCtx.state === 'suspended') audioCtx.resume();
playAlarm(0.35);
}
}

function playAlarm(dur) {
if (!soundOn || !audioCtx) return;
if (audioCtx.state === 'suspended') audioCtx.resume();
const d = dur || 1.5;
const osc = audioCtx.createOscillator();
const gain = audioCtx.createGain();
osc.connect(gain); gain.connect(audioCtx.destination);
osc.frequency.value = 880; osc.type = 'sawtooth';
gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + d);
osc.start(); osc.stop(audioCtx.currentTime + d);
}

/* ---------- auto refresh ---------- */
function toggleAutoRefresh() {
autoRefreshOn = !autoRefreshOn;
document.getElementById('autoBtn').textContent = autoRefreshOn ? '⏸ AUTO' : '▶ AUTO';
document.getElementById('autoBtn').classList.toggle('active', !autoRefreshOn);
if (autoRefreshOn) startAutoRefresh(); else stopAutoRefresh();
toast(autoRefreshOn ? 'Auto-refresh activado (15s)' : 'Auto-refresh en pausa');
}
function startAutoRefresh() {
stopAutoRefresh();
autoRefreshTimer = setInterval(() => loadData(false), 15000);
}
function stopAutoRefresh() {
if (autoRefreshTimer) { clearInterval(autoRefreshTimer); autoRefreshTimer = null; }
}

/* ---------- carga de datos ---------- */
async function loadData(force) {
if (loadingNow && !force) return;
loadingNow = true;
if (force) document.getElementById('statusText').textContent = 'ACTUALIZANDO...';
try {
const url = force ? '/api/alerts?force=1' : '/api/alerts';
const resp = await fetch(url);
if (!resp.ok) throw new Error('HTTP ' + resp.status);
const json = await resp.json();
allAlerts = json.data.alerts || [];
allErrors = json.data.errors || [];
const stats = json.data.stats || { total: 0, critical: 0, by_source: {} };
allStatus = stats.source_status || {};
fillSources(); refreshView(); renderErrors(); updateCharts(stats); renderSummary(stats);

document.getElementById('badgeTotal').textContent = `${stats.total || 0} alertas`;
document.getElementById('badgeCritical').textContent = `${stats.critical || 0} críticas`;

const banner = document.getElementById('alertBanner');
const critical = (stats.critical_recent != null) ? stats.critical_recent : (stats.critical || 0);
if (critical > 0) { banner.classList.add('show'); banner.textContent = `⚠ ${critical} ALERTA(S) CRÍTICA(S) ACTIVA(S) ⚠`; }
else { banner.classList.remove('show'); }
if (critical > lastCritical) { playAlarm(); notify('🚨 Alerta crítica', critical + ' alerta(s) crítica(s) activa(s)'); }
lastCritical = critical;
const sp = stats.special_statements || [];
updateStmtBanner(sp);
if (sp.length > lastStmt) { playAlarm(); notify('📜 Comunicado especial', sp[0].source + ': ' + sp[0].description); }
lastStmt = sp.length;
const exN = stats.explosions || 0;
if (exN > lastExpl) notify('💥 Posible explosión sísmica', exN + ' evento(s) en las últimas 6 h');
lastExpl = exN;
firstData = false;

document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
document.getElementById('statusDot').className = 'dot ' + (allErrors.length > 0 ? 'error' : 'live');
document.getElementById('statusText').textContent = allErrors.length > 0 ? 'PARCIAL' : (json.cached ? 'CACHED' : 'LIVE');
if (rangeKey() !== 'live') loadHistory();
} catch (e) {
document.getElementById('feedList').innerHTML = `<div class="error-banner">❌ Error de conexión: ${esc(e.message)}</div>`;
document.getElementById('statusDot').className = 'dot error';
document.getElementById('statusText').textContent = 'OFFLINE';
} finally {
loadingNow = false;
}
}

async function loadHistory() {
const h = parseInt(rangeKey(), 10) || 24;
try {
const r = await fetch('/api/history?hours=' + h + '&limit=1000');
if (!r.ok) throw new Error('HTTP ' + r.status);
const j = await r.json();
histAlerts = j.alerts || [];
fillSources(); refreshView();
} catch (e) {
toast('Error cargando historial: ' + e.message);
}
}

/* ---------- filtros ---------- */
function rangeKey() { return document.getElementById('fRange').value; }
function baseAlerts() { return rangeKey() === 'live' ? allAlerts : histAlerts; }
function typeGroup(a) {
const t = a.type || '';
if (['missile_alert', 'air_raid', 'kyiv_alert', 'drone_missile_track'].includes(t)) return 'alerta';
if (['seismic_explosion', 'explosion_candidate', 'nuclear_test_candidate'].includes(t)) return 'explosion';
if (t === 'earthquake') return 'sismo';
if (t === 'official_statement') return 'comunicado';
if (t === 'state_media') return 'estatal';
if (['war_news', 'news', 'news_geo'].includes(t)) return 'noticia';
if (t === 'telegram_signal') return 'telegram';
return 'otro';
}
function getVisible() {
const q = (document.getElementById('fSearch').value || '').trim().toLowerCase();
const sev = document.getElementById('fSev').value;
const src = document.getElementById('fSrc').value;
const typ = document.getElementById('fType').value;
const cred = document.getElementById('fCred').value;
return baseAlerts().filter(a =>
(!sev || a.severity === sev) &&
(!src || a.source === src) &&
(!typ || typeGroup(a) === typ || (typ === 'sismo' && typeGroup(a) === 'explosion')) &&
(!cred || (a.cred || 'D') === cred) &&
(!q || ((a.source || '') + ' ' + (a.description || '') + ' ' + (a.region || '') + ' ' + (a.type || '') + ' ' + (a.tags || []).join(' ')).toLowerCase().includes(q)));
}
function fillSources() {
const sel = document.getElementById('fSrc');
const cur = sel.value;
const names = Array.from(new Set(baseAlerts().map(a => a.source).filter(Boolean))).sort();
if (cur && !names.includes(cur)) names.push(cur);
sel.innerHTML = '<option value="">Fuente: todas</option>' + names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
sel.value = cur;
}
function onFilterChange() { refreshView(); }
function onRangeChange() {
document.getElementById('fSrc').value = '';
if (rangeKey() === 'live') { fillSources(); refreshView(); } else { loadHistory(); }
}
function exportData(fmt) {
const r = rangeKey();
const h = r === 'live' ? 24 : r;
window.location.href = '/api/export?format=' + fmt + '&hours=' + h;
}
function refreshView() { renderFeed(); updateMap(); }

/* ---------- feed ---------- */
function renderFeed(skipAuto) {
const c = document.getElementById('feedList');
visibleList = getVisible();
document.getElementById('fCount').textContent = visibleList.length + '/' + baseAlerts().length;
if (visibleList.length === 0) { c.innerHTML = '<div class="loading">Sin alertas activas</div>'; return; }
c.innerHTML = visibleList.slice(0, 300).map((a, i) => {
const cred = a.cred || 'D';
const tr = trCache.get(trKey(a.description || ''));
const showTr = tr && tr.text && !tr.same && tr.text.trim() !== (a.description || '').trim();
const tags = (a.tags || []).map(t => `<span class="sevtag tagw tag-${esc(String(t).replace(/[^a-z0-9]+/gi, '-'))}">${esc(t)}</span>`).join('');
return `<div class="alert-item" onclick="focusIdx(${i})">
<span class="src">${esc(a.source)}</span><span class="sevtag">${esc(a.severity || '')}</span><span class="credtag cred-${esc(cred)}" title="${esc(CRED_LABELS[cred] || '')}">${esc(cred)}</span>${tags}
${safeUrl(a.link) ? `<a class="lnk" href="${esc(safeUrl(a.link))}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Abrir fuente">↗</a>` : ''}
${needsTr(a) ? `<span class="lnk" style="cursor:pointer" onclick="event.stopPropagation();tradOne(${i})" title="Traducir">🌐</span>` : ''}
<span class="time">${esc(fmtTime(a.detected_at))}</span>
<div class="desc">${esc(a.description || a.type)}</div>
${showTr ? `<div class="trans">🌐 ${esc(tr.text)}</div>` : ''}
</div>`;
}).join('');
if (!skipAuto) autoTranslateVisible();
}

function focusIdx(i) {
const a = visibleList[i];
if (!a) return;
if (hasGeo(a)) map.flyTo({ center: [Number(a.lon), Number(a.lat)], zoom: 6, duration: 1000 });
else toast('Esta alerta no tiene coordenadas');
}

/* ---------- errores + estado de fuentes ---------- */
function renderErrors() {
const c = document.getElementById('errList');
let html = '';
if (allErrors.length === 0) html += '<div class="loading" style="color:var(--green);padding:14px">✓ SIN ERRORES</div>';
else html += allErrors.map(e => `<div class="error-banner">⚠ ${esc(e.message)} — ${esc(fmtTime(e.timestamp))}</div>`).join('');
const names = Object.keys(allStatus).sort();
if (names.length) {
const colors = { ok: 'var(--green)', warn: 'var(--orange)', error: 'var(--red)', skipped: 'var(--dim)' };
html += '<div class="chart-box"><h3>ESTADO DE FUENTES</h3>' + names.map(n => {
const s = allStatus[n] || {};
return `<div class="st-row"><span class="dot" style="background:${colors[s.status] || 'var(--dim)'}"></span>
<span class="nm">${esc(n)}</span><span class="meta">${s.disabled ? 'desactivada' : esc(s.status)} · ${s.count || 0} · ${s.ms || 0}ms</span>
<button class="btn" data-n="${esc(n)}" data-d="${s.disabled ? 1 : 0}" onclick="toggleSource(this.dataset.n, this.dataset.d === '1')">${s.disabled ? 'ACTIVAR' : 'DESACTIVAR'}</button></div>
${s.error ? `<div class="meta" style="padding:2px 0 4px 14px">${esc(s.error)}</div>` : ''}`;
}).join('') + '</div>';
}
c.innerHTML = html;
}

/* ---------- gráficos ---------- */
let chartInstance = null;
function updateCharts(stats) {
document.getElementById('totalAlerts').textContent = stats.total || 0;
document.getElementById('criticalAlerts').textContent = stats.critical || 0;
const labels = Object.keys(stats.by_source || {});
const data = Object.values(stats.by_source || {});
const palette = ['#ff3344','#ff8800','#4488ff','#00ff88','#aa66ff','#ffcc00','#00ccff','#ff66aa'];
const cols = (labels.length ? labels : ['Sin datos']).map((_, i) => palette[i % palette.length]);
if (chartInstance) {
chartInstance.data.labels = labels.length ? labels : ['Sin datos'];
chartInstance.data.datasets[0].data = data.length ? data : [0];
chartInstance.data.datasets[0].backgroundColor = cols;
chartInstance.update();
return;
}
const ctx = document.getElementById('chartSources').getContext('2d');
chartInstance = new Chart(ctx, {
type: 'bar',
data: { labels: labels.length ? labels : ['Sin datos'], datasets: [{ data: data.length ? data : [0], backgroundColor: cols }] },
options: { responsive: true, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#888', font: { size: 9 } } }, y: { ticks: { color: '#888' }, beginAtZero: true } } }
});
}

/* ---------- capa de alertas en el mapa ---------- */
function popupHtml(p) {
return `<div style="color:#111;font-family:'Courier New',monospace;font-size:11px;max-width:240px"><b>${esc(p.source)}</b><br>${esc(p.desc)}${safeUrl(p.link) ? `<br><a href="${esc(safeUrl(p.link))}" target="_blank" rel="noopener">abrir fuente ↗</a>` : ''}</div>`;
}
function sevCss(sev) { return sev === 'critical' ? '#ff3344' : sev === 'high' ? '#ff8800' : '#00ff88'; }

function initOverlay() {
if (overlay || overlayFailed) return;
try {
if (!window.deck || !deck.MapboxOverlay) throw new Error('deck.MapboxOverlay no disponible');
overlay = new deck.MapboxOverlay({
interleaved: false,
layers: [],
getTooltip: ({ object }) => object ? { html: `<b>${esc(object.source)}</b><br>${esc(object.desc)}`, style: { background: '#111', border: '1px solid #ff3344', color: '#e0e0e0' } } : null,
onClick: info => {
if (info && info.object) new maplibregl.Popup({ closeButton: true }).setLngLat(info.object.coordinates).setHTML(popupHtml(info.object)).addTo(map);
}
});
map.addControl(overlay);
} catch (e) {
console.warn('[MAP] deck.gl overlay no disponible, uso marcadores:', e);
overlayFailed = true; overlay = null;
}
}

function updateMapFallback(points) {
fbMarkers.forEach(m => m.remove()); fbMarkers = [];
points.slice(0, 500).forEach(p => {
const el = document.createElement('div');
el.style.cssText = 'width:12px;height:12px;border-radius:50%;border:1px solid #000;cursor:pointer;background:' + (p.grp === 'explosion' ? '#aa66ff' : sevCss(p.sev));
if (p.emoji) { el.textContent = p.emoji; el.style.cssText = 'font-size:20px;line-height:20px;cursor:pointer;'; }
const m = new maplibregl.Marker({ element: el }).setLngLat(p.coordinates)
.setPopup(new maplibregl.Popup({ offset: 10 }).setHTML(popupHtml(p))).addTo(map);
fbMarkers.push(m);
});
}

function updateMap() {
if (!map) return;
if (!mapReady) { mapPending = true; return; }
const points = getVisible().filter(hasGeo).map(a => ({ coordinates: [Number(a.lon), Number(a.lat)], source: a.source, desc: a.description, sev: a.severity, link: a.link, grp: typeGroup(a), emoji: a.emoji }));
initOverlay();
if (!overlay) { updateMapFallback(points); return; }
const layer = new deck.ScatterplotLayer({
id: 'alerts', data: points,
getPosition: d => d.coordinates,
getRadius: d => d.sev === 'critical' ? 150000 : 80000,
getFillColor: d => d.grp === 'explosion' ? [170, 102, 255, 230] : d.sev === 'critical' ? [255, 51, 68, 220] : d.sev === 'high' ? [255, 136, 0, 200] : [0, 255, 136, 180],
radiusMinPixels: 5, radiusMaxPixels: 30, pickable: true
});
const layers = [layer];
if (deck.TextLayer) {
layers.push(new deck.TextLayer({ id: 'emoji', data: points.filter(p => p.emoji), getPosition: d => d.coordinates, getText: d => d.emoji,
getSize: 22, sizeUnits: 'pixels', getTextAnchor: 'middle', getAlignmentBaseline: 'center', pickable: false,
fontFamily: 'Apple Color Emoji, Segoe UI Emoji, Noto Color Emoji, sans-serif', characterSet: 'auto' }));
}
overlay.setProps({ layers });
}

/* ---------- pestañas ---------- */
document.querySelectorAll('.tab').forEach(t => {
t.addEventListener('click', (e) => {
const name = e.currentTarget.dataset.tab;
document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
document.querySelectorAll('.content').forEach(x => x.classList.remove('active'));
e.currentTarget.classList.add('active');
document.getElementById('tab-' + name).classList.add('active');
if (name === 'search') tgLoadChannels();
if (name === 'charts') loadStats();
});
});

/* ---------- Telegram: buscador y canales ---------- */
async function tgLoadChannels() {
try {
const r = await fetch('/api/telegram/channels');
const j = await r.json();
const list = j.channels || [];
tgFollowing = new Set(list.filter(c => c.enabled).map(c => c.id.toLowerCase()));
const c = document.getElementById('tgChannels');
c.innerHTML = list.map(ch => `
<div class="st-row">
<span class="dot" style="background:${ch.enabled ? 'var(--green)' : 'var(--dim)'}"></span>
<span class="nm">${esc(ch.name)} <span class="meta">@${esc(ch.id)}${ch.custom ? ' · añadido' : ''}${ch.keywords_only === false ? ' · todo' : ''}</span></span>
<button class="btn" data-u="${esc(ch.id)}" onclick="tgPreview(this.dataset.u)">VER</button>
${ch.enabled
? `<button class="btn btn-red" data-u="${esc(ch.id)}" onclick="tgUnfollow(this.dataset.u)">QUITAR</button>`
: `<button class="btn" data-u="${esc(ch.id)}" data-n="${esc(ch.name)}" onclick="tgFollow(this.dataset.u, this.dataset.n)">ACTIVAR</button>`}
</div>`).join('') || '<div class="loading">Sin canales</div>';
if (tgLastResults.length) tgRenderResults([]);
} catch (e) {
document.getElementById('tgChannels').innerHTML = `<div class="error-banner">${esc(e.message)}</div>`;
}
}

async function tgSearch() {
const q = document.getElementById('tgQuery').value.trim();
if (q.length < 2) { toast('Escribe al menos 2 caracteres'); return; }
const box = document.getElementById('tgResults');
box.innerHTML = '<div class="loading"><div class="spinner"></div>BUSCANDO...</div>';
try {
const r = await fetch('/api/telegram/search?q=' + encodeURIComponent(q));
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
tgLastResults = j.results || [];
tgRenderResults(j.notes || []);
} catch (e) {
box.innerHTML = `<div class="error-banner">❌ ${esc(e.message)}</div>`;
}
}

function tgRenderResults(notes) {
const box = document.getElementById('tgResults');
let html = (notes || []).map(n => `<div class="hint" style="color:var(--orange)">ℹ ${esc(n)}</div>`).join('');
if (!notes.length && box.dataset.notes) html = box.dataset.notes;
box.dataset.notes = html;
if (!tgLastResults.length) { box.innerHTML = html || '<div class="loading">Sin resultados</div>'; return; }
html += tgLastResults.map(r => {
const following = tgFollowing.has((r.username || '').toLowerCase());
const kind = r.kind && r.kind !== 'unknown' ? `<span class="sevtag">${esc(r.kind)}</span>` : '';
const members = r.members ? `<span class="sevtag">${esc(r.members)} miembros</span>` : '';
return `<div class="tg-item">
<div class="tg-title">${esc(r.title)} ${kind}${members}</div>
<div class="tg-user">@${esc(r.username)} · ${esc((r.via || []).join(', '))}</div>
<div class="tg-desc">${esc(r.snippet || '')}</div>
<div class="tg-btns">
<button class="btn btn-blue" data-u="${esc(r.username)}" onclick="tgPreview(this.dataset.u)">VER</button>
${following
? `<button class="btn active" data-u="${esc(r.username)}" onclick="tgUnfollow(this.dataset.u)">✓ SIGUIENDO</button>`
: `<button class="btn" data-u="${esc(r.username)}" data-n="${esc(r.title)}" onclick="tgFollow(this.dataset.u, this.dataset.n)">＋ SEGUIR</button>`}
<a class="btn" href="${esc(safeUrl(r.url))}" target="_blank" rel="noopener">↗ t.me</a>
</div></div>`;
}).join('');
box.innerHTML = html;
}

async function tgFollow(u, name) {
try {
const r = await fetch('/api/telegram/channels', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: u, name: name }) });
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
toast('Siguiendo @' + u + (j.warning ? ' (' + j.warning + ')' : ''));
await tgLoadChannels();
tgRenderResults([]);
loadData(true);
} catch (e) { toast('Error: ' + e.message); }
}

async function tgUnfollow(u) {
try {
const r = await fetch('/api/telegram/channels/' + encodeURIComponent(u), { method: 'DELETE' });
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
toast('Quitado @' + u);
await tgLoadChannels();
tgRenderResults([]);
loadData(true);
} catch (e) { toast('Error: ' + e.message); }
}

async function tgPreview(u, q) {
tgPvUser = u;
const box = document.getElementById('tgPreviewBox');
box.style.display = 'block';
box.innerHTML = '<div class="loading"><div class="spinner"></div>CARGANDO @' + esc(u) + '...</div>';
box.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
try {
const r = await fetch('/api/telegram/preview?channel=' + encodeURIComponent(u) + (q ? '&q=' + encodeURIComponent(q) : ''));
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
const counters = Object.entries(j.counters || {}).map(([k, v]) => `<span class="badge">${esc(v)} ${esc(k)}</span>`).join(' ');
box.innerHTML = `<h3>VISTA PREVIA — @${esc(j.username)}</h3>
<div class="tg-title">${esc(j.title)}</div>
<div class="tg-desc">${esc(j.description)}</div>
<div class="frow" style="margin-bottom:8px">${counters}</div>
<div class="frow" style="margin-bottom:8px">
<input id="tgPvQ" class="fi" type="text" placeholder="buscar dentro del canal..." value="${esc(q || '')}" onkeydown="if(event.key==='Enter')tgPvSearch()">
<button class="btn" style="flex:0 0 auto" onclick="tgPvSearch()">BUSCAR EN CANAL</button>
<button class="btn" style="flex:0 0 auto" onclick="translatePreview(true)" title="Traducir mensajes">🌐</button>
<button class="btn" style="flex:0 0 auto" onclick="document.getElementById('tgPreviewBox').style.display='none'">✕</button>
</div>
${j.note ? `<div class="hint" style="color:var(--orange)">${esc(j.note)}</div>` : ''}
${(j.messages || []).map((m, mi) => `<div class="tg-msg"><span class="tm">${esc(fmtDateTime(m.datetime))}</span>${m.views ? ` <span class="meta">👁 ${esc(m.views)}</span>` : ''}${safeUrl(m.link) ? ` <a class="lnk" href="${esc(safeUrl(m.link))}" target="_blank" rel="noopener">↗</a>` : ''}<br>${esc(m.text)}<div class="trans" id="pvtr-${mi}"></div></div>`).join('')}`;
pvMsgs = j.messages || [];
if (tradOn) translatePreview();
} catch (e) {
box.innerHTML = `<div class="error-banner">❌ ${esc(e.message)}</div>`;
}
}

function tgPvSearch() {
const q = (document.getElementById('tgPvQ').value || '').trim();
if (tgPvUser) tgPreview(tgPvUser, q);
}

/* ---------- v2.3: traducción ---------- */
const CRED_LABELS = { A: 'Oficial / instrumental', B: 'Prensa / análisis / OSINT', C: 'Parte implicada (afirmación)', D: 'No verificado' };
let tradOn = false, trBackoff = 0, pvMsgs = [], lastStmt = 0, lastSpecial = [];
const trCache = new Map(), trPending = new Set(), trTried = new Set();
function trLang() { return document.getElementById('tradLang').value || 'es'; }
function trKey(t) { return trLang() + '|' + t; }
function needsTr(a) {
const t = a.type || '', txt = (a.description || '') + ' ' + (a.region || '');
if (['telegram_signal', 'news', 'war_news', 'official_statement', 'state_media'].includes(t)) return true;
return /[\u0400-\u04FF\u0590-\u05FF\u0600-\u06FF\u4E00-\u9FFF]/.test(txt);
}
async function translateTexts(texts, force) {
const todo = Array.from(new Set(texts)).filter(t => t && !trPending.has(trKey(t)) && (force || (!trCache.has(trKey(t)) && !trTried.has(trKey(t)))));
if (!todo.length || (!force && Date.now() < trBackoff)) return 0;
let okCount = 0;
for (let i = 0; i < todo.length; i += 20) {
const chunk = todo.slice(i, i + 20);
chunk.forEach(t => { trPending.add(trKey(t)); trTried.add(trKey(t)); });
try {
const r = await fetch('/api/translate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ texts: chunk, target: trLang() }) });
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
(j.translations || []).forEach((tr, idx) => { if (tr && tr.ok) { trCache.set(trKey(chunk[idx]), tr); okCount++; } });
} catch (e) {
trBackoff = Date.now() + 30000;
toast('Traductor: ' + e.message);
} finally {
chunk.forEach(t => trPending.delete(trKey(t)));
}
}
return okCount;
}
async function autoTranslateVisible() {
if (!tradOn) return;
const texts = visibleList.slice(0, 60).filter(needsTr).map(a => a.description).filter(Boolean);
const n = await translateTexts(texts, false);
if (n > 0) renderFeed(true);
}
function toggleTrad() {
tradOn = !tradOn; trTried.clear(); trBackoff = 0;
const b = document.getElementById('tradBtn');
b.textContent = tradOn ? '🌐 TRAD: ON' : '🌐 TRAD: OFF';
b.classList.toggle('active', tradOn);
renderFeed();
if (tradOn && pvMsgs.length) translatePreview();
toast(tradOn ? 'Traducción automática activada' : 'Traducción desactivada');
}
function onTradLang() { trTried.clear(); trBackoff = 0; renderFeed(); updateStmtBanner(lastSpecial); if (tradOn && pvMsgs.length) translatePreview(); }
async function tradOne(i) {
const a = visibleList[i];
if (!a || !a.description) return;
const n = await translateTexts([a.description], true);
if (n === 0) toast('No se pudo traducir (traductor no disponible)');
renderFeed(true);
}
async function translatePreview(force) {
if (!pvMsgs.length) return;
const n = await translateTexts(pvMsgs.map(m => m.text), !!force);
pvMsgs.forEach((m, mi) => {
const tr = trCache.get(trKey(m.text));
const el = document.getElementById('pvtr-' + mi);
if (el && tr && tr.text && !tr.same) el.innerHTML = '🌐 ' + esc(tr.text);
});
if (force && n === 0) toast('No se pudo traducir');
}

/* ---------- v2.3: banner de comunicados especiales ---------- */
function updateStmtBanner(list) {
lastSpecial = list || [];
const b = document.getElementById('stmtBanner');
if (!lastSpecial.length) { b.classList.remove('show'); return; }
const s0 = lastSpecial[0];
const tr = trCache.get(trKey(s0.description || ''));
const txt = (tr && tr.text) ? tr.text : s0.description;
b.textContent = `📜 COMUNICADO ESPECIAL (${lastSpecial.length}) — ${s0.source}: ${txt}`;
b.classList.add('show');
if (!tr) translateTexts([s0.description], false).then(n => { if (n > 0) updateStmtBanner(lastSpecial); });
}
function openStatements() {
document.getElementById('fType').value = 'comunicado';
document.getElementById('fRange').value = 'live';
document.querySelector('.tab[data-tab="feed"]').click();
onFilterChange();
}

/* ---------- v2.3: estadísticas y telemetría ---------- */
const charts = {};
let statsLoading = false;
const AX = { ticks: { color: '#888', font: { size: 9 } }, grid: { color: '#222' } };
function mkChart(id, cfg) {
const el = document.getElementById(id);
if (!el) return;
if (charts[id]) { charts[id].data = cfg.data; charts[id].options = cfg.options; charts[id].update(); return; }
charts[id] = new Chart(el.getContext('2d'), cfg);
}
function barCfg(labels, data, color, horiz) {
return { type: 'bar', data: { labels, datasets: [{ data, backgroundColor: color }] },
options: { indexAxis: horiz ? 'y' : 'x', responsive: true, plugins: { legend: { display: false } },
scales: { x: Object.assign({ beginAtZero: true }, AX), y: Object.assign({ beginAtZero: true }, AX) } } };
}
function donutCfg(labels, data, colors) {
return { type: 'doughnut', data: { labels, datasets: [{ data, backgroundColor: colors, borderColor: '#111' }] },
options: { responsive: true, plugins: { legend: { position: 'right', labels: { color: '#aaa', font: { size: 10 } } } } } };
}
function fmtDur(sec) {
sec = Math.floor(sec || 0);
const d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60);
return (d ? d + 'd ' : '') + (h ? h + 'h ' : '') + m + 'm';
}
async function loadStats() {
if (statsLoading) return;
statsLoading = true;
try {
const h = document.getElementById('stRange').value;
const r = await fetch('/api/stats?hours=' + h);
if (!r.ok) throw new Error('HTTP ' + r.status);
renderStats(await r.json());
} catch (e) {
toast('Estadísticas: ' + e.message);
} finally {
statsLoading = false;
}
}
function renderStats(s) {
const act = s.activity || {}, tel = s.telemetry || {};
const srcs = Object.values(tel.sources || {});
const okN = srcs.filter(x => x.status === 'ok').length;
const idx = act.index || 0;
const col = idx >= 60 ? 'var(--red)' : idx >= 30 ? 'var(--orange)' : 'var(--green)';
document.getElementById('kpis').innerHTML = [
['Índice de actividad*', idx + '/100', col],
['Alertas en rango', s.rows || 0, 'var(--text)'],
['Críticas (30 min)', act.critical_30m || 0, 'var(--red)'],
['Posibles explosiones (2h)', act.explosions_2h || 0, 'var(--purple)'],
['Comunicados graves (6h)', act.statements_6h || 0, 'var(--yellow)'],
['Fuentes OK', okN + '/' + srcs.length, 'var(--green)'],
].map(([l, v, c]) => `<div class="kpi"><div class="kv" style="color:${c}">${esc(v)}</div><div class="kl">${esc(l)}</div></div>`).join('')
+ '<div class="hint" style="grid-column:1/-1">* Índice heurístico (críticas, explosiones candidatas y comunicados recientes). No es una predicción.</div>';

const tl = s.timeline || [];
const tlLabels = tl.map(t => t.h.slice(5).replace('T', ' ') + 'h');
const ds = (k, c) => ({ label: k, data: tl.map(t => t[k]), backgroundColor: c });
mkChart('chartTimeline', { type: 'bar', data: { labels: tlLabels, datasets: [ds('critical', '#ff3344'), ds('high', '#ff8800'), ds('medium', '#ffcc00'), ds('low', '#4488ff')] },
options: { responsive: true, plugins: { legend: { labels: { color: '#aaa', font: { size: 10 } } } },
scales: { x: Object.assign({ stacked: true }, AX, { ticks: { color: '#888', font: { size: 9 }, maxTicksLimit: 10 } }), y: Object.assign({ stacked: true, beginAtZero: true }, AX) } } });

const sevK = ['critical', 'high', 'medium', 'low'];
mkChart('chartSeverity', donutCfg(sevK, sevK.map(k => (s.by_severity || {})[k] || 0), ['#ff3344', '#ff8800', '#ffcc00', '#4488ff']));
const types = Object.entries(s.by_type || {}).sort((a, b) => b[1] - a[1]).slice(0, 10);
mkChart('chartTypes', barCfg(types.map(x => x[0]), types.map(x => x[1]), '#4488ff', true));
const credK = ['A', 'B', 'C', 'D'];
mkChart('chartCred', donutCfg(credK.map(k => k + ' · ' + CRED_LABELS[k]), credK.map(k => (s.by_cred || {})[k] || 0), ['#00ff88', '#4488ff', '#ff8800', '#666666']));
const mg = s.magnitudes || [];
mkChart('chartMag', barCfg(mg.map(x => x.bin), mg.map(x => x.n), '#aa66ff', false));
document.getElementById('exList').innerHTML = (s.explosions || []).length
? (s.explosions || []).map(e => `<div class="ex-item">💥 ${esc(fmtDateTime(e.detected_at))} — ${esc(e.description)}</div>`).join('')
: '<div class="hint">Sin candidatos a explosión en el rango.</div>';
const rg = s.regions || [];
mkChart('chartRegions', barCfg(rg.map(x => x[0]), rg.map(x => x[1]), '#ff8800', true));

const words = s.words || [];
const wc = document.getElementById('wordCloud');
const mx = Math.max(1, ...words.map(w => w[1]));
wc.innerHTML = words.map(([w, n]) => `<span style="font-size:${10 + Math.round(14 * n / mx)}px;margin:2px 6px;display:inline-block;color:var(--blue)" title="${n}">${esc(w)}</span>`).join('') || '<div class="hint">Sin datos</div>';
if (words.length) {
const joined = words.map(w => w[0]).join(' · ');
translateTexts([joined], false).then(() => {
const tr = trCache.get(trKey(joined));
if (tr && tr.text && !tr.same) wc.insertAdjacentHTML('beforeend', '<div class="trans">🌐 ' + esc(tr.text) + '</div>');
});
}

const srcEntries = Object.entries(tel.sources || {}).sort((a, b) => (b[1].ms || 0) - (a[1].ms || 0));
const stCol = { ok: '#00ff88', warn: '#ff8800', error: '#ff3344', skipped: '#666666' };
mkChart('chartLatency', barCfg(srcEntries.map(x => x[0]), srcEntries.map(x => x[1].ms || 0), srcEntries.map(x => stCol[x[1].status] || '#666'), true));
const f = tel.fetches || [];
mkChart('chartFetch', { type: 'line', data: { labels: f.map(x => fmtTime(x.ts)), datasets: [
{ label: 'ms', data: f.map(x => x.ms), borderColor: '#ff8800', backgroundColor: '#ff8800', tension: 0.3, yAxisID: 'y' },
{ label: 'alertas', data: f.map(x => x.total), borderColor: '#00ff88', backgroundColor: '#00ff88', tension: 0.3, yAxisID: 'y1' }] },
options: { responsive: true, plugins: { legend: { labels: { color: '#aaa', font: { size: 10 } } } },
scales: { x: Object.assign({}, AX, { ticks: { color: '#888', font: { size: 9 }, maxTicksLimit: 6 } }), y: Object.assign({ beginAtZero: true, position: 'left' }, AX), y1: Object.assign({ beginAtZero: true, position: 'right' }, AX, { grid: { drawOnChartArea: false } }) } } });

const rows = [
['Versión', tel.version], ['Tiempo activo', fmtDur(tel.uptime_s)], ['Peticiones HTTP', tel.requests],
['Alertas en BD', tel.db_rows], ['Tamaño BD', (tel.db_kb || 0) + ' KB'],
['Edad de caché', tel.cache_age_s == null ? '—' : tel.cache_age_s + ' s'],
['Traductores', (tel.translators || []).join(', ')], ['Telethon', tel.telethon ? 'activo' : 'no configurado'],
];
document.getElementById('sysTel').innerHTML = rows.map(([k, v]) => `<div class="st-row"><span class="nm">${esc(k)}</span><span class="meta">${esc(v)}</span></div>`).join('');
}

/* ---------- v2.4: resumen, avisos de escritorio, diagnóstico, interruptores de fuentes ---------- */
let notifyOn = false, lastExpl = 0, firstData = true, lastDiag = null;
function renderSummary(stats) {
const now = Date.now();
const recent = allAlerts.filter(a => now - new Date(a.detected_at).getTime() <= 6 * 3600 * 1000);
const byPlace = {};
recent.filter(a => ['alerta', 'telegram'].includes(typeGroup(a))).forEach(a => { const p = a.place || a.region || '?'; byPlace[p] = (byPlace[p] || 0) + 1; });
const top = Object.entries(byPlace).sort((x, y) => y[1] - x[1]).slice(0, 4).map(([p, n]) => `${esc(p)} (${n})`).join(', ');
const cnt = g => recent.filter(a => typeGroup(a) === g).length;
const crit = stats.critical_recent || 0;
const st = stats.source_status || {};
const names = Object.keys(st);
const okN = names.filter(n => st[n].status === 'ok').length;
const offN = names.filter(n => st[n].status === 'error').length;
const sp = (stats.special_statements || []).length;
const lines = [
`${crit > 0 ? '🚨' : '✅'} <b>${crit}</b> alertas críticas en los últimos 30 min`,
top ? `📍 Más actividad (alertas + Telegram, 6h): ${top}` : '📍 Sin actividad geolocalizada en las últimas 6 h',
`💥 Posibles explosiones sísmicas (6h): <b>${stats.explosions || 0}</b> · 🌍 sismos relevantes: ${cnt('sismo')}`,
`📜 Comunicados oficiales (6h): <b>${cnt('comunicado')}</b>${sp ? ' · <span style="color:var(--yellow)">' + sp + ' grave(s)</span>' : ''}`,
`🗞 Noticias de guerra: ${cnt('noticia')} · 🏛 estatales (versión de parte): ${cnt('estatal')} · 📡 Telegram sin verificar: ${cnt('telegram')}`,
`🔌 Fuentes: ${okN} OK · ${offN} caídas de ${names.length}${offN ? ' — mira ⚠ ERRORES › 🩺 DIAGNÓSTICO' : ''}`,
];
document.getElementById('summaryBody').innerHTML = lines.map(l => `<div style="margin:3px 0">${l}</div>`).join('');
}
async function toggleNotify() {
if (!('Notification' in window)) { toast('Este navegador no soporta notificaciones'); return; }
if (!notifyOn) {
const p = await Notification.requestPermission();
if (p !== 'granted') { toast('Permiso de notificaciones denegado'); return; }
}
notifyOn = !notifyOn;
document.getElementById('notifyBtn').textContent = notifyOn ? '🔔 AVISOS ON' : '🔔 AVISOS OFF';
document.getElementById('notifyBtn').classList.toggle('active', notifyOn);
toast(notifyOn ? 'Avisos de escritorio activados' : 'Avisos desactivados');
}
function notify(title, body) {
if (!notifyOn || firstData || !('Notification' in window) || Notification.permission !== 'granted') return;
try { new Notification(title, { body: String(body || '').slice(0, 180) }); } catch (e) { /* ignorar */ }
}
async function toggleSource(name, wasDisabled) {
try {
const r = await fetch('/api/sources/toggle', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, enabled: wasDisabled }) });
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
toast((wasDisabled ? 'Activada: ' : 'Desactivada: ') + name);
loadData(true);
} catch (e) { toast('Error: ' + e.message); }
}
async function runDiagnostics() {
const box = document.getElementById('diagBox');
box.innerHTML = '<div class="loading"><div class="spinner"></div>PROBANDO TODOS LOS FEEDS (≈30 s)...</div>';
try {
const r = await fetch('/api/diagnostics?run=1');
const j = await r.json();
if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
lastDiag = j; renderDiag(j);
} catch (e) { box.innerHTML = `<div class="error-banner">❌ ${esc(e.message)}</div>`; }
}
function renderDiag(j) {
const rows = j.results || [];
const okN = rows.filter(r => r.ok).length;
const groups = {};
rows.forEach(r => { (groups[r.group] = groups[r.group] || []).push(r); });
let html = `<div class="chart-box"><h3>DIAGNÓSTICO — ${okN}/${rows.length} FUENTES RESPONDEN</h3>`;
Object.keys(groups).forEach(g => {
html += `<div class="hint" style="margin-top:8px;color:var(--yellow)">${esc(g)}</div>`;
groups[g].forEach(r => {
html += `<div class="st-row"><span class="dot" style="background:${r.ok ? 'var(--green)' : 'var(--red)'}"></span><span class="nm">${esc(r.name)}</span><span class="meta">${r.ok ? esc((r.items != null ? r.items + ' items · ' : '') + r.ms + 'ms') : esc(r.error || 'error')}</span></div>`;
});
});
document.getElementById('diagBox').innerHTML = html + '</div>';
}
function exportDiag() { window.location.href = '/api/diagnostics?format=txt'; }

function refresh(force) { loadData(!!force); }

window.addEventListener('DOMContentLoaded', () => {
loadData(false);
startAutoRefresh();
setInterval(() => { if (document.getElementById('tab-charts').classList.contains('active')) loadStats(); }, 30000);
});
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return HTML_PANEL


def is_port_free(host: str, port: int) -> bool:
    """True si se puede enlazar host:port (no hay otro programa usándolo)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if os.name == "posix":  # en Windows SO_REUSEADDR permitiría pisar un puerto en uso
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def find_free_port(host: str, start: int, tries: int = 50) -> int:
    """Devuelve 'start' si está libre; si no, el siguiente libre; si no hay, uno asignado por el SO."""
    for p in range(start, start + tries):
        if p <= 65535 and is_port_free(host, p):
            return p
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, 0))
        return s.getsockname()[1]
    finally:
        s.close()


init_db()



# ===== EXTENSIÓN v2.5 =====
# -*- coding: utf-8 -*-
# ============================================================
# GLOBAL EARLY WARNING OSINT MONITOR — v2.5 (extensión sobre v2.4, sin tocar el original)
#
# USO:
#   1) Guarda tu script v2.4 como  osint_monitor.py  (o define OSINT_BASE_MODULE=nombre_sin_.py)
#   2) Deja este archivo en la MISMA carpeta y ejecútalo:   python osint_monitor_v25.py
#
# Añade:
#   - Cruce de fuentes (incidentes por proximidad espacio-temporal + fuentes independientes + confianza)
#   - Tooltip detallado al pasar el ratón por cualquier punto (+ enlaces entre reportes cruzados, mapa de calor)
#   - Informe automatizado (HTML / Markdown / JSON), guardado periódico y descarga ZIP completa
#   - Gráficos nuevos: radar, polar, burbujas, dispersión, áreas apiladas, histórico del índice, matriz de calor
#   - Grupos de canales de Telegram (crear, activar/desactivar en bloque) y filtro por grupo/categoría
#   - Muchas más fuentes (gobiernos, organismos, prensa, OSINT, estatales, GDACS, GDELT DOC, FIRMS abierto,
#     OpenSky, NewsAPI opcional) con caché por fuente para no ser bloqueado
#   - Lista de vigilancia (palabras clave + zonas geográficas) y notificaciones externas
#     (NOTIFY_WEBHOOK_URL, NTFY_URL, TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID)
#
# Variables opcionales: REPORT_INTERVAL_MIN (60, 0=off), REPORT_DIR (reports), REPORT_KEEP (72),
#   CLUSTER_KM (120), CLUSTER_MIN (90), NEWSAPI_KEY, OPENSKY_USER/OPENSKY_PASS
# ============================================================
import os
import re
import io
import sys
import csv
import copy
import json
import math
import time
import zipfile
import sqlite3
import hashlib
import logging
import importlib
import threading
import statistics
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone, timedelta
from html import escape as hesc
from typing import Any, Dict, List, Optional, Tuple

BASE_MODULE = "GEWOM"
base = sys.modules[__name__]

from flask import jsonify, request, Response, send_from_directory

app = base.app
log = logging.getLogger("v25")
VERSION = "2.5"
base.APP_VERSION = VERSION

CLUSTER_KM = float(os.getenv("CLUSTER_KM", "120"))
CLUSTER_MIN = float(os.getenv("CLUSTER_MIN", "90"))
CORR_WINDOW_H = float(os.getenv("CORR_WINDOW_H", "6"))
REPORT_INTERVAL_MIN = float(os.getenv("REPORT_INTERVAL_MIN", "60"))
REPORT_DIR = os.getenv("REPORT_DIR", "reports")
REPORT_KEEP = int(os.getenv("REPORT_KEEP", "72"))
NOTIFY_WEBHOOK_URL = os.getenv("NOTIFY_WEBHOOK_URL", "")
NTFY_URL = os.getenv("NTFY_URL", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASS = os.getenv("OPENSKY_PASS", "")
WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "watchlist.json")
TG_GROUPS_FILE = os.getenv("TG_GROUPS_FILE", "telegram_groups.json")

LEVEL = base._LEVEL_ORDER
CRED_W = {"A": 1.0, "B": 0.7, "C": 0.4, "D": 0.2}

# ------------------------------------------------------------
# Categorías (grupos) de alertas
# ------------------------------------------------------------
GROUP_OF_TYPE = {
    "missile_alert": "Alertas", "air_raid": "Alertas", "kyiv_alert": "Alertas", "drone_missile_track": "Alertas",
    "earthquake": "Sísmico", "seismic_explosion": "Sísmico", "explosion_candidate": "Sísmico",
    "nuclear_test_candidate": "Sísmico", "thermal_anomaly": "Satélite", "natural_event": "Naturales",
    "disaster": "Naturales", "news": "Prensa/OSINT", "news_geo": "Prensa/OSINT", "war_news": "Prensa/OSINT",
    "official_statement": "Gobierno", "state_media": "Estatal", "telegram_signal": "Telegram",
    "acled_conflict": "Conflicto", "conflict_event": "Conflicto", "flight_activity": "Aviación",
}
TEXT_TYPES = {"telegram_signal", "news", "war_news", "state_media", "official_statement", "news_geo"}


def alert_group(a: dict) -> str:
    return GROUP_OF_TYPE.get(a.get("type"), "Otros")


def _age(a: dict) -> float:
    return base.age_seconds(a.get("detected_at", ""))


def hasgeo(a: dict) -> bool:
    la, lo = base.to_float(a.get("lat")), base.to_float(a.get("lon"))
    return la is not None and lo is not None and not (la == 0 and lo == 0)


# ============================================================
# 1) MÁS FUENTES (listas existentes + nuevos fetchers)
# ============================================================
def _extend(lst: list, items: list):
    have = {str(x[0]).lower() for x in lst}
    for it in items:
        if str(it[0]).lower() not in have:
            lst.append(it)
            have.add(str(it[0]).lower())


# Noticias generales filtradas por palabras de ataque
_extend(base.RSS_FEEDS, [
    ("BBC Oriente Medio", "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml"),
    ("BBC Europa", "https://feeds.bbci.co.uk/news/world/europe/rss.xml"),
    ("Guardian Ucrania", "https://www.theguardian.com/world/ukraine/rss"),
    ("Guardian Mundo", "https://www.theguardian.com/world/rss"),
    ("DW", "https://rss.dw.com/rdf/rss-en-all"),
    ("France 24", "https://www.france24.com/en/rss"),
    ("Sky News Mundo", "https://feeds.skynews.com/feeds/rss/world.xml"),
    ("CNN Mundo", "http://rss.cnn.com/rss/edition_world.rss"),
    ("NYT Mundo", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("NPR Mundo", "https://feeds.npr.org/1004/rss.xml"),
    ("Jerusalem Post", "https://www.jpost.com/rss/rssfeedsheadlines.aspx"),
    ("Euronews", "https://www.euronews.com/rss?format=mrss"),
])
# Prensa de guerra / OSINT / think tanks (nombre, url, credibilidad, filtrar)
_extend(base.WAR_FEEDS, [
    ("Militarnyi", "https://militarnyi.com/en/feed/", "B", True),
    ("USNI News", "https://news.usni.org/feed", "B", True),
    ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml", "B", True),
    ("Jamestown Foundation", "https://jamestown.org/feed/", "B", True),
    ("The Insider (RU)", "https://theins.ru/en/feed", "B", True),
    ("CSIS análisis", "https://www.csis.org/analysis/feed", "B", True),
    ("Politico Europe", "https://www.politico.eu/feed/", "B", True),
    ("The Diplomat", "https://thediplomat.com/feed/", "B", True),
    ("Middle East Eye", "https://www.middleeasteye.net/rss", "B", True),
    ("The New Arab", "https://www.newarab.com/rss", "B", True),
    ("Haaretz", "https://www.haaretz.com/srv/haaretz-latest-headlines", "B", True),
    ("ReliefWeb (ONU/OCHA)", "https://reliefweb.int/updates/rss.xml", "A", True),
    ("ONU Noticias — Oriente Medio", "https://news.un.org/feed/subscribe/en/news/region/middle-east/feed/rss.xml", "A", False),
    ("ONU Noticias — Europa", "https://news.un.org/feed/subscribe/en/news/region/europe/feed/rss.xml", "A", False),
])
# Comunicados de gobiernos y organismos (nombre, url, credibilidad, modo)
_extend(base.STATEMENT_FEEDS, [
    ("Ucrania Presidente (oficial)", "https://www.president.gov.ua/en/rss", "C", "gov"),
    ("Ucrania MoD (oficial)", "https://www.mod.gov.ua/en/news/feed", "C", "gov"),
    ("Ucrania MAE (oficial)", "https://mfa.gov.ua/en/rss", "C", "gov"),
    ("Rusia MAE (oficial)", "https://mid.ru/en/rss/", "C", "gov"),
    ("Reino Unido Downing St", "https://www.gov.uk/government/organisations/prime-ministers-office-10-downing-street.atom", "C", "gov"),
    ("EE.UU. Ejército (noticias)", "https://www.army.mil/rss/2/", "C", "gov"),
    ("EE.UU. Armada (noticias)", "https://www.navy.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=10", "C", "gov"),
    ("EE.UU. Fuerza Aérea", "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=10", "C", "gov"),
    ("UE Parlamento Europeo", "https://www.europarl.europa.eu/rss/doc/top-stories/en.xml", "C", "gov"),
    ("ONU Oriente Medio (comunicados)", "https://news.un.org/feed/subscribe/en/news/region/middle-east/feed/rss.xml", "A", "gov"),
    ("ONU Europa (comunicados)", "https://news.un.org/feed/subscribe/en/news/region/europe/feed/rss.xml", "A", "gov"),
    ("ReliefWeb (comunicados)", "https://reliefweb.int/updates/rss.xml", "A", "gov"),
])
_extend(base.STATE_MEDIA_FEEDS, [
    ("RIA Novosti EN (Rusia)", "https://en.ria.ru/export/rss2/archive/index.xml"),
    ("Mehr (Irán)", "https://en.mehrnews.com/rss"),
    ("Global Times (China)", "https://www.globaltimes.cn/rss/outbrain.xml"),
    ("BelTA (Bielorrusia)", "https://eng.belta.by/rss"),
])
base.TELEGRAM_CHANNELS.extend([c for c in [
    {"id": "V_Zelenskiy_official", "name": "Zelensky (oficial UA)", "lang": "uk"},
    {"id": "dsns_telegram", "name": "DSNS Emergencias UA (oficial)", "lang": "uk"},
    {"id": "mvs_ukraine", "name": "Interior UA (oficial)", "lang": "uk"},
    {"id": "nexta_live", "name": "NEXTA Live", "lang": "ru"},
    {"id": "meduzalive", "name": "Meduza Live", "lang": "ru"},
    {"id": "tass_agency", "name": "TASS (estatal RU)", "lang": "ru"},
    {"id": "rian_ru", "name": "RIA Novosti (estatal RU)", "lang": "ru"},
    {"id": "kyivindependent_official", "name": "Kyiv Independent", "lang": "en"},
    {"id": "RVvoenkor", "name": "Voenkor RV (RU)", "lang": "ru"},
    {"id": "dva_majors", "name": "Dva Majors (RU)", "lang": "ru"},
] if c["id"].lower() not in {x["id"].lower() for x in base.TELEGRAM_CHANNELS}])
base.TG_CRED.update({"v_zelenskiy_official": "C", "dsns_telegram": "C", "mvs_ukraine": "C", "tass_agency": "C",
                     "rian_ru": "C", "meduzalive": "B", "kyivindependent_official": "B", "nexta_live": "D"})
base.CRED_RULES[:0] = [("gdacs", "A"), ("opensky", "A"), ("firms", "A"), ("gdelt doc", "B"), ("newsapi", "B")]


def _nearest_place(lat: float, lon: float, maxkm: float = 60.0) -> Optional[Tuple[str, float]]:
    best = None
    for name, la, lo, _al in base.PLACES:
        d = base.haversine_km(lat, lon, la, lo)
        if d <= maxkm and (best is None or d < best[1]):
            best = (name, d)
    return best


def fetch_gdacs() -> List[dict]:
    """GDACS (ONU/UE): desastres con coordenadas (sin API)."""
    name = "GDACS"
    try:
        r = base.create_session().get("https://www.gdacs.org/xml/rss.xml", headers=base.get_headers(html=True), timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        now = datetime.now(timezone.utc)
        out = []
        for it in root.iter("item"):
            f: Dict[str, str] = {}
            for c in it:
                f.setdefault(c.tag.split("}")[-1], (c.text or "").strip())
            dt = base._parse_dt_any(f.get("pubDate"))
            if not dt or (now - dt).total_seconds() > 72 * 3600:
                continue
            pt = (f.get("point") or "").split()
            if len(pt) < 2:
                continue
            lat, lon = base.to_float(pt[0]), base.to_float(pt[1])
            if lat is None or lon is None:
                continue
            lvl = (f.get("alertlevel") or "").lower()
            sev = "high" if lvl == "red" else ("medium" if lvl == "orange" else "low")
            out.append({"source": "GDACS (ONU/UE)", "type": "disaster", "region": f.get("title", "")[:120],
                        "lat": lat, "lon": lon, "severity": sev, "description": f.get("title", "")[:200],
                        "detected_at": dt.isoformat(), "link": f.get("link"), "cred": "A",
                        "tags": [f.get("eventtype", "")] if f.get("eventtype") else [],
                        "raw": {"alertlevel": lvl, "eventtype": f.get("eventtype")}})
        return base._ok(name, out[:60])
    except Exception as e:
        return base._fail(name, e)


def fetch_gdelt_doc() -> List[dict]:
    """GDELT DOC 2.0 (público, sin key): artículos recientes sobre ataques/crisis en cualquier idioma."""
    name = "GDELT DOC"
    params = {"query": '(missile OR airstrike OR drone OR shelling OR ceasefire OR mobilization OR "state of emergency")',
              "mode": "artlist", "format": "json", "maxrecords": 60, "timespan": "3h", "sort": "datedesc"}
    try:
        r = base.create_session().get("https://api.gdeltproject.org/api/v2/doc/doc", params=params,
                                      headers=base.get_headers(), timeout=20)
        r.raise_for_status()
        arts = r.json().get("articles", [])
        out = []
        for a in arts[:60]:
            title = str(a.get("title") or "").strip()
            if not title:
                continue
            try:
                dt = datetime.strptime(a.get("seendate", ""), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                dt = base.now_iso()
            lvl, tags = base.classify_statement(title)
            loc = base.geolocate_text(title)
            out.append({"source": "GDELT DOC (público)", "type": "news", "region": loc[2] if loc else str(a.get("domain", "")),
                        "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
                        "severity": "high" if LEVEL[lvl] >= 2 else "low", "description": title[:220],
                        "detected_at": dt, "link": a.get("url"), "cred": "B", "tags": tags,
                        "raw": {"domain": a.get("domain"), "lang": a.get("language"), "country": a.get("sourcecountry")}})
        return base._ok(name, out)
    except Exception as e:
        return base._fail(name, e)


def fetch_firms_open() -> List[dict]:
    """NASA FIRMS CSV abierto de Europa (24 h, sin MAP_KEY): anomalías térmicas fuertes en el corredor del conflicto."""
    name = "FIRMS abierto"
    if base.NASA_FIRMS_KEY != "TU_MAP_KEY_AQUI":
        return base._skip(name, "Ya usas FIRMS con clave")
    url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv"
    try:
        r = base.create_session().get(url, headers=base.get_headers(), timeout=40)
        r.raise_for_status()
        rows = []
        for row in csv.DictReader(io.StringIO(r.text)):
            lat, lon = base.to_float(row.get("latitude")), base.to_float(row.get("longitude"))
            frp = base.to_float(row.get("frp"), 0.0) or 0.0
            if lat is None or lon is None or frp < base.FIRMS_MIN_FRP or str(row.get("confidence", "")).lower() in ("l", "low"):
                continue
            if not (44.0 <= lat <= 56.5 and 22.0 <= lon <= 46.0):
                continue
            rows.append((frp, lat, lon, row))
        rows.sort(key=lambda x: x[0], reverse=True)
        out = []
        for frp, lat, lon, row in rows[:100]:
            try:
                dt = datetime.strptime(f"{row.get('acq_date')} {str(row.get('acq_time', '0')).zfill(4)}", "%Y-%m-%d %H%M")
                det = dt.replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                det = base.now_iso()
            near = _nearest_place(lat, lon)
            out.append({"source": "NASA FIRMS (abierto)", "type": "thermal_anomaly",
                        "region": f"{near[0]} (~{near[1]:.0f} km)" if near else f"Lat {lat:.2f}, Lon {lon:.2f}",
                        "place": near[0] if near else "", "lat": lat, "lon": lon, "severity": "medium",
                        "description": f"Anomalía térmica (FRP={frp:.0f} MW) — puede ser incendio, industria o impacto",
                        "detected_at": det, "cred": "A", "raw": {"frp": frp, "sat": row.get("satellite")}})
        return base._ok(name, out)
    except Exception as e:
        return base._fail(name, e)


_OS_HIST: Dict[str, deque] = defaultdict(lambda: deque(maxlen=48))
OPENSKY_ZONES = [("Israel–Líbano–Siria", 29.0, 35.5, 33.0, 39.0), ("Polonia–Bálticos (frontera este)", 49.0, 57.0, 19.0, 29.0),
                 ("Irán–Golfo", 24.0, 40.0, 44.0, 64.0)]


def fetch_opensky() -> List[dict]:
    """OpenSky (sin clave, cuota baja): nº de aeronaves en vuelo; caídas bruscas = posible cierre de espacio aéreo."""
    name = "OpenSky"
    out = []
    errs = []
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER else None
    for zname, la0, la1, lo0, lo1 in OPENSKY_ZONES:
        try:
            r = base.create_session().get("https://opensky-network.org/api/states/all",
                                          params={"lamin": la0, "lamax": la1, "lomin": lo0, "lomax": lo1},
                                          headers=base.get_headers(), timeout=20, auth=auth)
            r.raise_for_status()
            states = r.json().get("states") or []
            n = sum(1 for s in states if len(s) > 8 and not s[8])
            hist = _OS_HIST[zname]
            med = statistics.median(hist) if len(hist) >= 6 else None
            hist.append(n)
            sev, extra = "low", ""
            if med and med >= 8 and n < 0.35 * med:
                sev, extra = "high", f" — caída del {100 - 100 * n / med:.0f}% frente a lo habitual: posible cierre/restricción de espacio aéreo (heurístico)"
            out.append({"source": "OpenSky (tráfico aéreo)", "type": "flight_activity", "region": zname,
                        "place": zname, "lat": (la0 + la1) / 2, "lon": (lo0 + lo1) / 2, "severity": sev,
                        "description": f"Tráfico aéreo {zname}: {n} aeronaves en vuelo" + (f" (mediana {med:.0f})" if med else "") + extra,
                        "detected_at": base.now_iso(), "cred": "A", "raw": {"count": n, "median": med}})
        except Exception as e:
            errs.append(f"{zname}: {str(e)[:60]}")
    if not out:
        return base._fail(name, "; ".join(errs) or "sin datos")
    if errs:
        return base._warn(name, out, "; ".join(errs))
    return base._ok(name, out)


def fetch_newsapi() -> List[dict]:
    name = "NewsAPI"
    if not NEWSAPI_KEY:
        return base._skip(name, "Sin NEWSAPI_KEY")
    try:
        r = base.create_session().get("https://newsapi.org/v2/everything", params={
            "q": "(missile OR airstrike OR drone OR shelling OR ceasefire OR mobilization OR nuclear) AND (Ukraine OR Russia OR Israel OR Iran OR Gaza OR NATO)",
            "language": "en", "sortBy": "publishedAt", "pageSize": 50, "apiKey": NEWSAPI_KEY}, timeout=15)
        r.raise_for_status()
        out = []
        for a in r.json().get("articles", []):
            title = str(a.get("title") or "")
            lvl, tags = base.classify_statement(title)
            loc = base.geolocate_text(title)
            out.append({"source": "NewsAPI", "type": "news", "region": loc[2] if loc else (a.get("source") or {}).get("name", ""),
                        "lat": loc[0] if loc else None, "lon": loc[1] if loc else None, "geo_approx": bool(loc),
                        "severity": "high" if LEVEL[lvl] >= 2 else "low", "description": title[:220],
                        "detected_at": base.normalize_dt(a.get("publishedAt")), "link": a.get("url"), "cred": "B", "tags": tags,
                        "raw": {"outlet": (a.get("source") or {}).get("name")}})
        return base._ok(name, out)
    except Exception as e:
        return base._fail(name, e)


# ------------------------------------------------------------
# Caché por fuente (evita machacar ~70 feeds cada 15 s y que te bloqueen)
# ------------------------------------------------------------
_REAL_MS: Dict[str, int] = {}
_TTL_STATE: Dict[str, dict] = {}
NO_FORCE = {"OpenSky", "GDELT DOC", "GDELT GEO", "FIRMS abierto", "NewsAPI"}


def ttl_wrap(name: str, fn, ttl: float):
    st = {"ts": 0.0, "data": [], "lock": threading.Lock()}
    _TTL_STATE[name] = st

    def w():
        with st["lock"]:
            if time.time() - st["ts"] < ttl:
                return copy.deepcopy(st["data"])
            t0 = time.time()
            res = fn() or []
            st["data"] = copy.deepcopy(res)
            st["ts"] = time.time()
            if base.SOURCE_STATUS.get(name, {}).get("status") == "error":
                st["ts"] = time.time() - ttl + min(ttl, 30)  # reintenta pronto si falló
            _REAL_MS[name] = int((time.time() - t0) * 1000)
            return res
    return w


_TTL = {"RSS Noticias": 120, "Guerra (prensa/OSINT)": 120, "Comunicados oficiales": 120, "Medios estatales": 120,
        "Telegram": 30, "GDELT GEO": 300, "USGS": 60, "EMSC": 60, "GEOFON": 120, "NASA EONET": 300,
        "NASA FIRMS": 900, "GDELT": 120, "World Monitor": 120}
_NEW_SOURCES = [("GDACS", fetch_gdacs, 300), ("GDELT DOC", fetch_gdelt_doc, 300), ("FIRMS abierto", fetch_firms_open, 1800),
                ("OpenSky", fetch_opensky, 1800), ("NewsAPI", fetch_newsapi, 600)]
base.SOURCES[:] = [(n, ttl_wrap(n, f, _TTL[n]) if n in _TTL else f) for n, f in base.SOURCES]
for _n, _f, _t in _NEW_SOURCES:
    if _n not in {n for n, _ in base.SOURCES}:
        base.SOURCES.append((_n, ttl_wrap(_n, _f, _t)))

_orig_invalidate = base.invalidate_cache


def _invalidate_v25():
    for n, st in _TTL_STATE.items():
        st["ts"] = 0.0
    _orig_invalidate()


base.invalidate_cache = _invalidate_v25


@app.before_request
def _force_clears_ttl():
    if request.path == "/api/alerts" and request.args.get("force") == "1":
        for n, st in _TTL_STATE.items():
            if n not in NO_FORCE:
                st["ts"] = 0.0


_orig_diag_targets = base.diag_targets


def _diag_targets_v25():
    t = _orig_diag_targets()
    t += [("Alertas y sensores", "GDACS", "https://www.gdacs.org/xml/rss.xml", "feed"),
          ("Alertas y sensores", "GDELT DOC", "https://api.gdeltproject.org/api/v2/doc/doc?query=missile&mode=artlist&format=json&maxrecords=1", "json"),
          ("Alertas y sensores", "OpenSky", "https://opensky-network.org/api/states/all?lamin=31&lomin=34&lamax=32&lomax=35", "json")]
    return t


base.diag_targets = _diag_targets_v25


# ============================================================
# 2) GRUPOS DE TELEGRAM
# ============================================================
DEFAULT_TG_GROUPS = {
    "Oficial Ucrania": ["GeneralStaffZSU", "SBUkr", "kpszsu", "ukrainenowenglish", "V_Zelenskiy_official", "dsns_telegram", "mvs_ukraine"],
    "Alertas aéreas UA": ["air_alert_ua", "kpszsu", "operativnoZSU"],
    "Oficial Rusia": ["mod_russia", "MID_Russia"],
    "Oficial Israel": ["idfofficial"],
    "Blogueros militares RU": ["rybar", "readovkanews", "wargonzo", "boris_rozhin", "RVvoenkor", "dva_majors"],
    "Estatales RU": ["tass_agency", "rian_ru"],
    "Independientes": ["meduzalive", "nexta_live", "kyivindependent_official"],
    "OSINT / mapas": ["DeepStateUA", "militarysummary", "intelslava"],
}
_grp_lock = threading.Lock()


def tg_groups_read() -> Dict[str, List[str]]:
    with _grp_lock:
        try:
            with open(TG_GROUPS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            g = d.get("groups", {})
            if isinstance(g, dict):
                return {str(k): [str(x) for x in v] for k, v in g.items() if isinstance(v, list)}
        except Exception:
            pass
    return {k: list(v) for k, v in DEFAULT_TG_GROUPS.items()}


def tg_groups_write(groups: Dict[str, List[str]]):
    with _grp_lock:
        tmp = TG_GROUPS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"groups": groups}, f, ensure_ascii=False, indent=2)
        os.replace(tmp, TG_GROUPS_FILE)


_orig_get_tg = base.get_telegram_channels


def _get_tg_v25(include_disabled: bool = False) -> List[dict]:
    chans = _orig_get_tg(include_disabled=True)
    with base._tg_lock:
        cfg = base._tg_read_cfg()
    dis = {str(x).lower() for x in cfg["disabled"]}
    groups = tg_groups_read()
    for c in chans:
        if c["id"].lower() in dis:
            c["enabled"] = False
        c["groups"] = [g for g, m in groups.items() if c["id"].lower() in {x.lower() for x in m}]
    return chans if include_disabled else [c for c in chans if c["enabled"]]


base.get_telegram_channels = _get_tg_v25


@app.route("/api/telegram/groups", methods=["GET", "POST"])
def v25_tg_groups():
    if request.method == "GET":
        chans = {c["id"].lower(): c for c in base.get_telegram_channels(include_disabled=True)}
        out = []
        for name, members in tg_groups_read().items():
            act = sum(1 for m in members if chans.get(m.lower(), {}).get("enabled"))
            out.append({"name": name, "members": [{"id": m, "name": chans.get(m.lower(), {}).get("name", m),
                                                   "enabled": bool(chans.get(m.lower(), {}).get("enabled"))} for m in members],
                        "active": act, "enabled": bool(members) and act == len(members)})
        return jsonify({"groups": out})
    body = request.get_json(silent=True) or {}
    name = re.sub(r"[\x00-\x1f]", "", str(body.get("name") or "")).strip()[:40]
    if not name:
        return jsonify({"error": "Nombre de grupo requerido"}), 400
    groups = tg_groups_read()
    cur = groups.setdefault(name, [])
    warnings = []
    known = {c["id"].lower() for c in base.get_telegram_channels(include_disabled=True)}
    for raw in body.get("add") or []:
        u = base.clean_username(raw)
        if not u:
            warnings.append(f"{raw}: usuario no válido")
            continue
        if u.lower() not in known:
            try:
                info = base.tg_channel_info(u)
                if not info.get("exists"):
                    warnings.append(f"@{u} no existe o no es público")
                    continue
                base.add_telegram_channel(u, info.get("title") or u)
                known.add(u.lower())
            except Exception as e:
                warnings.append(f"@{u}: no verificado ({str(e)[:60]})")
                continue
        if u.lower() not in {x.lower() for x in cur}:
            cur.append(u)
    rem = {base.clean_username(x).lower() for x in (body.get("remove") or []) if base.clean_username(x)}
    groups[name] = [x for x in cur if x.lower() not in rem]
    tg_groups_write(groups)
    base.invalidate_cache()
    return jsonify({"ok": True, "warnings": warnings})


@app.route("/api/telegram/groups/toggle", methods=["POST"])
def v25_tg_group_toggle():
    body = request.get_json(silent=True) or {}
    members = tg_groups_read().get(str(body.get("name") or ""))
    if members is None:
        return jsonify({"error": "Grupo no encontrado"}), 404
    enabled = bool(body.get("enabled"))
    with base._tg_lock:
        cfg = base._tg_read_cfg()
        dis = [d for d in cfg["disabled"] if str(d).lower() not in {m.lower() for m in members}]
        if not enabled:
            dis += list(members)
        cfg["disabled"] = dis
        base._tg_write_cfg(cfg)
    base.invalidate_cache()
    return jsonify({"ok": True})


@app.route("/api/telegram/groups/<name>", methods=["DELETE"])
def v25_tg_group_delete(name):
    groups = tg_groups_read()
    if name not in groups:
        return jsonify({"error": "Grupo no encontrado"}), 404
    groups.pop(name)
    tg_groups_write(groups)
    return jsonify({"ok": True})


# ============================================================
# 3) LISTA DE VIGILANCIA (palabras + zonas)
# ============================================================
_watch_lock = threading.Lock()


def watch_read() -> Dict[str, list]:
    with _watch_lock:
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            return {"keywords": [str(k) for k in d.get("keywords", [])], "zones": [z for z in d.get("zones", []) if isinstance(z, dict)]}
        except Exception:
            return {"keywords": [], "zones": []}


def watch_write(w: Dict[str, list]):
    with _watch_lock:
        tmp = WATCHLIST_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(w, f, ensure_ascii=False, indent=2)
        os.replace(tmp, WATCHLIST_FILE)


@app.route("/api/watchlist", methods=["GET", "POST", "DELETE"])
def v25_watchlist():
    w = watch_read()
    if request.method == "GET":
        return jsonify(w)
    body = request.get_json(silent=True) or {}
    if request.method == "POST":
        kw = str(body.get("keyword") or "").strip().lower()[:60]
        z = body.get("zone")
        if kw and kw not in w["keywords"]:
            w["keywords"].append(kw)
        if isinstance(z, dict):
            la, lo, rk = base.to_float(z.get("lat")), base.to_float(z.get("lon")), base.to_float(z.get("radius_km"), 100.0)
            nm = str(z.get("name") or "").strip()[:40]
            if nm and la is not None and lo is not None and -90 <= la <= 90 and -180 <= lo <= 180:
                w["zones"] = [x for x in w["zones"] if x.get("name") != nm] + [{"name": nm, "lat": la, "lon": lo, "radius_km": max(1.0, min(rk or 100.0, 3000.0))}]
            else:
                return jsonify({"error": "Zona no válida (nombre, lat, lon)"}), 400
        if not kw and not z:
            return jsonify({"error": "Nada que añadir"}), 400
    else:
        kw = str(request.args.get("keyword") or "").strip().lower()
        zn = str(request.args.get("zone") or "")
        w["keywords"] = [k for k in w["keywords"] if k != kw]
        w["zones"] = [z for z in w["zones"] if z.get("name") != zn]
    watch_write(w)
    base.invalidate_cache()
    return jsonify({"ok": True, **w})


def apply_watch(alerts: List[dict]) -> int:
    w = watch_read()
    if not w["keywords"] and not w["zones"]:
        return 0
    hits = 0
    for a in alerts:
        tags = a.setdefault("tags", [])
        low = f"{a.get('description', '')} {a.get('region', '')}".lower()
        hit = False
        for kw in w["keywords"]:
            if kw in low:
                tags.append(f"kw:{kw}")
                hit = True
        if hasgeo(a) and is_precise(a):
            for z in w["zones"]:
                if base.haversine_km(float(a["lat"]), float(a["lon"]), z["lat"], z["lon"]) <= z["radius_km"]:
                    tags.append(f"zona:{z['name']}")
                    hit = True
        if hit:
            tags.append("vigilado")
            hits += 1
    return hits


# ============================================================
# 4) MOTOR DE CRUCE (incidentes multi-fuente)
# ============================================================
COUNTRY_NAMES = {c[0] for c in base.COUNTRIES}
CORR_TYPES = {"missile_alert", "air_raid", "kyiv_alert", "drone_missile_track", "telegram_signal", "war_news", "news",
              "news_geo", "seismic_explosion", "explosion_candidate", "nuclear_test_candidate", "thermal_anomaly",
              "acled_conflict", "conflict_event", "state_media"}
NON_CONFLICT = {"earthquake", "natural_event", "disaster", "flight_activity"}


def is_precise(a: dict) -> bool:
    if not hasgeo(a) or a.get("geo_known") is False:
        return False
    if a.get("type") == "missile_alert" and a.get("geo_approx"):
        return False
    return (a.get("place") or "") not in COUNTRY_NAMES


def source_family(src: str) -> str:
    s = (src or "").lower()
    if "tzeva" in s or "oref" in s:
        return "Alertas Israel (Oref)"
    if any(k in s for k in ("alerts.com.ua", "alerts.in.ua", "ubilling", "kyiv digital")):
        return "Alertas Ucrania (sistema oficial)"
    return src


def _tok(s: str) -> set:
    return {w for w in re.findall(r"[^\W\d_]{4,}", (s or "").lower()) if w not in base._STOPWORDS}


def build_incidents(alerts: List[dict]) -> List[dict]:
    cands = [a for a in alerts if a.get("type") in CORR_TYPES and is_precise(a) and _age(a) <= CORR_WINDOW_H * 3600]
    cands.sort(key=lambda a: a.get("detected_at", ""), reverse=True)
    cands = cands[:900]
    n = len(cands)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    ts = [base.epoch_of(a["detected_at"]) for a in cands]
    la = [float(a["lat"]) for a in cands]
    lo = [float(a["lon"]) for a in cands]
    for i in range(n):
        for j in range(i + 1, n):
            if abs(ts[i] - ts[j]) > CLUSTER_MIN * 60 or abs(la[i] - la[j]) * 111.0 > CLUSTER_KM:
                continue
            if base.haversine_km(la[i], lo[i], la[j], lo[j]) <= CLUSTER_KM:
                parent[find(i)] = find(j)
    clusters: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        clusters[find(i)].append(i)

    incidents = []
    for idxs in clusters.values():
        mem = [cands[i] for i in idxs]
        fams: Dict[str, List[dict]] = defaultdict(list)
        for m in mem:
            fams[source_family(m["source"])].append(m)
        if len(fams) < 2:
            continue
        groups = Counter(alert_group(m) for m in mem)
        wsum = sum(CRED_W.get(m.get("cred", "D"), 0.2) for m in mem) or 1.0
        clat = sum(float(m["lat"]) * CRED_W.get(m.get("cred", "D"), 0.2) for m in mem) / wsum
        clon = sum(float(m["lon"]) * CRED_W.get(m.get("cred", "D"), 0.2) for m in mem) / wsum
        radius = max(base.haversine_km(clat, clon, float(m["lat"]), float(m["lon"])) for m in mem)
        text_mem = [m for m in mem if m.get("type") in TEXT_TYPES]
        nontext_fams = {source_family(m["source"]) for m in mem if m.get("type") not in TEXT_TYPES}
        toks = [_tok(m.get("description", "")) for m in text_mem]
        tp = list(range(len(text_mem)))

        def tf(x):
            while tp[x] != x:
                tp[x] = tp[tp[x]]
                x = tp[x]
            return x

        for i in range(len(text_mem)):
            for j in range(i + 1, len(text_mem)):
                u = toks[i] | toks[j]
                if u and len(toks[i] & toks[j]) / len(u) >= 0.55:
                    tp[tf(i)] = tf(j)
        indep = len(nontext_fams) + len({tf(i) for i in range(len(text_mem))})
        has_a = any(m.get("cred") == "A" for m in mem)
        if has_a and len(groups) >= 2 and indep >= 3:
            conf = "alta"
        elif indep >= 3 or (has_a and indep >= 2):
            conf = "media"
        else:
            conf = "baja"
        if all(m.get("cred", "D") == "D" for m in mem):
            conf = "baja"
        score = sum(max(CRED_W.get(m.get("cred", "D"), 0.2) for m in ms) for ms in fams.values()) + 0.5 * (len(groups) - 1)
        mem.sort(key=lambda m: m["detected_at"])
        place = Counter((m.get("place") or m.get("region") or "?") for m in mem).most_common(1)[0][0]
        sev = max((m.get("severity", "low") for m in mem), key=lambda s: LEVEL.get(s, 0))
        t0, t1 = mem[0]["detected_at"], mem[-1]["detected_at"]
        iid = hashlib.sha1(base.make_fingerprint(mem[0]).encode()).hexdigest()[:10]
        srcs = sorted(fams)
        inc = {
            "id": iid, "place": str(place)[:40], "lat": clat, "lon": clon, "radius_km": round(radius, 1),
            "first_seen": t0, "last_seen": t1, "n_alerts": len(mem), "n_sources": len(fams), "independent": indep,
            "groups": dict(groups), "sources": srcs, "conf": conf, "score": min(100, int(score * 20)), "severity": sev,
            "cred_mix": dict(Counter(m.get("cred", "D") for m in mem)),
            "summary": f"{place}: {len(mem)} reportes de {len(fams)} fuentes ({', '.join(sorted(groups))}) entre "
                       f"{t0[11:16]} y {t1[11:16]} UTC",
            "members": [{"source": m["source"], "type": m.get("type"), "group": alert_group(m), "cred": m.get("cred", "D"),
                         "severity": m.get("severity"), "detected_at": m["detected_at"], "description": str(m.get("description", ""))[:160],
                         "link": m.get("link"), "lat": float(m["lat"]), "lon": float(m["lon"])} for m in mem[-25:]],
        }
        for m in mem:
            m["incident_id"], m["corr_n"], m["corr_conf"], m["corr_sources"] = iid, len(fams) - 1, conf, srcs[:6]
        incidents.append(inc)
    rank = {"alta": 2, "media": 1, "baja": 0}
    incidents.sort(key=lambda i: (rank[i["conf"]], i["score"], i["last_seen"]), reverse=True)
    return incidents[:40]


def build_matrix(alerts: List[dict], top: int = 12) -> dict:
    cnt: Dict[str, Counter] = defaultdict(Counter)
    for a in alerts:
        if a.get("type") in NON_CONFLICT or a.get("geo_known") is False:
            continue
        if a.get("type") in TEXT_TYPES and not hasgeo(a):
            continue
        pl = str(a.get("place") or a.get("region") or "?")[:28]
        cnt[pl][alert_group(a)] += 1
    places = sorted(cnt, key=lambda p: -sum(cnt[p].values()))[:top]
    groups = sorted({g for p in places for g in cnt[p]})
    return {"places": places, "groups": groups, "cells": [[cnt[p][g] for g in groups] for p in places]}


def activity_index(alerts: List[dict]) -> Tuple[int, dict]:
    c30 = h30 = ex2 = st6 = 0
    for a in alerts:
        ag, sev = _age(a), a.get("severity")
        if ag <= 1800:
            c30 += sev == "critical"
            h30 += sev == "high"
        if a.get("type") in base.EXPLOSION_TYPES and ag <= 7200:
            ex2 += 1
        if a.get("type") == "official_statement" and sev in ("critical", "high") and ag <= 6 * 3600:
            st6 += 1
    return min(100, c30 * 8 + h30 * 3 + ex2 * 12 + st6 * 15), {"critical_30m": c30, "high_30m": h30, "explosions_2h": ex2, "statements_6h": st6}


def level_of(idx: int) -> str:
    return "NORMAL" if idx < 15 else ("VIGILANCIA" if idx < 35 else ("ELEVADO" if idx < 60 else "CRÍTICO"))


# ------------------------------------------------------------
# Histórico del índice
# ------------------------------------------------------------
_last_bucket = {"b": 0}


def init_v25_db():
    with base._db_lock:
        conn = sqlite3.connect(base.DB_PATH, timeout=10)
        conn.execute("CREATE TABLE IF NOT EXISTS index_history (ts TEXT PRIMARY KEY, idx INTEGER, critical INTEGER, total INTEGER, incidents INTEGER, explosions INTEGER)")
        conn.commit()
        conn.close()


def log_index(idx: int, stats: dict, ninc: int):
    b = int(time.time() // 300)
    if b == _last_bucket["b"]:
        return
    _last_bucket["b"] = b
    try:
        with base._db_lock:
            conn = sqlite3.connect(base.DB_PATH, timeout=10)
            try:
                conn.execute("INSERT OR REPLACE INTO index_history VALUES (?,?,?,?,?,?)",
                             (base.now_iso()[:16], idx, stats.get("critical", 0), stats.get("total", 0), ninc, stats.get("explosions", 0)))
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        log.warning(f"index_history: {e}")


def index_history(hours: float) -> List[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()[:16]
    try:
        with base._db_lock:
            conn = sqlite3.connect(base.DB_PATH, timeout=10)
            try:
                rows = conn.execute("SELECT ts, idx, critical, total, incidents FROM index_history WHERE ts >= ? ORDER BY ts", (since,)).fetchall()
            finally:
                conn.close()
        return [{"ts": r[0], "idx": r[1], "critical": r[2], "total": r[3], "incidents": r[4]} for r in rows]
    except Exception:
        return []


# ------------------------------------------------------------
# Notificaciones externas
# ------------------------------------------------------------
_notified: set = set()
_notified_order: deque = deque(maxlen=2000)
_warm = {"done": False}


def send_external(text: str) -> List[str]:
    sent = []
    import requests as rq
    try:
        if NOTIFY_WEBHOOK_URL:
            rq.post(NOTIFY_WEBHOOK_URL, json={"text": text, "content": text}, timeout=10)
            sent.append("webhook")
    except Exception as e:
        log.warning(f"notify webhook: {str(e)[:80]}")
    try:
        if NTFY_URL:
            rq.post(NTFY_URL, data=text.encode("utf-8"), headers={"Title": "OSINT Monitor"}, timeout=10)
            sent.append("ntfy")
    except Exception as e:
        log.warning(f"notify ntfy: {str(e)[:80]}")
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            rq.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    data={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3900]}, timeout=10)
            sent.append("telegram")
    except Exception as e:
        log.warning("notify telegram: error")
    return sent


def notify_events(alerts: List[dict], incidents: List[dict], special: List[dict]):
    ev: List[Tuple[str, str]] = []
    for i in incidents:
        if i["conf"] in ("alta", "media") and LEVEL.get(i["severity"], 0) >= 2:
            ev.append(("inc:" + i["id"], f"🔗 Incidente cruzado [{i['conf']}] — {i['summary']}"))
    for s in special:
        ev.append(("st:" + hashlib.sha1(s["description"].encode("utf-8", "ignore")).hexdigest()[:12], f"📜 Comunicado ({s['source']}): {s['description']}"))
    for a in alerts:
        fp = base.make_fingerprint(a)
        if a.get("type") in base.EXPLOSION_TYPES and "correlado" in (a.get("tags") or []) and _age(a) <= 7200:
            ev.append(("ex:" + fp, f"💥 {a['description']}"))
        elif "vigilado" in (a.get("tags") or []) and _age(a) <= 3600:
            ev.append(("w:" + fp, f"👁 Vigilancia [{a['source']}]: {a['description'][:200]}"))
    new = [(k, t) for k, t in ev if k not in _notified]
    for k, _t in new:
        _notified.add(k)
        _notified_order.append(k)
    while len(_notified) > 2500 and _notified_order:
        _notified.discard(_notified_order.popleft())
    if not _warm["done"]:
        _warm["done"] = True  # el primer ciclo solo registra lo existente, no envía
        return
    if new and (NOTIFY_WEBHOOK_URL or NTFY_URL or (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)):
        text = "\n".join(t for _k, t in new[:6])
        threading.Thread(target=send_external, args=(text,), daemon=True).start()


# ------------------------------------------------------------
# Enriquecimiento de cada ciclo de fetch_all
# ------------------------------------------------------------
_orig_fetch_all = base.fetch_all


def fetch_all_v25() -> Dict[str, Any]:
    result = _orig_fetch_all()
    try:
        alerts, stats = result["alerts"], result["stats"]
        tgmap: Dict[str, List[str]] = defaultdict(list)
        for g, members in tg_groups_read().items():
            for m in members:
                tgmap[m.lower()].append(g)
        for a in alerts:
            a["group"] = alert_group(a)
            a.setdefault("tags", [])
            if a.get("type") == "telegram_signal":
                for g in tgmap.get(str((a.get("raw") or {}).get("channel", "")).lower(), []):
                    a["tags"].append(f"grupo:{g}")
        watch_hits = apply_watch(alerts)
        incidents = build_incidents(alerts)
        idx, parts = activity_index(alerts)
        stats["incidents"] = incidents
        stats["matrix"] = build_matrix(alerts)
        stats["by_group"] = dict(Counter(a["group"] for a in alerts))
        stats["seismic_points"] = [{"mag": a["raw"].get("mag"), "depth": a["raw"].get("depth"), "type": a["type"], "place": a.get("region"),
                                    "t": a["detected_at"]} for a in alerts if a.get("type") in base.SEISMIC_TYPES
                                   and isinstance(a.get("raw"), dict) and a["raw"].get("mag") is not None][:200]
        stats["index"], stats["level"], stats["index_parts"] = idx, level_of(idx), parts
        stats["watch_hits"] = watch_hits
        stats["version"] = VERSION
        for n, ms in _REAL_MS.items():
            if n in stats["source_status"]:
                stats["source_status"][n]["ms"] = ms
            with base._status_lock:
                if n in base.SOURCE_STATUS:
                    base.SOURCE_STATUS[n]["ms"] = ms
        log_index(idx, stats, len(incidents))
        notify_events(alerts, incidents, stats.get("special_statements", []))
    except Exception as e:
        log.warning(f"enriquecimiento v2.5: {e}")
    return result


base.fetch_all = fetch_all_v25


# ============================================================
# 5) INFORME AUTOMATIZADO
# ============================================================
def live_snapshot(force: bool = False) -> Dict[str, Any]:
    if not force:
        d, _age_s = base._cached_fresh()
        if d:
            return d
    with base._fetch_lock:
        d = base.fetch_all()
        with base._cache_lock:
            base._cache["data"] = d
            base._cache["last_fetch"] = datetime.now(timezone.utc)
    return d


def hist_group_timeline(rows: List[dict], hours: float) -> dict:
    nb = int(min(48, max(6, round(hours))))
    bin_s = hours * 3600.0 / nb
    now = time.time()
    series: Dict[str, List[int]] = defaultdict(lambda: [0] * nb)
    for r in rows:
        k = int((now - base.epoch_of(r["detected_at"])) // bin_s)
        if 0 <= k < nb:
            series[GROUP_OF_TYPE.get(r.get("type"), "Otros")][nb - 1 - k] += 1
    labels = [datetime.fromtimestamp(now - (nb - 1 - i) * bin_s, tz=timezone.utc).strftime("%d %H:%M") for i in range(nb)]
    return {"labels": labels, "series": dict(series)}


def collect_report(hours: float = 24.0) -> Dict[str, Any]:
    hours = max(1.0, min(float(hours), 168.0))
    snap = live_snapshot()
    alerts, stats = snap["alerts"], snap["stats"]
    hist = base.query_history(hours=hours, limit=5000)
    now = time.time()
    half = hours * 1800.0
    rec = sum(1 for r in hist if r["severity"] in ("critical", "high") and now - base.epoch_of(r["detected_at"]) <= half)
    prev = sum(1 for r in hist if r["severity"] in ("critical", "high") and half < now - base.epoch_of(r["detected_at"]) <= 2 * half)
    tlabel = "al alza" if rec > prev * 1.25 + 1 else ("a la baja" if rec * 1.25 + 1 < prev else "estable")
    incidents = stats.get("incidents", [])
    idx, level = stats.get("index", 0), stats.get("level", level_of(0))
    win = hours * 3600
    statements = sorted([a for a in alerts if a.get("type") == "official_statement" and _age(a) <= win],
                        key=lambda a: (LEVEL.get(a["severity"], 0), a["detected_at"]), reverse=True)[:20]
    expl = [a for a in alerts if a.get("type") in base.EXPLOSION_TYPES and _age(a) <= win][:15]
    state = [a for a in alerts if a.get("type") == "state_media" and _age(a) <= win][:8]
    src_health = [{"name": n, "status": s.get("status"), "count": s.get("count", 0), "ms": s.get("ms", 0),
                   "error": s.get("error", ""), "disabled": bool(s.get("disabled"))} for n, s in sorted(stats.get("source_status", {}).items())]
    rep: Dict[str, Any] = {
        "generated_at": base.now_iso(), "hours": hours, "version": VERSION, "index": idx, "level": level,
        "index_parts": stats.get("index_parts", {}), "trend": {"recent": rec, "previous": prev, "label": tlabel},
        "live_total": stats.get("total", 0), "live_critical": stats.get("critical", 0),
        "incidents": incidents[:15], "regions": stats.get("matrix", {}),
        "by_group": dict(Counter(GROUP_OF_TYPE.get(r.get("type"), "Otros") for r in hist)),
        "by_sev": dict(Counter(r["severity"] for r in hist)), "by_cred": dict(Counter(r.get("cred", "D") for r in hist)),
        "by_source": dict(Counter(r["source"] for r in hist).most_common(12)),
        "history_rows": len(hist), "timeline": hist_group_timeline(hist, hours),
        "statements": [{"source": a["source"], "severity": a["severity"], "description": a["description"], "detected_at": a["detected_at"],
                        "link": a.get("link"), "cred": a.get("cred", "C"), "tags": a.get("tags", [])} for a in statements],
        "explosions": [{"description": a["description"], "severity": a["severity"], "detected_at": a["detected_at"], "tags": a.get("tags", [])} for a in expl],
        "state_media": [{"source": a["source"], "description": a["description"], "detected_at": a["detected_at"], "link": a.get("link")} for a in state],
        "telegram_unverified": sum(1 for a in alerts if a.get("type") == "telegram_signal" and _age(a) <= win),
        "sources": src_health, "watch_hits": stats.get("watch_hits", 0),
        "watch_items": [{"source": a["source"], "description": a["description"][:200], "detected_at": a["detected_at"],
                         "tags": [t for t in a.get("tags", []) if t.startswith(("kw:", "zona:"))]}
                        for a in alerts if "vigilado" in (a.get("tags") or [])][:15],
        "index_history": index_history(hours),
    }
    rep["findings"] = make_findings(rep)
    rep["headline"] = (f"{level}: {len(incidents)} incidente(s) con cruce de fuentes, "
                       f"{stats.get('index_parts', {}).get('critical_30m', 0)} alerta(s) crítica(s) en los últimos 30 min")
    return rep


def make_findings(rep: Dict[str, Any]) -> List[str]:
    f = [f"Nivel de actividad {rep['level']} (índice {rep['index']}/100). Tendencia {rep['trend']['label']}: "
         f"{rep['trend']['recent']} eventos high/critical en la segunda mitad de la ventana frente a {rep['trend']['previous']} en la primera."]
    incs = rep["incidents"]
    if incs:
        i = incs[0]
        f.append(f"Cruce más relevante: {i['summary']} — confianza {i['conf']}, ≈{i['independent']} fuentes independientes, puntuación {i['score']}/100.")
        f.append(f"{len(incs)} incidente(s) con cruce de fuentes; {sum(1 for i in incs if i['conf'] == 'alta')} de confianza alta y {sum(1 for i in incs if i['conf'] == 'media')} media.")
    else:
        f.append("Sin incidentes con cruce de fuentes en la ventana de correlación actual.")
    reg = rep["regions"]
    if reg.get("places"):
        tot = sorted(zip(reg["places"], [sum(r) for r in reg["cells"]]), key=lambda x: -x[1])[:3]
        f.append("Zonas con más reportes: " + ", ".join(f"{p} ({n})" for p, n in tot) + ".")
    if rep["explosions"]:
        corr = sum(1 for e in rep["explosions"] if "correlado" in e["tags"])
        f.append(f"{len(rep['explosions'])} evento(s) sísmico(s) anómalo(s) compatible(s) con explosión (heurístico); {corr} coinciden con alertas de ataque cercanas.")
    sts = [s for s in rep["statements"] if s["severity"] in ("critical", "high")]
    if sts:
        f.append(f"{len(sts)} comunicado(s) oficial(es) de severidad alta (p. ej. {sts[0]['source'].replace('Comunicado — ', '')}: {sts[0]['description'][:110]}).")
    if rep["state_media"]:
        f.append(f"{len(rep['state_media'])} titular(es) de medios estatales: reflejan la versión de cada gobierno, no hechos verificados.")
    f.append(f"{rep['telegram_unverified']} señal(es) de Telegram sin verificar en la ventana.")
    if rep["watch_hits"]:
        f.append(f"{rep['watch_hits']} alerta(s) coinciden con tu lista de vigilancia.")
    down = [s["name"] for s in rep["sources"] if s["status"] == "error" and not s["disabled"]]
    if down:
        f.append(f"Fuentes caídas ({len(down)}): {', '.join(down[:8])}. La cobertura puede estar incompleta.")
    return f


CAVEATS = [
    "Este informe es una agregación automática de fuentes abiertas; no confirma hechos. En un conflicto ninguna fuente garantiza veracidad.",
    "La credibilidad (A/B/C/D) indica el TIPO de fuente (oficial/instrumental, prensa/OSINT, parte implicada, no verificado), no la certeza del dato.",
    "Los cruces agrupan reportes por proximidad (≤%d km) y tiempo (≤%d min). Se cuentan como independientes solo fuentes distintas; "
    "alertas del mismo sistema oficial y mensajes casi idénticos cuentan una vez. Un cruce eleva la confianza, no la convierte en certeza." % (CLUSTER_KM, CLUSTER_MIN),
    "Candidatos a explosión (sismos someros en zona de conflicto) son heurísticos y pueden ser sismos reales, minería o demoliciones.",
    "Las anomalías térmicas pueden ser incendios o industria. Los medios estatales y Telegram reflejan la versión de quien los emite.",
    "El historial solo contiene lo que el monitor ha registrado desde que se puso en marcha.",
]

SEV_ES = {"critical": "crítica", "high": "alta", "medium": "media", "low": "baja"}


def render_md(rep: Dict[str, Any]) -> str:
    L = [f"# Informe de situación — OSINT Early Warning v{rep['version']}",
         f"_Generado: {rep['generated_at']} UTC · ventana: {rep['hours']:.0f} h_", "",
         f"## {rep['headline']}", f"**Nivel:** {rep['level']} · **Índice:** {rep['index']}/100 · **Tendencia:** {rep['trend']['label']}", "",
         "## Hallazgos clave"] + [f"- {x}" for x in rep["findings"]] + ["", "## Incidentes con cruce de fuentes"]
    if rep["incidents"]:
        L += ["| Lugar | Confianza | Puntuación | Fuentes indep. | Severidad | Última | Fuentes |", "|---|---|---|---|---|---|---|"]
        for i in rep["incidents"]:
            L.append(f"| {i['place']} | {i['conf']} | {i['score']} | {i['independent']} | {SEV_ES.get(i['severity'], i['severity'])} | {i['last_seen'][11:16]} UTC | {', '.join(i['sources'])[:120]} |")
    else:
        L.append("Sin incidentes cruzados.")
    L += ["", "## Reportes por grupo (historial)"] + [f"- {k}: {v}" for k, v in sorted(rep["by_group"].items(), key=lambda x: -x[1])]
    L += ["", "## Comunicados oficiales"]
    L += [f"- [{s['cred']}] {s['detected_at'][:16]} · {s['source']}: {s['description']}" for s in rep["statements"]] or ["- Ninguno relevante."]
    L += ["", "## Posibles explosiones sísmicas"] + ([f"- {e['detected_at'][:16]} · {e['description']}" for e in rep["explosions"]] or ["- Ninguna."])
    L += ["", "## Medios estatales (versión de parte)"] + ([f"- {s['source']}: {s['description']}" for s in rep["state_media"]] or ["- Ninguno."])
    if rep["watch_items"]:
        L += ["", "## Lista de vigilancia"] + [f"- {w['source']}: {w['description']} ({', '.join(w['tags'])})" for w in rep["watch_items"]]
    L += ["", "## Estado de fuentes"] + [f"- {s['name']}: {s['status']}{' (desactivada)' if s['disabled'] else ''} · {s['count']} · {s['ms']} ms" + (f" · {s['error']}" if s["error"] else "") for s in rep["sources"]]
    L += ["", "## Avisos y metodología"] + [f"- {c}" for c in CAVEATS]
    return "\n".join(L)


_E = lambda x: hesc(str(x if x is not None else ""), quote=True)
PAL = ["#2f6fed", "#e5484d", "#f5a524", "#17c964", "#9b59f6", "#06b6d4", "#ec4899", "#71717a", "#84cc16", "#f97316"]
GCOL = {"Alertas": "#e5484d", "Sísmico": "#9b59f6", "Satélite": "#f5a524", "Naturales": "#06b6d4", "Prensa/OSINT": "#2f6fed",
        "Gobierno": "#17c964", "Estatal": "#eab308", "Telegram": "#38bdf8", "Conflicto": "#ec4899", "Aviación": "#71717a", "Otros": "#a1a1aa"}


def svg_hbar(title, pairs, color="#2f6fed", w=560):
    pairs = list(pairs)[:14]
    if not pairs:
        return f'<p class="mut">{_E(title)}: sin datos</p>'
    rh, lw = 20, 190
    h = 34 + rh * len(pairs)
    mx = max(v for _l, v in pairs) or 1
    bw = w - lw - 60
    s = [f'<svg viewBox="0 0 {w} {h}" width="100%"><text x="0" y="16" class="t">{_E(title)}</text>']
    for i, (l, v) in enumerate(pairs):
        y, ww = 30 + i * rh, max(1.0, bw * v / mx)
        s.append(f'<text x="{lw - 6}" y="{y + 13}" text-anchor="end" class="l">{_E(str(l)[:30])}</text>'
                 f'<rect x="{lw}" y="{y + 2}" width="{ww:.1f}" height="14" fill="{color}" rx="2"/>'
                 f'<text x="{lw + ww + 4:.1f}" y="{y + 13}" class="l">{v}</text>')
    return "".join(s) + "</svg>"


def svg_donut(title, pairs, colors=None, w=560):
    pairs = [(l, v) for l, v in pairs if v]
    tot = sum(v for _l, v in pairs)
    if not tot:
        return f'<p class="mut">{_E(title)}: sin datos</p>'
    r, cx, cy = 60, 90, 100
    C = 2 * math.pi * r
    s = [f'<svg viewBox="0 0 {w} 200" width="100%"><text x="0" y="16" class="t">{_E(title)}</text>']
    off = 0.0
    for i, (l, v) in enumerate(pairs):
        col = (colors or {}).get(l) or PAL[i % len(PAL)]
        ln = C * v / tot
        s.append(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{col}" stroke-width="28" stroke-dasharray="{ln:.2f} {C - ln:.2f}" '
                 f'stroke-dashoffset="{-off:.2f}" transform="rotate(-90 {cx} {cy})"/>')
        s.append(f'<rect x="200" y="{36 + i * 18}" width="12" height="12" fill="{col}"/><text x="218" y="{47 + i * 18}" class="l">{_E(l)} — {v} ({100 * v / tot:.0f}%)</text>')
        off += ln
    return "".join(s) + "</svg>"


def svg_stack(title, labels, series, colors, w=560, h=230):
    n = len(labels)
    if not n or not series:
        return f'<p class="mut">{_E(title)}: sin datos</p>'
    tot = [sum(series[k][i] for k in series) for i in range(n)]
    mx = max(tot + [1])
    ch, bw = h - 70, (w - 40) / n
    s = [f'<svg viewBox="0 0 {w} {h}" width="100%"><text x="0" y="16" class="t">{_E(title)}</text>']
    for i in range(n):
        y = 30 + ch
        for k in series:
            v = series[k][i]
            if v:
                hh = ch * v / mx
                y -= hh
                s.append(f'<rect x="{30 + i * bw:.1f}" y="{y:.1f}" width="{max(1.0, bw - 1):.1f}" height="{hh:.1f}" fill="{colors.get(k, "#999")}"/>')
    s.append(f'<text x="30" y="{h - 34}" class="l">{_E(labels[0])}</text><text x="{w - 10}" y="{h - 34}" text-anchor="end" class="l">{_E(labels[-1])}</text>')
    s.append(f'<text x="2" y="40" class="l">{mx}</text>')
    for j, k in enumerate(series):
        s.append(f'<rect x="{30 + (j % 5) * 105}" y="{h - 24 + (j // 5) * 14}" width="9" height="9" fill="{colors.get(k, "#999")}"/><text x="{43 + (j % 5) * 105}" y="{h - 16 + (j // 5) * 14}" class="l">{_E(k)}</text>')
    return "".join(s) + "</svg>"


def svg_line(title, labels, values, color="#e5484d", w=560, h=170):
    if len(values) < 2:
        return f'<p class="mut">{_E(title)}: aún sin histórico suficiente</p>'
    mx = max(values + [1])
    ch = h - 50
    pts = " ".join(f"{30 + i * (w - 40) / (len(values) - 1):.1f},{30 + ch - ch * v / mx:.1f}" for i, v in enumerate(values))
    return (f'<svg viewBox="0 0 {w} {h}" width="100%"><text x="0" y="16" class="t">{_E(title)}</text>'
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/>'
            f'<text x="30" y="{h - 8}" class="l">{_E(labels[0])}</text><text x="{w - 10}" y="{h - 8}" text-anchor="end" class="l">{_E(labels[-1])}</text>'
            f'<text x="2" y="40" class="l">{mx}</text></svg>')


def render_html(rep: Dict[str, Any]) -> str:
    lvl_col = {"NORMAL": "#17c964", "VIGILANCIA": "#eab308", "ELEVADO": "#f97316", "CRÍTICO": "#e5484d"}.get(rep["level"], "#71717a")
    tl = rep["timeline"]
    h = [f"""<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><title>Informe OSINT {rep['generated_at'][:16]}</title>
<style>body{{font:14px/1.5 system-ui,sans-serif;max-width:980px;margin:0 auto;padding:20px;color:#1f2430;background:#fff}}
h1{{font-size:22px;margin:0}}h2{{font-size:16px;border-bottom:2px solid #e5e7eb;padding-bottom:4px;margin-top:26px}}
.banner{{background:{lvl_col};color:#fff;padding:14px 18px;border-radius:8px;margin:14px 0}}.banner b{{font-size:20px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}}.kpi{{border:1px solid #e5e7eb;border-radius:8px;padding:10px;text-align:center}}
.kpi .v{{font-size:24px;font-weight:700}}.kpi .k{{font-size:11px;color:#667;text-transform:uppercase}}
table{{border-collapse:collapse;width:100%;font-size:12px}}th,td{{border:1px solid #e5e7eb;padding:4px 6px;text-align:left;vertical-align:top}}th{{background:#f4f5f7}}
.mut{{color:#889;font-size:12px}}.t{{font:600 13px sans-serif;fill:#222}}.l{{font:11px sans-serif;fill:#444}}.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
.c-alta{{color:#0a8f4a;font-weight:700}}.c-media{{color:#c27800;font-weight:700}}.c-baja{{color:#888}}.cav li{{font-size:12px;color:#556}}
@media print{{.grid{{grid-template-columns:1fr}}}}</style></head><body>
<h1>Informe de situación — OSINT Early Warning v{_E(rep['version'])}</h1>
<div class="mut">Generado {_E(rep['generated_at'])} UTC · ventana {rep['hours']:.0f} h · {rep['history_rows']} registros en histórico</div>
<div class="banner"><b>{_E(rep['level'])}</b> — índice {rep['index']}/100 · tendencia {_E(rep['trend']['label'])}<br>{_E(rep['headline'])}</div>
<div class="kpis">"""]
    ip = rep["index_parts"]
    for k, v in [("Alertas en vivo", rep["live_total"]), ("Críticas (30 min)", ip.get("critical_30m", 0)), ("Incidentes cruzados", len(rep["incidents"])),
                 ("Posibles explosiones", len(rep["explosions"])), ("Comunicados", len(rep["statements"])), ("Telegram s/verificar", rep["telegram_unverified"])]:
        h.append(f'<div class="kpi"><div class="v">{v}</div><div class="k">{_E(k)}</div></div>')
    h.append("</div><h2>Hallazgos clave</h2><ul>" + "".join(f"<li>{_E(x)}</li>" for x in rep["findings"]) + "</ul>")
    h.append("<h2>Incidentes con cruce de fuentes</h2>")
    if rep["incidents"]:
        h.append("<table><tr><th>Lugar</th><th>Confianza</th><th>Punt.</th><th>Indep.</th><th>Severidad</th><th>Primera–última (UTC)</th><th>Grupos</th><th>Fuentes</th></tr>")
        for i in rep["incidents"]:
            h.append(f"<tr><td>{_E(i['place'])}</td><td class='c-{i['conf']}'>{_E(i['conf'])}</td><td>{i['score']}</td><td>{i['independent']}</td>"
                     f"<td>{_E(SEV_ES.get(i['severity'], i['severity']))}</td><td>{_E(i['first_seen'][11:16])}–{_E(i['last_seen'][11:16])}</td>"
                     f"<td>{_E(', '.join(i['groups']))}</td><td>{_E(', '.join(i['sources'])[:200])}</td></tr>")
        h.append("</table>")
    else:
        h.append('<p class="mut">Sin incidentes cruzados.</p>')
    h.append('<h2>Gráficos</h2><div class="grid">')
    h.append(svg_stack("Reportes por grupo en el tiempo", tl["labels"], tl["series"], GCOL))
    h.append(svg_donut("Reportes por grupo", sorted(rep["by_group"].items(), key=lambda x: -x[1]), GCOL))
    h.append(svg_donut("Severidad", [(k, rep["by_sev"].get(k, 0)) for k in ("critical", "high", "medium", "low")],
                       {"critical": "#e5484d", "high": "#f97316", "medium": "#eab308", "low": "#2f6fed"}))
    h.append(svg_donut("Credibilidad de la fuente (A oficial · B prensa/OSINT · C parte · D sin verificar)", [(k, rep["by_cred"].get(k, 0)) for k in "ABCD"],
                       {"A": "#17c964", "B": "#2f6fed", "C": "#f97316", "D": "#71717a"}))
    h.append(svg_hbar("Fuentes con más reportes", list(rep["by_source"].items()), "#2f6fed"))
    reg = rep["regions"]
    if reg.get("places"):
        h.append(svg_hbar("Zonas con más reportes", sorted(zip(reg["places"], [sum(r) for r in reg["cells"]]), key=lambda x: -x[1]), "#f97316"))
    ih = rep["index_history"]
    h.append(svg_line("Índice de actividad (histórico)", [x["ts"][5:] for x in ih], [x["idx"] for x in ih]))
    h.append("</div>")
    if reg.get("places"):
        h.append("<h2>Matriz zona × grupo</h2><table><tr><th>Zona</th>" + "".join(f"<th>{_E(g)}</th>" for g in reg["groups"]) + "</tr>")
        mx = max([c for r in reg["cells"] for c in r] + [1])
        for p, row in zip(reg["places"], reg["cells"]):
            h.append(f"<tr><td>{_E(p)}</td>" + "".join(f"<td style='text-align:center;background:rgba(229,72,77,{0.08 + 0.7 * c / mx if c else 0:.2f})'>{c or ''}</td>" for c in row) + "</tr>")
        h.append("</table>")
    h.append("<h2>Comunicados oficiales</h2>")
    if rep["statements"]:
        h.append("<table><tr><th>Hora</th><th>Fuente</th><th>Cred.</th><th>Sev.</th><th>Texto</th></tr>" + "".join(
            f"<tr><td>{_E(s['detected_at'][:16])}</td><td>{_E(s['source'])}</td><td>{_E(s['cred'])}</td><td>{_E(SEV_ES.get(s['severity'], ''))}</td>"
            f"<td>{_E(s['description'])}{(' <a href=\"' + _E(s['link']) + '\">↗</a>') if str(s.get('link') or '').startswith('http') else ''}</td></tr>" for s in rep["statements"]) + "</table>")
    else:
        h.append('<p class="mut">Ninguno relevante.</p>')
    h.append("<h2>Posibles explosiones sísmicas (heurístico)</h2>" + ("<ul>" + "".join(f"<li>{_E(e['detected_at'][:16])} — {_E(e['description'])}</li>" for e in rep["explosions"]) + "</ul>" if rep["explosions"] else '<p class="mut">Ninguna.</p>'))
    h.append("<h2>Medios estatales (versión de parte, no verificada)</h2>" + ("<ul>" + "".join(f"<li><b>{_E(s['source'])}</b>: {_E(s['description'])}</li>" for s in rep["state_media"]) + "</ul>" if rep["state_media"] else '<p class="mut">Ninguno.</p>'))
    if rep["watch_items"]:
        h.append("<h2>Lista de vigilancia</h2><ul>" + "".join(f"<li>{_E(w['source'])}: {_E(w['description'])} <span class='mut'>({_E(', '.join(w['tags']))})</span></li>" for w in rep["watch_items"]) + "</ul>")
    h.append("<h2>Estado de las fuentes</h2><table><tr><th>Fuente</th><th>Estado</th><th>Reportes</th><th>ms</th><th>Nota</th></tr>" + "".join(
        f"<tr><td>{_E(s['name'])}</td><td>{_E(s['status'])}{' (desactivada)' if s['disabled'] else ''}</td><td>{s['count']}</td><td>{s['ms']}</td><td>{_E(s['error'])}</td></tr>" for s in rep["sources"]) + "</table>")
    h.append("<h2>Avisos y metodología</h2><ul class='cav'>" + "".join(f"<li>{_E(c)}</li>" for c in CAVEATS) + "</ul></body></html>")
    return "".join(h)


def save_report(hours: float = 24.0) -> str:
    rep = collect_report(hours)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs(REPORT_DIR, exist_ok=True)
    for ext, body in (("html", render_html(rep)), ("md", render_md(rep)), ("json", json.dumps(rep, ensure_ascii=False, indent=1, default=str))):
        with open(os.path.join(REPORT_DIR, f"informe_{stamp}.{ext}"), "w", encoding="utf-8") as f:
            f.write(body)
    files = sorted(x for x in os.listdir(REPORT_DIR) if re.fullmatch(r"informe_\d{8}_\d{6}\.(html|md|json)", x))
    for old in files[:-REPORT_KEEP * 3]:
        try:
            os.remove(os.path.join(REPORT_DIR, old))
        except OSError:
            pass
    return stamp


def _report_loop():
    time.sleep(120)
    while True:
        try:
            save_report(24)
            log.info("Informe automático guardado")
        except Exception as e:
            log.warning(f"informe automático: {e}")
        time.sleep(max(60.0, REPORT_INTERVAL_MIN * 60))


# ------------------------------------------------------------
# Exportaciones
# ------------------------------------------------------------
def incidents_csv(incs: List[dict]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "place", "lat", "lon", "radius_km", "confidence", "score", "severity", "n_sources", "independent", "first_seen", "last_seen", "groups", "sources", "summary"])
    for i in incs:
        w.writerow([i["id"], i["place"], f"{i['lat']:.4f}", f"{i['lon']:.4f}", i["radius_km"], i["conf"], i["score"], i["severity"], i["n_sources"],
                    i["independent"], i["first_seen"], i["last_seen"], "|".join(i["groups"]), "|".join(i["sources"]), i["summary"]])
    return "\ufeff" + buf.getvalue()


def geojson_of(snap: Dict[str, Any]) -> dict:
    feats = []
    for a in snap["alerts"]:
        if hasgeo(a):
            feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [float(a["lon"]), float(a["lat"])]},
                          "properties": {"kind": "alert", "source": a["source"], "type": a.get("type"), "group": alert_group(a), "severity": a.get("severity"),
                                         "cred": a.get("cred"), "detected_at": a.get("detected_at"), "description": a.get("description"),
                                         "precise": is_precise(a), "incident_id": a.get("incident_id"), "link": a.get("link")}})
    for i in snap["stats"].get("incidents", []):
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [i["lon"], i["lat"]]},
                      "properties": {"kind": "incident", **{k: i[k] for k in ("id", "place", "conf", "score", "severity", "n_sources", "independent", "first_seen", "last_seen", "summary", "radius_km")}}})
    return {"type": "FeatureCollection", "features": feats}


@app.route("/api/report")
def v25_report():
    fmt = request.args.get("format", "html").lower()
    hours = request.args.get("hours", 24.0, type=float)
    rep = collect_report(hours)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if fmt == "json":
        body, mt, ext = json.dumps(rep, ensure_ascii=False, indent=1, default=str), "application/json", "json"
    elif fmt in ("md", "txt"):
        body, mt, ext = render_md(rep), "text/plain", "md"
    else:
        body, mt, ext = render_html(rep), "text/html", "html"
    hdr = {"Content-Disposition": f"attachment; filename=informe_{stamp}.{ext}"} if request.args.get("download") == "1" else {}
    return Response(body, mimetype=mt, headers=hdr)


@app.route("/api/report/generate", methods=["POST"])
def v25_report_generate():
    try:
        return jsonify({"ok": True, "stamp": save_report(float((request.get_json(silent=True) or {}).get("hours", 24)))})
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500


@app.route("/api/reports")
def v25_reports():
    out: Dict[str, List[str]] = defaultdict(list)
    if os.path.isdir(REPORT_DIR):
        for x in sorted(os.listdir(REPORT_DIR), reverse=True):
            m = re.fullmatch(r"informe_(\d{8}_\d{6})\.(html|md|json)", x)
            if m:
                out[m.group(1)].append(x)
    return jsonify({"interval_min": REPORT_INTERVAL_MIN, "dir": REPORT_DIR, "reports": [{"stamp": k, "files": v} for k, v in list(out.items())[:40]]})


@app.route("/api/reports/<fname>")
def v25_report_file(fname):
    if not re.fullmatch(r"informe_\d{8}_\d{6}\.(html|md|json)", fname):
        return jsonify({"error": "Nombre no válido"}), 400
    return send_from_directory(os.path.abspath(REPORT_DIR), fname, as_attachment=request.args.get("download") == "1")


@app.route("/api/export/incidents.csv")
def v25_export_incidents():
    snap = live_snapshot()
    return Response(incidents_csv(snap["stats"].get("incidents", [])), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=incidentes.csv"})


@app.route("/api/export/geojson")
def v25_export_geojson():
    return Response(json.dumps(geojson_of(live_snapshot()), ensure_ascii=False), mimetype="application/geo+json",
                    headers={"Content-Disposition": "attachment; filename=alertas.geojson"})


@app.route("/api/download/all")
def v25_download_all():
    hours = request.args.get("hours", 24.0, type=float)
    snap = live_snapshot()
    rep = collect_report(hours)
    rows = base.query_history(hours=hours, limit=5000)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("informe.html", render_html(rep))
        z.writestr("informe.md", render_md(rep))
        z.writestr("informe.json", json.dumps(rep, ensure_ascii=False, indent=1, default=str))
        z.writestr("alertas_en_vivo.json", json.dumps(snap["alerts"], ensure_ascii=False, default=str))
        z.writestr("incidentes.csv", incidents_csv(snap["stats"].get("incidents", [])))
        z.writestr("alertas.geojson", json.dumps(geojson_of(snap), ensure_ascii=False))
        z.writestr("estado_fuentes.json", json.dumps(snap["stats"].get("source_status", {}), ensure_ascii=False, indent=1))
        c = io.StringIO()
        w = csv.writer(c)
        cols = ["detected_at", "source", "type", "severity", "cred", "region", "lat", "lon", "description", "link"]
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(k, "") for k in cols])
        z.writestr("historial.csv", "\ufeff" + c.getvalue())
        if base.LAST_DIAG.get("data"):
            z.writestr("diagnostico_feeds.txt", base.diag_report_text(base.LAST_DIAG["data"]))
        z.writestr("LEEME.txt", "Paquete generado por OSINT Early Warning v%s\n%s\n\n%s\n" % (VERSION, rep["generated_at"], "\n".join(CAVEATS)))
    return Response(buf.getvalue(), mimetype="application/zip",
                    headers={"Content-Disposition": f"attachment; filename=paquete_osint_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.zip"})


@app.route("/api/analytics")
def v25_analytics():
    hours = max(1.0, min(request.args.get("hours", 24.0, type=float), 168.0))
    rows = base.query_history(hours=hours, limit=5000)
    return jsonify({"hours": hours, "group_timeline": hist_group_timeline(rows, hours),
                    "by_group": dict(Counter(GROUP_OF_TYPE.get(r.get("type"), "Otros") for r in rows)),
                    "by_cred": dict(Counter(r.get("cred", "D") for r in rows)),
                    "by_source": dict(Counter(r["source"] for r in rows).most_common(8)),
                    "index_history": index_history(hours)})


@app.route("/api/notify/status")
def v25_notify_status():
    return jsonify({"webhook": bool(NOTIFY_WEBHOOK_URL), "ntfy": bool(NTFY_URL), "telegram": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
                    "report_interval_min": REPORT_INTERVAL_MIN})


@app.route("/api/notify/test", methods=["POST"])
def v25_notify_test():
    sent = send_external("✅ Prueba de notificación — OSINT Monitor v" + VERSION)
    return jsonify({"sent": sent}) if sent else (jsonify({"error": "Ningún canal configurado"}), 400)


# ============================================================
# 6) INTERFAZ (inyectada en el panel existente)
# ============================================================
EXTRA_UI = r"""
<script>
(function(){
const st=document.createElement('style');
st.textContent=`.tab{padding:10px 2px!important;font-size:9px!important}
.mx{border-collapse:collapse;font-size:10px;width:100%}.mx td,.mx th{border:1px solid var(--border);padding:3px;text-align:center}.mx td:first-child,.mx th:first-child{text-align:left}
.inc{border-left-color:var(--yellow)}details summary{cursor:pointer}
.tag-vigilado{color:var(--red);border-color:var(--red)}
.tt-h{font-weight:bold;margin-bottom:3px}.tt-m{opacity:.75;font-size:10px}.tt-b{margin:4px 0;line-height:1.35;word-break:break-word}.tt-x{margin-top:5px;border-top:1px solid #444;padding-top:4px}
.chip{display:inline-block;border:1px solid var(--border);border-radius:10px;padding:1px 7px;margin:2px;font-size:10px}.chip span{cursor:pointer;color:var(--red);margin-left:4px}`;
document.head.appendChild(st);
const tabs=document.querySelector('.tabs'), side=document.querySelector('.sidebar');
tabs.insertAdjacentHTML('beforeend','<div class="tab" data-tab="links">🔗 CRUCES</div><div class="tab" data-tab="report">📑 INFORME</div>');
side.insertAdjacentHTML('beforeend',`
<div class="content" id="tab-links">
 <div class="frow" style="margin-bottom:10px"><select id="lkRange" class="fi" onchange="loadLinks()"><option value="6">6 h</option><option value="24" selected>24 h</option><option value="72">72 h</option><option value="168">7 d</option></select>
 <button class="btn" style="flex:0 0 auto" onclick="loadLinks()">⟳</button></div>
 <div class="kpis" id="lkKpis"></div>
 <div class="chart-box"><h3>🔗 INCIDENTES CON CRUCE DE FUENTES</h3><div class="hint">Reportes de fuentes distintas cercanos en espacio y tiempo. «Independientes» no cuenta alertas del mismo sistema ni mensajes casi idénticos. Más cruce = más confianza, no certeza.</div><div id="lkList"></div></div>
 <div class="chart-box"><h3>MATRIZ ZONA × GRUPO</h3><div id="lkMatrix"></div></div>
 <div class="chart-box"><h3>RADAR — ACTIVIDAD POR GRUPO</h3><canvas id="chRadar"></canvas></div>
 <div class="chart-box"><h3>ÁREA APILADA — GRUPOS EN EL TIEMPO</h3><canvas id="chGrpTL"></canvas></div>
 <div class="chart-box"><h3>BURBUJAS — INCIDENTES (tiempo · fuentes indep. · puntuación)</h3><canvas id="chBubble"></canvas></div>
 <div class="chart-box"><h3>POLAR — FUENTES CON MÁS REPORTES</h3><canvas id="chPolar"></canvas></div>
 <div class="chart-box"><h3>DISPERSIÓN — SISMOS (profundidad vs magnitud)</h3><canvas id="chSeis"></canvas></div>
 <div class="chart-box"><h3>HISTÓRICO DEL ÍNDICE DE ACTIVIDAD</h3><canvas id="chIdx"></canvas></div>
</div>
<div class="content" id="tab-report">
 <div class="frow" style="margin-bottom:8px"><select id="rpRange" class="fi" onchange="repPreview()"><option value="6">Ventana 6 h</option><option value="12">12 h</option><option value="24" selected>24 h</option><option value="72">72 h</option><option value="168">7 d</option></select>
 <button class="btn btn-blue" style="flex:0 0 auto" onclick="repPreview()">👁 VISTA PREVIA</button></div>
 <div class="frow" style="margin-bottom:8px">
 <button class="btn" onclick="location.href=repUrl('html',1)">⬇ HTML</button><button class="btn" onclick="location.href=repUrl('md',1)">⬇ MD</button><button class="btn" onclick="location.href=repUrl('json',1)">⬇ JSON</button>
 <button class="btn" onclick="location.href='/api/export/incidents.csv'">⬇ INCIDENTES CSV</button><button class="btn" onclick="location.href='/api/export/geojson'">⬇ GEOJSON</button>
 <button class="btn btn-orange" onclick="location.href='/api/download/all?hours='+document.getElementById('rpRange').value">📦 TODO (ZIP)</button>
 <button class="btn" onclick="repSave()">💾 GUARDAR AHORA</button></div>
 <iframe id="rpFrame" style="width:100%;height:460px;border:1px solid var(--border);background:#fff;border-radius:3px"></iframe>
 <div class="chart-box" style="margin-top:10px"><h3>INFORMES AUTOMÁTICOS GUARDADOS</h3><div id="rpList" class="hint">Cargando...</div></div>
 <div class="chart-box"><h3>👁 LISTA DE VIGILANCIA</h3><div class="hint">Palabras clave y zonas que quieres seguir. Las alertas que coincidan se marcan «vigilado» y se notifican.</div>
 <div class="frow"><input id="wKw" class="fi" placeholder="palabra clave (ej. zaporizhzhia nuclear)"><button class="btn" style="flex:0 0 auto" onclick="wAddKw()">＋</button></div>
 <div class="frow" style="margin-top:6px"><input id="wZn" class="fi" placeholder="zona" style="flex:2"><input id="wLa" class="fi" placeholder="lat" style="flex:1"><input id="wLo" class="fi" placeholder="lon" style="flex:1"><input id="wKm" class="fi" placeholder="km" value="150" style="flex:1"><button class="btn" style="flex:0 0 auto" onclick="wAddZone()">＋</button></div>
 <div id="wList" style="margin-top:8px"></div></div>
 <div class="chart-box"><h3>🔔 NOTIFICACIONES EXTERNAS</h3><div id="nStatus" class="hint"></div><button class="btn" onclick="nTest()">ENVIAR PRUEBA</button></div>
</div>`);
side.querySelector('#tab-search').insertAdjacentHTML('beforeend',`<div class="chart-box"><h3>GRUPOS DE CANALES</h3>
 <div class="frow"><input id="gNew" class="fi" placeholder="nombre del nuevo grupo"><button class="btn" style="flex:0 0 auto" onclick="tgGroupCreate()">CREAR</button></div><div id="tgGroups" style="margin-top:8px"></div></div>`);
document.querySelector('.map-controls').insertAdjacentHTML('beforeend','<button class="btn active" id="btnLinks" onclick="toggleLinks()" title="Mostrar cruces entre fuentes">CRUCES</button><button class="btn" id="btnHeat" onclick="toggleHeat()" title="Mapa de calor">CALOR</button>');
tabs.querySelectorAll('.tab[data-tab="links"],.tab[data-tab="report"]').forEach(t=>t.addEventListener('click',()=>openTab(t.dataset.tab)));
tabs.querySelector('.tab[data-tab="search"]').addEventListener('click',()=>{tgGroupsLoad();});
})();

const GRP_COLORS={'Alertas':'#ff3344','Sísmico':'#aa66ff','Satélite':'#ff8800','Naturales':'#00ccff','Prensa/OSINT':'#4488ff','Gobierno':'#00ff88','Estatal':'#ffcc00','Telegram':'#5ac8fa','Conflicto':'#ff66aa','Aviación':'#cccccc','Otros':'#666666'};
const GRP_OF_TYPE={missile_alert:'Alertas',air_raid:'Alertas',kyiv_alert:'Alertas',drone_missile_track:'Alertas',earthquake:'Sísmico',seismic_explosion:'Sísmico',explosion_candidate:'Sísmico',nuclear_test_candidate:'Sísmico',thermal_anomaly:'Satélite',natural_event:'Naturales',disaster:'Naturales',news:'Prensa/OSINT',news_geo:'Prensa/OSINT',war_news:'Prensa/OSINT',official_statement:'Gobierno',state_media:'Estatal',telegram_signal:'Telegram',acled_conflict:'Conflicto',conflict_event:'Conflicto',flight_activity:'Aviación'};
function grpOf(a){return a.group||GRP_OF_TYPE[a.type]||'Otros';}
let lastStats={}, lastIncidents=[], lastPts=[], showLinks=true, showHeat=false, tgGroupsCache=[], incSig='', seenInc=new Set();

function openTab(name){
 document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.tab===name));
 document.querySelectorAll('.content').forEach(x=>x.classList.toggle('active',x.id==='tab-'+name));
 if(name==='links')loadLinks(); if(name==='report')initReportTab();
}

/* ---------- filtro por grupo ---------- */
function ensureGrpSelect(){
 let sel=document.getElementById('fGrp');
 if(!sel){const row=document.getElementById('fCred').parentElement;sel=document.createElement('select');sel.id='fGrp';sel.className='fi';sel.onchange=onFilterChange;row.appendChild(sel);}
 const cur=sel.value;
 sel.innerHTML='<option value="">Grupo: todos</option><option value="__watch">★ Vigilados</option>'+Object.keys(GRP_COLORS).map(g=>`<option value="${esc(g)}">${esc(g)}</option>`).join('')+tgGroupsCache.map(g=>`<option value="tg:${esc(g.name)}">TG · ${esc(g.name)}</option>`).join('');
 sel.value=cur;
}
function getVisible(){
 const q=(document.getElementById('fSearch').value||'').trim().toLowerCase();
 const sev=document.getElementById('fSev').value, src=document.getElementById('fSrc').value, typ=document.getElementById('fType').value, cred=document.getElementById('fCred').value;
 const grp=(document.getElementById('fGrp')||{}).value||'';
 return baseAlerts().filter(a=>(!sev||a.severity===sev)&&(!src||a.source===src)&&(!typ||typeGroup(a)===typ||(typ==='sismo'&&typeGroup(a)==='explosion'))&&(!cred||(a.cred||'D')===cred)
  &&(!grp||(grp.indexOf('tg:')===0?(a.tags||[]).includes('grupo:'+grp.slice(3)):grp==='__watch'?(a.tags||[]).includes('vigilado'):grpOf(a)===grp))
  &&(!q||((a.source||'')+' '+(a.description||'')+' '+(a.region||'')+' '+(a.type||'')+' '+(a.tags||[]).join(' ')).toLowerCase().includes(q)));
}

/* ---------- tooltip detallado ---------- */
function ageTxt(iso){const s=(Date.now()-new Date(iso).getTime())/1000;if(!isFinite(s))return '';if(s<90)return 'hace '+Math.round(s)+' s';if(s<5400)return 'hace '+Math.round(s/60)+' min';if(s<172800)return 'hace '+(s/3600).toFixed(1)+' h';return 'hace '+Math.round(s/86400)+' d';}
function detailHtml(a,near){
 const cred=a.cred||'D', raw=a.raw||{}, tr=trCache.get(trKey(a.description||''));
 let x='';
 if(typeof raw.mag==='number')x+=`<div>Magnitud <b>M${raw.mag.toFixed(1)}</b>${raw.depth!=null?' · prof. '+Number(raw.depth).toFixed(0)+' km':''}</div>`;
 if(raw.frp)x+=`<div>Potencia radiativa (FRP): <b>${Number(raw.frp).toFixed(0)} MW</b></div>`;
 if(raw.count!=null&&a.type==='flight_activity')x+=`<div>Aeronaves en vuelo: <b>${esc(raw.count)}</b>${raw.median?' (mediana '+Number(raw.median).toFixed(0)+')':''}</div>`;
 if(raw.views)x+=`<div>Vistas: ${esc(raw.views)} · canal @${esc(raw.channel||'')}</div>`;
 if(raw.domain)x+=`<div>Medio: ${esc(raw.domain)}${raw.country?' · '+esc(raw.country):''}</div>`;
 const tags=(a.tags||[]).map(t=>`<span class="sevtag tagw">${esc(t)}</span>`).join('');
 const corr=a.incident_id?`<div class="tt-x">🔗 <b>Cruzado con ${a.corr_n} fuente(s) independiente(s)</b> · confianza <b>${esc(a.corr_conf)}</b><br><span style="opacity:.8">${esc((a.corr_sources||[]).join(' · '))}</span></div>`:'<div class="tt-x" style="opacity:.6">Sin cruce con otras fuentes cercanas en espacio/tiempo</div>';
 const nr=(near&&near.length)?`<div class="tt-x" style="opacity:.85">＋ ${near.length} reporte(s) más en este punto: ${esc(near.slice(0,4).map(n=>n.source).join(' · '))}${near.length>4?'…':''}</div>`:'';
 const geo=hasGeo(a)?`${Number(a.lat).toFixed(3)}, ${Number(a.lon).toFixed(3)}${a.geo_approx?' (aprox.)':''}`:'sin coordenadas';
 return `<div class="tt-h">${esc(a.source)} <span class="credtag cred-${esc(cred)}">${esc(cred)}</span> <span class="sevtag">${esc(a.severity||'')}</span></div>
 <div class="tt-m">${esc(grpOf(a))} · ${esc(a.type||'')} · ${esc(fmtDateTime(a.detected_at))} (${esc(ageTxt(a.detected_at))})</div>
 <div class="tt-m">📍 ${esc(a.place||a.region||'')} · ${esc(geo)}</div>
 <div class="tt-b">${esc(String(a.description||'').slice(0,600))}</div>
 ${(tr&&tr.text&&!tr.same)?`<div class="tt-b" style="color:#00ff88">🌐 ${esc(tr.text.slice(0,500))}</div>`:''}${x}<div>${tags}</div>${corr}${nr}
 ${safeUrl(a.link)?'<div class="tt-m" style="margin-top:4px">clic en el punto para abrir la ficha y el enlace de la fuente</div>':''}`;
}
function incidentHtml(i){
 const col={alta:'#00ff88',media:'#ff8800',baja:'#888'}[i.conf];
 return `<div class="tt-h">🔗 Incidente — ${esc(i.place)}</div><div class="tt-m">confianza <b style="color:${col}">${esc(i.conf)}</b> · puntuación ${i.score}/100 · severidad ${esc(i.severity)}</div>
 <div class="tt-b">${esc(i.summary)}</div><div class="tt-m">Fuentes independientes ≈ ${i.independent} · radio ${i.radius_km} km</div>
 <div class="tt-x">${i.members.slice(-6).map(m=>`<div>• <span class="credtag cred-${esc(m.cred)}">${esc(m.cred)}</span> ${esc(m.source)}: ${esc(String(m.description).slice(0,90))}</div>`).join('')}</div>`;
}
function popupCard(a,near){return `<div style="color:#111;font-family:'Courier New',monospace;font-size:11px;max-width:320px">${detailHtml(a,near)}${safeUrl(a.link)?`<a href="${esc(safeUrl(a.link))}" target="_blank" rel="noopener">abrir fuente ↗</a>`:''}</div>`;}
function nearOf(obj){return lastPts.filter(p=>p!==obj&&Math.abs(p.coordinates[1]-obj.coordinates[1])<0.2&&Math.abs(p.coordinates[0]-obj.coordinates[0])<0.3).map(p=>p.a);}
function tipFn(info){
 const o=info&&info.object; if(!o)return null;
 const html=o.kind==='incident'?incidentHtml(o.inc):detailHtml(o.a,nearOf(o));
 return {html,style:{background:'#111',border:'1px solid #4488ff',color:'#e0e0e0',fontFamily:'Courier New,monospace',fontSize:'11px',maxWidth:'380px',whiteSpace:'normal',padding:'8px',borderRadius:'4px',zIndex:'1000'}};
}
function clickFn(info){
 const o=info&&info.object; if(!o)return;
 const html=o.kind==='incident'?incidentHtml(o.inc).replace(/color:#e0e0e0/g,''):popupCard(o.a,nearOf(o));
 new maplibregl.Popup({closeButton:true,maxWidth:'360px'}).setLngLat(o.coordinates).setHTML(o.kind==='incident'?`<div style="color:#111;font-family:'Courier New',monospace;font-size:11px">${html}</div>`:html).addTo(map);
}
function updateMapFallback2(pts){
 fbMarkers.forEach(m=>m.remove());fbMarkers=[];
 pts.slice(0,500).forEach(p=>{
  const a=p.a, el=document.createElement('div');
  el.style.cssText='width:12px;height:12px;border-radius:50%;border:1px solid #000;cursor:pointer;background:'+(p.grp==='explosion'?'#aa66ff':sevCss(a.severity));
  if(a.emoji){el.textContent=a.emoji;el.style.cssText='font-size:20px;line-height:20px;cursor:pointer;';}
  const pop=new maplibregl.Popup({closeButton:false,offset:10,maxWidth:'340px'}).setHTML(popupCard(a,nearOf(p)));
  el.addEventListener('mouseenter',()=>pop.setLngLat(p.coordinates).addTo(map));el.addEventListener('mouseleave',()=>pop.remove());
  fbMarkers.push(new maplibregl.Marker({element:el}).setLngLat(p.coordinates).addTo(map));
 });
}
function updateMap(){
 if(!map)return; if(!mapReady){mapPending=true;return;}
 const pts=getVisible().filter(hasGeo).map(a=>({kind:'alert',coordinates:[Number(a.lon),Number(a.lat)],a,grp:typeGroup(a)}));
 lastPts=pts; initOverlay();
 if(!overlay){updateMapFallback2(pts);return;}
 const layers=[];
 if(showHeat&&deck.HeatmapLayer)layers.push(new deck.HeatmapLayer({id:'heat',data:pts,getPosition:d=>d.coordinates,getWeight:d=>({critical:4,high:2.5,medium:1.2,low:.5}[d.a.severity]||.5),radiusPixels:45,intensity:1.4,threshold:0.05}));
 if(showLinks&&lastIncidents.length){
  const links=[],incPts=lastIncidents.map(i=>({kind:'incident',coordinates:[i.lon,i.lat],inc:i}));
  lastIncidents.forEach(i=>i.members.forEach(m=>links.push({from:[i.lon,i.lat],to:[m.lon,m.lat],conf:i.conf})));
  if(deck.LineLayer)layers.push(new deck.LineLayer({id:'links',data:links,getSourcePosition:d=>d.from,getTargetPosition:d=>d.to,getColor:[255,204,0,150],getWidth:1.5,widthUnits:'pixels'}));
  layers.push(new deck.ScatterplotLayer({id:'inc',data:incPts,getPosition:d=>d.coordinates,getRadius:d=>Math.max(30000,d.inc.radius_km*1000+20000),stroked:true,filled:false,
   getLineColor:d=>d.inc.conf==='alta'?[0,255,136,230]:d.inc.conf==='media'?[255,136,0,230]:[160,160,160,200],lineWidthMinPixels:2,radiusMinPixels:16,radiusMaxPixels:70,pickable:true}));
 }
 layers.push(new deck.ScatterplotLayer({id:'alerts',data:pts,getPosition:d=>d.coordinates,getRadius:d=>d.a.severity==='critical'?150000:80000,
  getFillColor:d=>d.grp==='explosion'?[170,102,255,230]:d.a.severity==='critical'?[255,51,68,220]:d.a.severity==='high'?[255,136,0,200]:[0,255,136,180],
  radiusMinPixels:5,radiusMaxPixels:30,pickable:true,autoHighlight:true,highlightColor:[255,255,255,120]}));
 if(deck.TextLayer)layers.push(new deck.TextLayer({id:'emoji',data:pts.filter(p=>p.a.emoji),getPosition:d=>d.coordinates,getText:d=>d.a.emoji,getSize:22,sizeUnits:'pixels',getTextAnchor:'middle',getAlignmentBaseline:'center',pickable:false,fontFamily:'Apple Color Emoji, Segoe UI Emoji, Noto Color Emoji, sans-serif',characterSet:'auto'}));
 overlay.setProps({layers,getTooltip:tipFn,onClick:clickFn,pickingRadius:8});
}
function toggleLinks(){showLinks=!showLinks;document.getElementById('btnLinks').classList.toggle('active',showLinks);updateMap();}
function toggleHeat(){showHeat=!showHeat;document.getElementById('btnHeat').classList.toggle('active',showHeat);updateMap();}

/* ---------- hook de estadísticas ---------- */
const _renderSummary0=renderSummary;
renderSummary=function(stats){
 _renderSummary0(stats); lastStats=stats||{}; lastIncidents=lastStats.incidents||[];
 const sig=lastIncidents.map(i=>i.id+i.n_alerts).join(',');
 if(sig!==incSig){incSig=sig;updateMap();}
 lastIncidents.forEach(i=>{if(!seenInc.has(i.id)){seenInc.add(i.id);if((i.conf==='alta'||i.conf==='media')&&(i.severity==='high'||i.severity==='critical'))notify('🔗 Incidente cruzado ['+i.conf+']',i.summary);}});
 ensureGrpSelect();
 if(document.getElementById('tab-links').classList.contains('active'))renderLinks();
};

/* ---------- pestaña CRUCES ---------- */
function incCard(i,idx){
 const col={alta:'var(--green)',media:'var(--orange)',baja:'#888'}[i.conf];
 return `<div class="alert-item inc" style="border-left-color:${col}" onclick="flyInc(${idx})"><span class="src">🔗 ${esc(i.place)}</span><span class="sevtag">${esc(i.severity)}</span>
 <span class="sevtag" style="color:${col};border-color:${col}">confianza ${esc(i.conf)} · ${i.score}</span><span class="time">${esc(fmtTime(i.last_seen))}</span>
 <div class="desc">${esc(i.summary)}</div><div class="desc" style="color:var(--dim)">Independientes≈${i.independent} · ${esc(i.sources.join(', '))}</div>
 <details onclick="event.stopPropagation()"><summary class="hint">ver ${i.members.length} reportes</summary>${i.members.map(m=>`<div class="tg-msg"><span class="tm">${esc(fmtTime(m.detected_at))}</span> <span class="credtag cred-${esc(m.cred)}">${esc(m.cred)}</span> <b>${esc(m.source)}</b> — ${esc(m.description)}${safeUrl(m.link)?` <a class="lnk" href="${esc(safeUrl(m.link))}" target="_blank" rel="noopener">↗</a>`:''}</div>`).join('')}</details></div>`;
}
function flyInc(i){const x=lastIncidents[i];if(x)map.flyTo({center:[x.lon,x.lat],zoom:7,duration:900});}
function renderLinks(){
 const inc=lastIncidents, s=lastStats||{};
 const alta=inc.filter(i=>i.conf==='alta').length, ind=inc.length?(inc.reduce((a,i)=>a+i.independent,0)/inc.length).toFixed(1):'0';
 document.getElementById('lkKpis').innerHTML=[['Incidentes',inc.length,'var(--text)'],['Confianza alta',alta,'var(--green)'],['Indep. media',ind,'var(--blue)'],['Índice',(s.index||0)+'/100','var(--orange)'],['Nivel',s.level||'—','var(--yellow)'],['Vigilados',s.watch_hits||0,'var(--red)']].map(([l,v,c])=>`<div class="kpi"><div class="kv" style="color:${c};font-size:${String(v).length>6?13:22}px">${esc(v)}</div><div class="kl">${esc(l)}</div></div>`).join('');
 document.getElementById('lkList').innerHTML=inc.length?inc.map(incCard).join(''):'<div class="hint">Sin cruces en la ventana actual.</div>';
 const m=s.matrix;
 if(!m||!m.places||!m.places.length)document.getElementById('lkMatrix').innerHTML='<div class="hint">Sin datos geolocalizados</div>';
 else{const mx=Math.max(1,...m.cells.flat());
  document.getElementById('lkMatrix').innerHTML='<table class="mx"><tr><th>Zona</th>'+m.groups.map(g=>`<th>${esc(g.slice(0,8))}</th>`).join('')+'</tr>'+m.places.map((p,r)=>`<tr><td>${esc(p)}</td>${m.cells[r].map(c=>`<td style="background:rgba(255,51,68,${c?(0.12+0.7*c/mx).toFixed(2):0})">${c||''}</td>`).join('')}</tr>`).join('')+'</table>';}
 const live=s.by_group||{}, gk=Object.keys(live);
 mkChart('chRadar',{type:'radar',data:{labels:gk,datasets:[{label:'Alertas en vivo',data:gk.map(k=>live[k]),backgroundColor:'rgba(255,51,68,.25)',borderColor:'#ff3344',pointBackgroundColor:'#ff3344'}]},
  options:{responsive:true,plugins:{legend:{labels:{color:'#aaa',font:{size:10}}}},scales:{r:{ticks:{color:'#888',backdropColor:'transparent'},grid:{color:'#333'},angleLines:{color:'#333'},pointLabels:{color:'#aaa',font:{size:9}}}}}});
 mkChart('chBubble',{type:'bubble',data:{datasets:['alta','media','baja'].map(cf=>({label:cf,backgroundColor:{alta:'rgba(0,255,136,.55)',media:'rgba(255,136,0,.55)',baja:'rgba(150,150,150,.5)'}[cf],
   data:inc.filter(i=>i.conf===cf).map(i=>({x:-(Date.now()-new Date(i.last_seen).getTime())/3600000,y:i.independent,r:Math.max(5,i.score/6),l:i.place+' · '+i.score+' pts'}))}))},
  options:{responsive:true,plugins:{legend:{labels:{color:'#aaa',font:{size:10}}},tooltip:{callbacks:{label:c=>c.raw.l}}},scales:{x:Object.assign({title:{display:true,text:'horas (0 = ahora)',color:'#888'}},AX),y:Object.assign({beginAtZero:true,title:{display:true,text:'fuentes indep.',color:'#888'}},AX)}}});
 const sp=s.seismic_points||[], cls={earthquake:'#4488ff',explosion_candidate:'#aa66ff',seismic_explosion:'#ff3344',nuclear_test_candidate:'#ff3344'};
 mkChart('chSeis',{type:'scatter',data:{datasets:Object.keys(cls).map(t=>({label:t,backgroundColor:cls[t],data:sp.filter(p=>p.type===t&&p.depth!=null).map(p=>({x:Number(p.depth),y:Number(p.mag)}))}))},
  options:{responsive:true,plugins:{legend:{labels:{color:'#aaa',font:{size:10}}}},scales:{x:Object.assign({title:{display:true,text:'profundidad (km)',color:'#888'}},AX),y:Object.assign({title:{display:true,text:'magnitud',color:'#888'}},AX)}}});
}
async function loadLinks(){
 renderLinks();
 try{
  const r=await fetch('/api/analytics?hours='+(document.getElementById('lkRange').value||24)); const j=await r.json(); if(!r.ok)throw new Error(j.error||r.status);
  const tl=j.group_timeline||{labels:[],series:{}};
  mkChart('chGrpTL',{type:'line',data:{labels:tl.labels,datasets:Object.keys(tl.series).map(g=>({label:g,data:tl.series[g],fill:true,borderColor:GRP_COLORS[g]||'#888',backgroundColor:(GRP_COLORS[g]||'#888')+'66',tension:.3,pointRadius:0}))},
   options:{responsive:true,plugins:{legend:{labels:{color:'#aaa',font:{size:9}}}},scales:{x:Object.assign({},AX,{ticks:{color:'#888',font:{size:9},maxTicksLimit:6}}),y:Object.assign({stacked:true,beginAtZero:true},AX)}}});
  const bs=Object.entries(j.by_source||{});
  mkChart('chPolar',{type:'polarArea',data:{labels:bs.map(x=>x[0]),datasets:[{data:bs.map(x=>x[1]),backgroundColor:['#ff3344aa','#ff8800aa','#4488ffaa','#00ff88aa','#aa66ffaa','#ffcc00aa','#00ccffaa','#ff66aaaa']}]},
   options:{responsive:true,plugins:{legend:{position:'right',labels:{color:'#aaa',font:{size:9}}}},scales:{r:{ticks:{color:'#888',backdropColor:'transparent'},grid:{color:'#333'}}}}});
  const ih=j.index_history||[];
  mkChart('chIdx',{type:'line',data:{labels:ih.map(x=>x.ts.slice(5).replace('T',' ')),datasets:[{label:'Índice',data:ih.map(x=>x.idx),borderColor:'#ff3344',backgroundColor:'#ff334455',fill:true,tension:.3,pointRadius:0},{label:'Incidentes',data:ih.map(x=>x.incidents),borderColor:'#ffcc00',tension:.3,pointRadius:0}]},
   options:{responsive:true,plugins:{legend:{labels:{color:'#aaa',font:{size:10}}}},scales:{x:Object.assign({},AX,{ticks:{color:'#888',font:{size:9},maxTicksLimit:6}}),y:Object.assign({beginAtZero:true},AX)}}});
 }catch(e){toast('Análisis: '+e.message);}
}
setInterval(()=>{if(document.getElementById('tab-links').classList.contains('active'))loadLinks();},45000);

/* ---------- pestaña INFORME ---------- */
function repUrl(fmt,dl){return '/api/report?format='+fmt+'&hours='+document.getElementById('rpRange').value+(dl?'&download=1':'');}
function repPreview(){document.getElementById('rpFrame').src=repUrl('html');}
async function repSave(){try{const r=await fetch('/api/report/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({hours:Number(document.getElementById('rpRange').value)})});const j=await r.json();if(!r.ok)throw new Error(j.error);toast('Informe guardado: '+j.stamp);loadReports();}catch(e){toast('Error: '+e.message);}}
async function loadReports(){
 try{const j=await (await fetch('/api/reports')).json();
  document.getElementById('rpList').innerHTML=(j.interval_min>0?`Auto-informe cada ${j.interval_min} min en <code>${esc(j.dir)}/</code><br>`:'Auto-informe desactivado<br>')+((j.reports||[]).map(r=>`<div class="st-row"><span class="nm">${esc(r.stamp)}</span>${r.files.map(f=>`<a class="btn" href="/api/reports/${esc(f)}?download=1">${esc(f.split('.').pop().toUpperCase())}</a>`).join('')}</div>`).join('')||'Aún no hay informes guardados (el primero se genera a los 2 min).');
 }catch(e){document.getElementById('rpList').textContent=e.message;}
}
async function loadWatch(){
 try{const w=await (await fetch('/api/watchlist')).json();
  document.getElementById('wList').innerHTML=(w.keywords||[]).map(k=>`<span class="chip">🔎 ${esc(k)}<span data-k="${esc(k)}" onclick="wDel('keyword',this.dataset.k)">✕</span></span>`).join('')+(w.zones||[]).map(z=>`<span class="chip">📍 ${esc(z.name)} (${esc(z.radius_km)} km)<span data-k="${esc(z.name)}" onclick="wDel('zone',this.dataset.k)">✕</span></span>`).join('')||'<span class="hint">Vacía</span>';
 }catch(e){}
}
async function wPost(b){const r=await fetch('/api/watchlist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});const j=await r.json();if(!r.ok)toast(j.error||'Error');else{loadWatch();loadData(true);}}
function wAddKw(){const v=document.getElementById('wKw').value.trim();if(v){wPost({keyword:v});document.getElementById('wKw').value='';}}
function wAddZone(){wPost({zone:{name:document.getElementById('wZn').value,lat:document.getElementById('wLa').value,lon:document.getElementById('wLo').value,radius_km:document.getElementById('wKm').value}});}
async function wDel(kind,v){await fetch('/api/watchlist?'+kind+'='+encodeURIComponent(v),{method:'DELETE'});loadWatch();loadData(true);}
async function loadNotify(){try{const j=await (await fetch('/api/notify/status')).json();document.getElementById('nStatus').innerHTML=`Webhook: ${j.webhook?'✅':'—'} · ntfy: ${j.ntfy?'✅':'—'} · Telegram bot: ${j.telegram?'✅':'—'}<br>Se envía: incidentes cruzados (confianza alta/media), comunicados graves, explosiones correlacionadas y coincidencias de vigilancia. Configúralo con NOTIFY_WEBHOOK_URL, NTFY_URL o TELEGRAM_BOT_TOKEN+TELEGRAM_CHAT_ID.`;}catch(e){}}
async function nTest(){const r=await fetch('/api/notify/test',{method:'POST'});const j=await r.json();toast(r.ok?'Enviado a: '+j.sent.join(', '):(j.error||'Error'));}
function initReportTab(){repPreview();loadReports();loadWatch();loadNotify();}

/* ---------- grupos de Telegram ---------- */
async function tgGroupsLoad(){
 try{const j=await (await fetch('/api/telegram/groups')).json();tgGroupsCache=j.groups||[];ensureGrpSelect();
  document.getElementById('tgGroups').innerHTML=tgGroupsCache.map((g,i)=>`<div class="st-row" style="flex-wrap:wrap"><span class="dot" style="background:${g.enabled?'var(--green)':(g.active?'var(--orange)':'var(--dim)')}"></span>
  <span class="nm"><b>${esc(g.name)}</b> <span class="meta">${g.active}/${g.members.length} activos</span></span>
  <button class="btn" data-g="${esc(g.name)}" data-e="${g.enabled?0:1}" onclick="tgGroupToggle(this.dataset.g,this.dataset.e==='1')">${g.enabled?'DESACTIVAR':'ACTIVAR'} TODO</button>
  <button class="btn btn-red" data-g="${esc(g.name)}" onclick="tgGroupDelete(this.dataset.g)">✕</button></div>
  <div>${g.members.map(m=>`<span class="chip" title="${esc(m.name)}" style="${m.enabled?'':'opacity:.5'}">@${esc(m.id)}<span data-g="${esc(g.name)}" data-u="${esc(m.id)}" onclick="tgGroupEdit(this.dataset.g,[],[this.dataset.u])">✕</span></span>`).join('')}</div>
  <div class="frow" style="margin:4px 0 10px"><input id="gadd-${i}" class="fi" placeholder="@canal para añadir al grupo"><button class="btn" style="flex:0 0 auto" data-g="${esc(g.name)}" data-i="${i}" onclick="tgGroupEdit(this.dataset.g,[document.getElementById('gadd-'+this.dataset.i).value],[])">＋</button></div>`).join('')||'<div class="hint">Sin grupos</div>';
 }catch(e){}
}
async function tgGroupEdit(name,add,remove){
 const r=await fetch('/api/telegram/groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,add:add.filter(Boolean),remove})});const j=await r.json();
 if(!r.ok)toast(j.error||'Error');else if((j.warnings||[]).length)toast(j.warnings.join('; '));
 tgGroupsLoad();tgLoadChannels();loadData(true);
}
function tgGroupCreate(){const n=document.getElementById('gNew').value.trim();if(n){tgGroupEdit(n,[],[]);document.getElementById('gNew').value='';}}
async function tgGroupToggle(name,enabled){await fetch('/api/telegram/groups/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,enabled})});toast((enabled?'Activado: ':'Desactivado: ')+name);tgGroupsLoad();tgLoadChannels();loadData(true);}
async function tgGroupDelete(name){if(!confirm('¿Borrar el grupo «'+name+'»? (los canales siguen monitorizados)'))return;await fetch('/api/telegram/groups/'+encodeURIComponent(name),{method:'DELETE'});tgGroupsLoad();}
window.addEventListener('DOMContentLoaded',()=>{ensureGrpSelect();tgGroupsLoad();});
</script>
"""

if "</body>" in base.HTML_PANEL:
    base.HTML_PANEL = base.HTML_PANEL.replace("</body>", EXTRA_UI + "</body>", 1)
else:
    log.warning("No se encontró </body> en el panel base: la interfaz v2.5 no se inyectó")


# ============================================================
# ARRANQUE
# ============================================================
if __name__ == "__main__":
    if "--tg-login" in sys.argv:
        base.tg_login_cli()
        sys.exit(0)
    base.init_db()
    init_v25_db()
    port = base.find_free_port(base.HOST, base.PORT)
    if port != base.PORT:
        print(f"  [!] Puerto {base.PORT} ocupado -> usando el puerto {port}")
    print("=" * 62)
    print(f"  GLOBAL EARLY WARNING OSINT — v{VERSION}  (sobre {BASE_MODULE}.py)")
    print(f"  http://{base.HOST}:{port}")
    print(f"  Fuentes: {len(base.SOURCES)} módulos · feeds RSS {len(base.RSS_FEEDS)} · guerra {len(base.WAR_FEEDS)} · "
          f"comunicados {len(base.STATEMENT_FEEDS)} · estatales {len(base.STATE_MEDIA_FEEDS)} · Telegram {len(base.TELEGRAM_CHANNELS)}")
    print("  Informe automático: " + (f"cada {REPORT_INTERVAL_MIN:g} min en ./{REPORT_DIR}" if REPORT_INTERVAL_MIN > 0 else "desactivado"))
    print("  Notificaciones externas: " + (", ".join(n for n, ok in (("webhook", NOTIFY_WEBHOOK_URL), ("ntfy", NTFY_URL), ("telegram", TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)) if ok) or "no configuradas"))
    print("=" * 62)
    threading.Thread(target=base._startup_diag, daemon=True).start()
    if REPORT_INTERVAL_MIN > 0:
        threading.Thread(target=_report_loop, daemon=True).start()
    for _attempt in range(10):
        try:
            app.run(host=base.HOST, port=port, debug=False, threaded=True)
            break
        except OSError as e:
            print(f"  [!] No se pudo usar el puerto {port} ({e}); probando otro...")
            port = base.find_free_port(base.HOST, port + 1)
            print(f"  http://{base.HOST}:{port}")
