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

if __name__ == "__main__":
    if "--tg-login" in sys.argv:
        tg_login_cli()
        sys.exit(0)
    init_db()
    keyed = {
        "NASA FIRMS": NASA_FIRMS_KEY != "TU_MAP_KEY_AQUI",
        "GDELT Cloud": GDELT_API_KEY != "TU_GDELT_KEY_AQUI",
        "World Monitor": WORLD_MONITOR_KEY != "TU_WM_KEY_AQUI",
        "alerts.in.ua": bool(ALERTS_UA_TOKEN),
    }
    port = find_free_port(HOST, PORT)
    if port != PORT:
        print(f"  [!] Puerto {PORT} ocupado -> usando automáticamente el puerto {port}")
    print("=" * 60)
    print(f"  GLOBAL EARLY WARNING OSINT — v{APP_VERSION}")
    print(f"  http://{HOST}:{port}")
    print("  Fuentes públicas (sin API): Tzeva Adom, Oref, alerts.com.ua,")
    print("    Ubilling UA, Kyiv Digital, Neptun, sismos (USGS + EMSC + GEOFON), NASA EONET,")
    print("    GDELT GEO, RSS noticias, prensa/OSINT de guerra, comunicados oficiales, medios estatales, Telegram")
    print("  Traductor: " + ", ".join(n for n, _ in translation_providers()))
    print("  Con API: " + ", ".join(f"{k} [{'ON' if v else 'off'}]" for k, v in keyed.items()))
    print("  Telethon: " + ("listo" if (TELETHON_OK and TG_API_ID and TG_API_HASH) else "no configurado (búsqueda por web)"))
    print("=" * 60)
    threading.Thread(target=_startup_diag, daemon=True).start()
    # Si otro programa toma el puerto entre la comprobación y el arranque, reintenta con el siguiente
    for attempt in range(10):
        try:
            app.run(host=HOST, port=port, debug=False, threaded=True)
            break
        except OSError as e:
            print(f"  [!] No se pudo usar el puerto {port} ({e}); probando otro...")
            port = find_free_port(HOST, port + 1)
            print(f"  http://{HOST}:{port}")
