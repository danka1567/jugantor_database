#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           JUGANTOR DUAL-STREAM SCRAPER  —  Advanced Edition                ║
║                                                                              ║
║  • EVEN stream : pages 0, 2, 4, 6, …                                        ║
║  • ODD  stream : pages 1, 3, 5, 7, …                                        ║
║  • Shared SQLite DB — only unique URLs stored (UNIQUE constraint + IGNORE)  ║
║  • Auto-resume from last checkpoint on every run                            ║
║  • New URLs appended to same .db without duplicates across ANY run          ║
║  • Rich live dashboard in terminal + optional log file                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

Install:
    pip install requests aiohttp tqdm colorama

Run:
    python jugantor_scraper.py
"""

# ══════════════════════════════════════════════════════════════════════════════
#  ▼▼▼  CONFIG — edit everything between these markers, nowhere else  ▼▼▼
# ══════════════════════════════════════════════════════════════════════════════

CONFIG = {

    # ── Execution mode ────────────────────────────────────────────────────────
    # "normal"      → sequential requests, one at a time
    # "threadpool"  → concurrent via ThreadPoolExecutor (fastest for most cases)
    # "asyncio"     → fully async via aiohttp coroutines
    "MODE": "threadpool",

    # ── Page range ────────────────────────────────────────────────────────────
    # EVEN stream : MIN_PAGE, MIN_PAGE+2, … MAX_PAGE
    # ODD  stream : MIN_PAGE+1, MIN_PAGE+3, … MAX_PAGE
    "MIN_PAGE": 0,
    "MAX_PAGE": 1000,

    # ── Thread-pool worker count (only used when MODE = "threadpool") ─────────
    "WORKERS": 6,

    # ── SQLite database file (relative or absolute path) ──────────────────────
    "DB_FILE": "jugantor.db",

    # ── Politeness delay between batches (seconds, chosen at random) ──────────
    "DELAY_MIN": 0.3,
    "DELAY_MAX": 0.9,

    # ── Consecutive empty pages before a stream is declared finished ──────────
    "MAX_EMPTY_STREAK": 5,

    # ── Per-request retry policy ──────────────────────────────────────────────
    "MAX_RETRIES": 3,        # max retry attempts after first failure
    "RETRY_BACKOFF": 2.0,    # seconds * attempt_number between retries

    # ── HTTP request timeout (seconds) ────────────────────────────────────────
    "TIMEOUT": 15,

    # ── Reset on start? ───────────────────────────────────────────────────────
    # False → auto-resume from saved checkpoints (recommended)
    # True  → wipe checkpoints & restart from MIN_PAGE on every run
    "RESET_ON_START": False,

    # ── Log file (set "" to disable file logging) ─────────────────────────────
    "LOG_FILE": "jugantor_scraper.log",

    # ── Progress-bar colour ───────────────────────────────────────────────────
    # "cyan" | "green" | "magenta" | "yellow" | "white"
    "PBAR_COLOR": "cyan",
}

# ══════════════════════════════════════════════════════════════════════════════
#  ▲▲▲  END CONFIG  ▲▲▲
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import logging
import random
import signal
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False

try:
    from colorama import Fore, Style, init as _cinit
    _cinit(autoreset=True)
    C = {
        "ok":    Fore.GREEN,
        "warn":  Fore.YELLOW,
        "err":   Fore.RED,
        "info":  Fore.CYAN,
        "dim":   Style.DIM,
        "bold":  Style.BRIGHT,
        "reset": Style.RESET_ALL,
    }
except ImportError:
    C = {k: "" for k in ("ok","warn","err","info","dim","bold","reset")}

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.jugantor.com/ajax/load/latestnews/30/{page}/100"

_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile Safari/604.1",
]

_HEADERS_BASE: Dict[str, str] = {
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Referer":          "https://www.jugantor.com/",
    "X-Requested-With": "XMLHttpRequest",
    "Accept-Language":  "en-US,en;q=0.9,bn;q=0.8",
    "Accept-Encoding":  "gzip, deflate, br",
    "Connection":       "keep-alive",
}


def _hdrs() -> Dict[str, str]:
    return {**_HEADERS_BASE, "User-Agent": random.choice(_USER_AGENTS)}


# ══════════════════════════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

_stop = False


def _sig(sig, frame):
    global _stop
    _stop = True
    print(f"\n{C['warn']}⚠  Interrupt — finishing current batch then flushing …{C['reset']}")


signal.signal(signal.SIGINT,  _sig)
signal.signal(signal.SIGTERM, _sig)


# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def _mk_logger(log_file: str) -> logging.Logger:
    lg  = logging.getLogger("jugantor")
    lg.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)-7s] %(message)s", "%Y-%m-%d %H:%M:%S")
    ch  = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(fmt)
    lg.addHandler(ch)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        lg.addHandler(fh)
    return lg


LOG: logging.Logger = _mk_logger(CONFIG["LOG_FILE"])

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE  ─  WAL-mode SQLite, unique-URL guarantee, thread-safe
# ══════════════════════════════════════════════════════════════════════════════

_db_lock = Lock()


def open_db(path: str, reset: bool) -> sqlite3.Connection:
    """
    Open / create the database.

    Key design decisions:
      • UNIQUE constraint on `url`  →  INSERT OR IGNORE silently discards
        any URL already present, whether it arrived in this run or a
        previous one.  No duplicates can ever enter the table.
      • WAL journal mode  →  readers never block writers; safe for
        concurrent threads.
      • scrape_state table stores per-stream page checkpoints so every
        run resumes exactly where the previous one stopped.
      • scrape_runs table keeps a human-readable audit trail of every run.
    """
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous  = NORMAL")
    conn.execute("PRAGMA cache_size   = -16000")   # 16 MB page cache
    conn.execute("PRAGMA temp_store   = MEMORY")

    if reset:
        conn.executescript("""
            DROP TABLE IF EXISTS articles;
            DROP TABLE IF EXISTS scrape_state;
            DROP TABLE IF EXISTS scrape_runs;
        """)
        LOG.info("RESET: all tables dropped.")

    conn.executescript("""
        -- ── Main article table ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS articles (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id     INTEGER,
            title          TEXT,
            url            TEXT NOT NULL,
            published_date TEXT,
            stream         TEXT,
            page_number    INTEGER,
            scraped_at     TEXT,

            -- UNIQUE on URL: INSERT OR IGNORE drops duplicates silently
            -- regardless of which run, stream, or thread found the URL
            CONSTRAINT uq_url UNIQUE (url)
        );

        CREATE INDEX IF NOT EXISTS idx_url    ON articles(url);
        CREATE INDEX IF NOT EXISTS idx_date   ON articles(published_date DESC);
        CREATE INDEX IF NOT EXISTS idx_stream ON articles(stream);
        CREATE INDEX IF NOT EXISTS idx_page   ON articles(page_number);

        -- ── Resume checkpoints (one row per stream) ──────────────────────────
        CREATE TABLE IF NOT EXISTS scrape_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        -- ── Audit log (one row per execution) ────────────────────────────────
        CREATE TABLE IF NOT EXISTS scrape_runs (
            run_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            ended_at   TEXT,
            mode       TEXT,
            min_page   INTEGER,
            max_page   INTEGER,
            workers    INTEGER,
            new_saved  INTEGER DEFAULT 0,
            duplicates INTEGER DEFAULT 0,
            errors     INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    return conn


def cp_get(conn: sqlite3.Connection, stream: str) -> Optional[int]:
    row = conn.execute(
        "SELECT value FROM scrape_state WHERE key=?", (f"cp_{stream}",)
    ).fetchone()
    return int(row[0]) if row else None


def cp_set(conn: sqlite3.Connection, stream: str, page: int):
    with _db_lock:
        conn.execute(
            "INSERT OR REPLACE INTO scrape_state(key,value) VALUES(?,?)",
            (f"cp_{stream}", str(page))
        )
        conn.commit()


def db_insert(
    conn: sqlite3.Connection,
    items: List[dict],
    stream: str,
    page: int,
) -> Tuple[int, int]:
    """
    Persist a list of parsed article dicts.
    Returns (new_inserted, duplicates_skipped).

    The UNIQUE(url) constraint + INSERT OR IGNORE does all the dedup work:
      - Same URL from a different page?      → skipped
      - Same URL scraped in a previous run?  → skipped
      - Concurrent thread found the same URL? → skipped
    """
    inserted = skipped = 0
    now = datetime.utcnow().isoformat(timespec="seconds")
    with _db_lock:
        cur = conn.cursor()
        for a in items:
            url = (a.get("url") or "").strip()
            if not url:
                skipped += 1
                continue
            cur.execute(
                """INSERT OR IGNORE INTO articles
                       (article_id, title, url, published_date, stream, page_number, scraped_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    a.get("article_id"),
                    (a.get("title") or "").strip(),
                    url,
                    (a.get("published_date") or "").strip(),
                    stream,
                    page,
                    now,
                ),
            )
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1
        conn.commit()
    return inserted, skipped


def db_total(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]


def db_run_open(conn: sqlite3.Connection) -> int:
    cfg = CONFIG
    with _db_lock:
        cur = conn.execute(
            "INSERT INTO scrape_runs(started_at,mode,min_page,max_page,workers) VALUES(?,?,?,?,?)",
            (datetime.utcnow().isoformat(timespec="seconds"),
             cfg["MODE"], cfg["MIN_PAGE"], cfg["MAX_PAGE"], cfg["WORKERS"]),
        )
        conn.commit()
        return cur.lastrowid


def db_run_close(conn: sqlite3.Connection, run_id: int,
                 new: int, dup: int, errs: int):
    with _db_lock:
        conn.execute(
            "UPDATE scrape_runs SET ended_at=?,new_saved=?,duplicates=?,errors=? WHERE run_id=?",
            (datetime.utcnow().isoformat(timespec="seconds"), new, dup, errs, run_id),
        )
        conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  RESPONSE PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _unwrap(raw) -> List[dict]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for k in ("data", "news", "articles", "items", "results", "posts"):
            if k in raw and isinstance(raw[k], list):
                return raw[k]
        for v in raw.values():
            if isinstance(v, list) and v:
                return v
    return []


def _parse(raw_list: List[dict]) -> List[dict]:
    out = []
    for item in raw_list:
        pub = (
            item.get("publishDateTime")
            or item.get("publishTime")
            or item.get("publish_date")
            or item.get("created_at")
            or ""
        )
        url = (
            item.get("url")
            or item.get("link")
            or item.get("permalink")
            or ""
        )
        out.append({
            "article_id":    item.get("id") or item.get("article_id"),
            "title":         item.get("headline") or item.get("fullheadline") or item.get("title") or "",
            "url":           url,
            "published_date": pub,
        })
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  STREAM START PAGE  (respects checkpoint + parity)
# ══════════════════════════════════════════════════════════════════════════════

def stream_start(stream: str, conn: sqlite3.Connection) -> int:
    cfg = CONFIG
    if not cfg["RESET_ON_START"]:
        cp = cp_get(conn, stream)
        if cp is not None:
            LOG.info(f"[{stream}] resuming from checkpoint page {cp}")
            return cp
    # Fresh start: respect MIN_PAGE + stream parity
    base = cfg["MIN_PAGE"]
    if stream == "even":
        return base if base % 2 == 0 else base + 1
    else:
        return (base + 1) if base % 2 == 0 else base


# ══════════════════════════════════════════════════════════════════════════════
#  LIVE STATS  (thread-safe shared counters)
# ══════════════════════════════════════════════════════════════════════════════

class Stats:
    def __init__(self):
        self._lock = Lock()
        self.inserted = 0
        self.skipped  = 0
        self.errors   = 0
        self.pages    = {"even": 0, "odd": 0}

    def record(self, stream: str, page: int, ins: int, skip: int):
        with self._lock:
            self.inserted += ins
            self.skipped  += skip
            self.pages[stream] = page

    def err(self):
        with self._lock:
            self.errors += 1

    def postfix(self) -> dict:
        with self._lock:
            return {
                "E-pg": self.pages["even"],
                "O-pg": self.pages["odd"],
                "new":  self.inserted,
                "dup":  self.skipped,
            }


# ══════════════════════════════════════════════════════════════════════════════
#  HTTP HELPERS  (sync + async with retry)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_sync(sess: "requests.Session", page: int) -> Optional[List[dict]]:
    url = BASE_URL.format(page=page)
    cfg = CONFIG
    for attempt in range(cfg["MAX_RETRIES"] + 1):
        try:
            r = sess.get(url, headers=_hdrs(), timeout=cfg["TIMEOUT"])
            r.raise_for_status()
            return _unwrap(r.json())
        except Exception as exc:
            LOG.warning(f"page {page} attempt {attempt+1}: {exc}")
            if attempt < cfg["MAX_RETRIES"]:
                time.sleep(cfg["RETRY_BACKOFF"] * (attempt + 1))
    return None


async def fetch_async(
    sess: "aiohttp.ClientSession",
    page: int,
) -> Optional[List[dict]]:
    url = BASE_URL.format(page=page)
    cfg = CONFIG
    for attempt in range(cfg["MAX_RETRIES"] + 1):
        try:
            async with sess.get(
                url,
                headers=_hdrs(),
                timeout=aiohttp.ClientTimeout(total=cfg["TIMEOUT"]),
            ) as resp:
                resp.raise_for_status()
                return _unwrap(await resp.json(content_type=None))
        except Exception as exc:
            LOG.warning(f"[async] page {page} attempt {attempt+1}: {exc}")
            if attempt < cfg["MAX_RETRIES"]:
                await asyncio.sleep(cfg["RETRY_BACKOFF"] * (attempt + 1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  MODE 1 — NORMAL
# ══════════════════════════════════════════════════════════════════════════════

def run_normal(conn: sqlite3.Connection, stats: Stats, pbar: tqdm):
    cfg   = CONFIG
    sess  = requests.Session()
    cur   = {s: stream_start(s, conn) for s in ("even", "odd")}
    empty = {"even": 0, "odd": 0}
    done  = {"even": False, "odd": False}

    try:
        while not _stop and not all(done.values()):
            for stream in ("even", "odd"):
                if done[stream] or _stop:
                    continue
                page = cur[stream]

                if page > cfg["MAX_PAGE"]:
                    done[stream] = True
                    pbar.write(f"{C['ok']}  ✔ [{stream}] reached MAX_PAGE {cfg['MAX_PAGE']}{C['reset']}")
                    continue

                data = fetch_sync(sess, page)

                if data is None:
                    stats.err(); empty[stream] += 1
                elif len(data) == 0:
                    empty[stream] += 1
                else:
                    empty[stream] = 0
                    ins, skip = db_insert(conn, _parse(data), stream, page)
                    stats.record(stream, page, ins, skip)
                    LOG.debug(f"[{stream}] pg {page}: +{ins} new  {skip} dup")
                    if ins:
                        pbar.write(
                            f"{C['ok']}  ✓ [{stream}] pg {page:>5}  "
                            f"+{ins} new  {C['dim']}{skip} dup{C['reset']}")

                if empty[stream] >= cfg["MAX_EMPTY_STREAK"]:
                    done[stream] = True
                    pbar.write(f"{C['warn']}  ✔ [{stream}] empty-streak at pg {page}{C['reset']}")

                cur[stream] += 2
                cp_set(conn, stream, cur[stream])
                pbar.update(1)
                pbar.set_postfix(stats.postfix())
                time.sleep(random.uniform(cfg["DELAY_MIN"], cfg["DELAY_MAX"]))
    finally:
        sess.close()


# ══════════════════════════════════════════════════════════════════════════════
#  MODE 2 — THREADPOOL
# ══════════════════════════════════════════════════════════════════════════════

def _tp_worker(stream: str, page: int) -> Tuple[str, int, Optional[List[dict]]]:
    cfg = CONFIG
    url = BASE_URL.format(page=page)
    for attempt in range(cfg["MAX_RETRIES"] + 1):
        try:
            r = requests.get(url, headers=_hdrs(), timeout=cfg["TIMEOUT"])
            r.raise_for_status()
            return stream, page, _unwrap(r.json())
        except Exception as exc:
            LOG.warning(f"[tp][{stream}] pg {page} attempt {attempt+1}: {exc}")
            if attempt < cfg["MAX_RETRIES"]:
                time.sleep(cfg["RETRY_BACKOFF"] * (attempt + 1))
    return stream, page, None


def run_threadpool(conn: sqlite3.Connection, stats: Stats, pbar: tqdm):
    cfg   = CONFIG
    cur   = {s: stream_start(s, conn) for s in ("even", "odd")}
    empty = {"even": 0, "odd": 0}
    done  = {"even": False, "odd": False}
    per   = max(1, cfg["WORKERS"] // 2)   # pages per stream per batch

    with ThreadPoolExecutor(max_workers=cfg["WORKERS"]) as pool:
        while not _stop and not all(done.values()):

            # ── Build batch ───────────────────────────────────────────────────
            batch: List[Tuple[str, int]] = []
            for stream in ("even", "odd"):
                if done[stream]:
                    continue
                for _ in range(per):
                    page = cur[stream]
                    if page > cfg["MAX_PAGE"]:
                        done[stream] = True
                        pbar.write(f"{C['ok']}  ✔ [{stream}] reached MAX_PAGE {cfg['MAX_PAGE']}{C['reset']}")
                        break
                    batch.append((stream, page))
                    cur[stream] += 2

            if not batch:
                break

            futures = {pool.submit(_tp_worker, s, p): (s, p) for s, p in batch}

            for fut in as_completed(futures):
                stream, page, data = fut.result()

                if data is None:
                    stats.err(); empty[stream] += 1
                elif len(data) == 0:
                    empty[stream] += 1
                else:
                    empty[stream] = 0
                    ins, skip = db_insert(conn, _parse(data), stream, page)
                    stats.record(stream, page, ins, skip)
                    LOG.debug(f"[{stream}] pg {page}: +{ins} new  {skip} dup")
                    if ins:
                        pbar.write(
                            f"{C['ok']}  ✓ [{stream}] pg {page:>5}  "
                            f"+{ins} new  {C['dim']}{skip} dup{C['reset']}")

                if empty[stream] >= cfg["MAX_EMPTY_STREAK"]:
                    done[stream] = True
                    pbar.write(f"{C['warn']}  ✔ [{stream}] empty-streak at pg {page}{C['reset']}")

                cp_set(conn, stream, cur[stream])
                pbar.update(1)
                pbar.set_postfix(stats.postfix())

            time.sleep(random.uniform(cfg["DELAY_MIN"] / 2, cfg["DELAY_MAX"] / 2))


# ══════════════════════════════════════════════════════════════════════════════
#  MODE 3 — ASYNCIO
# ══════════════════════════════════════════════════════════════════════════════

async def _async_stream(
    stream: str,
    conn: sqlite3.Connection,
    stats: Stats,
    pbar: tqdm,
):
    cfg   = CONFIG
    page  = stream_start(stream, conn)
    empty = 0

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    ) as sess:
        while not _stop:
            if page > cfg["MAX_PAGE"]:
                pbar.write(f"{C['ok']}  ✔ [{stream}] reached MAX_PAGE {cfg['MAX_PAGE']}{C['reset']}")
                break

            data = await fetch_async(sess, page)

            if data is None:
                stats.err(); empty += 1
            elif len(data) == 0:
                empty += 1
            else:
                empty = 0
                ins, skip = db_insert(conn, _parse(data), stream, page)
                stats.record(stream, page, ins, skip)
                LOG.debug(f"[{stream}] pg {page}: +{ins} new  {skip} dup")
                if ins:
                    pbar.write(
                        f"{C['ok']}  ✓ [{stream}] pg {page:>5}  "
                        f"+{ins} new  {C['dim']}{skip} dup{C['reset']}")

            if empty >= cfg["MAX_EMPTY_STREAK"]:
                pbar.write(f"{C['warn']}  ✔ [{stream}] empty-streak at pg {page}{C['reset']}")
                break

            page += 2
            cp_set(conn, stream, page)
            pbar.update(1)
            pbar.set_postfix(stats.postfix())
            await asyncio.sleep(random.uniform(cfg["DELAY_MIN"], cfg["DELAY_MAX"]))


async def _async_main(conn: sqlite3.Connection, stats: Stats, pbar: tqdm):
    await asyncio.gather(
        _async_stream("even", conn, stats, pbar),
        _async_stream("odd",  conn, stats, pbar),
    )


def run_asyncio(conn: sqlite3.Connection, stats: Stats, pbar: tqdm):
    asyncio.run(_async_main(conn, stats, pbar))


# ══════════════════════════════════════════════════════════════════════════════
#  CHECKS + BANNER + REPORT
# ══════════════════════════════════════════════════════════════════════════════

def _check():
    mode = CONFIG["MODE"]
    errs = []
    if mode not in ("normal","threadpool","asyncio"):
        errs.append(f"MODE={mode!r} is invalid. Use normal|threadpool|asyncio")
    if CONFIG["MIN_PAGE"] < 0:
        errs.append("MIN_PAGE must be >= 0")
    if CONFIG["MAX_PAGE"] <= CONFIG["MIN_PAGE"]:
        errs.append("MAX_PAGE must be > MIN_PAGE")
    if CONFIG["WORKERS"] < 1:
        errs.append("WORKERS must be >= 1")
    if mode in ("normal","threadpool") and not REQUESTS_OK:
        errs.append("'requests' not installed — pip install requests")
    if mode == "asyncio" and not AIOHTTP_OK:
        errs.append("'aiohttp' not installed — pip install aiohttp")
    if errs:
        for e in errs:
            print(f"{C['err']}  CONFIG / DEP ERROR: {e}{C['reset']}")
        sys.exit(1)


def _banner(conn: sqlite3.Connection, run_id: int):
    cfg   = CONFIG
    total = db_total(conn)
    even_cp = cp_get(conn, "even")
    odd_cp  = cp_get(conn, "odd")
    resume_msg = (
        f"EVEN → pg {even_cp}  |  ODD → pg {odd_cp}"
        if (even_cp is not None or odd_cp is not None) and not cfg["RESET_ON_START"]
        else "Starting fresh from MIN_PAGE"
    )
    w_info = f"  (workers={cfg['WORKERS']})" if cfg["MODE"] == "threadpool" else ""
    print("\n".join([
        "",
        f"{C['info']}{'═'*62}{C['reset']}",
        f"{C['bold']}{C['info']}  ░  JUGANTOR DUAL-STREAM SCRAPER  —  Run #{run_id}  ░{C['reset']}",
        f"{C['info']}{'═'*62}{C['reset']}",
        f"  Mode       : {C['ok']}{cfg['MODE'].upper()}{C['reset']}{w_info}",
        f"  Page range : {cfg['MIN_PAGE']}  →  {cfg['MAX_PAGE']}",
        f"  Streams    : EVEN (0,2,4,…)  +  ODD (1,3,5,…)",
        f"  Database   : {cfg['DB_FILE']}  [{total:,} articles already stored]",
        f"  Dedup      : {C['ok']}UNIQUE(url) + INSERT OR IGNORE{C['reset']}  — zero duplicates guaranteed",
        f"  Resume     : {resume_msg}",
        f"  Delay      : {cfg['DELAY_MIN']}–{cfg['DELAY_MAX']} s   timeout={cfg['TIMEOUT']} s   retries={cfg['MAX_RETRIES']}",
        f"  Log file   : {cfg['LOG_FILE'] or '(disabled)'}",
        f"  Stop       : Ctrl+C  → checkpoints auto-saved",
        f"{C['info']}{'═'*62}{C['reset']}",
        "",
    ]))


def _report(conn: sqlite3.Connection, stats: Stats, elapsed: float):
    cfg   = CONFIG
    total = db_total(conn)
    rate  = stats.inserted / elapsed if elapsed > 0 else 0
    even_cp = cp_get(conn, "even") or "?"
    odd_cp  = cp_get(conn, "odd")  or "?"
    print("\n".join([
        "",
        f"{C['info']}{'─'*54}{C['reset']}",
        f"{C['ok']}{C['bold']}  ✅  RUN COMPLETE{C['reset']}",
        f"{'─'*54}",
        f"  New articles saved this run  : {C['ok']}{stats.inserted:,}{C['reset']}",
        f"  Duplicate URLs skipped       : {stats.skipped:,}",
        f"  HTTP errors                  : "
          f"{C['warn'] if stats.errors else ''}{stats.errors}{C['reset']}",
        f"  Total articles in DB (ever)  : {C['info']}{total:,}{C['reset']}",
        f"  EVEN stream last page        : {even_cp}",
        f"  ODD  stream last page        : {odd_cp}",
        f"  Elapsed                      : {elapsed:.1f} s  ({rate:.1f} new/s)",
        f"  Database                     : {cfg['DB_FILE']}",
        f"{'─'*54}",
        "  Next run will auto-resume from the checkpoints above.",
        "  Only brand-new URLs will be inserted; everything else is",
        "  silently skipped — zero duplicates guaranteed.",
        f"{'─'*54}",
        "",
    ]))


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    _check()
    cfg  = CONFIG
    mode = cfg["MODE"]

    # Estimated total page-fetches (even + odd combined) for the progress bar
    total_pages = cfg["MAX_PAGE"] - cfg["MIN_PAGE"] + 1

    conn   = open_db(cfg["DB_FILE"], reset=cfg["RESET_ON_START"])
    run_id = db_run_open(conn)
    stats  = Stats()

    _banner(conn, run_id)

    pbar = tqdm(
        total=total_pages,
        desc=f"[{mode}]",
        unit=" pg",
        dynamic_ncols=True,
        colour=cfg["PBAR_COLOR"],
        bar_format=(
            "{l_bar}{bar}| {n_fmt}/{total_fmt} pg "
            "[{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        ),
    )

    t0 = time.perf_counter()
    try:
        if mode == "normal":
            run_normal(conn, stats, pbar)
        elif mode == "threadpool":
            run_threadpool(conn, stats, pbar)
        elif mode == "asyncio":
            run_asyncio(conn, stats, pbar)
    finally:
        pbar.close()
        elapsed = time.perf_counter() - t0
        db_run_close(conn, run_id, stats.inserted, stats.skipped, stats.errors)
        _report(conn, stats, elapsed)
        conn.close()


if __name__ == "__main__":
    main()
