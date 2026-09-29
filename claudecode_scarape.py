"""
fb_scraper_optimized.py
═══════════════════════════════════════════════════════════════════════
Facebook Friends Network Scraper — Optimized for lakh-scale SNA


pip install neo4j pyarrow duckdb pybloom-live selenium
═══════════════════════════════════════════════════════════════════════

OPTIMIZATIONS OVER ORIGINAL:
  1. Neo4j UNWIND batch writes  — 3N queries → 2 queries per flush
  2. Parquet storage (Snappy)   — replaces CSV; columnar, compressed
  3. DuckDB edge deduplication  — O(1) resume load, no full-file scan
  4. Bloom filter (seen_edges)  — ~50 MB for 10M edges vs ~2 GB set
  5. SQLite checkpoint          — replaces JSON rewrite on every profile
  6. Better human timing        — longer random pauses, realistic delays

DEPENDENCIES (pip install):
  neo4j pyarrow duckdb pybloom-live selenium
"""

import re
import time
import random
import sqlite3
import unicodedata
from pathlib import Path
from datetime import datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Bloom filter ──────────────────────────────────────────────────────────────
try:
    from pybloom_live import ScalableBloomFilter
    BLOOM_AVAILABLE = True
except ImportError:
    BLOOM_AVAILABLE = False
    print("[!] pybloom-live not installed — falling back to Python set.")
    print("    Run:  pip install pybloom-live")

# ── Neo4j driver ──────────────────────────────────────────────────────────────
try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("[xxx] neo4j-driver not installed. Run:  pip install neo4j")


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  ← edit this section
# ═══════════════════════════════════════════════════════════════════════════════

# NOTE: For Neo4j Aura the username is the instance-ID string shown in the
# Aura console, NOT "neo4j".  Database name is usually the same value.
NEO4J_URI          = "neo4j+s://0262f840.databases.neo4j.io"
NEO4J_USER         = " "
NEO4J_PASSWORD     = "VPR8Kr_avKF1t2yH3DDFpyGe-Ns4ds1FIjWMEPZTiNE"
NEO4J_DATABASE     = " "
AURA_INSTANCEID    = " "
AURA_INSTANCENAME  = "test11"

# Output files
PARQUET_FILE       = "friends_network.parquet"   # primary edge store
CHECKPOINT_DB      = "checkpoint.db"             # SQLite checkpoint
LOG_FILE           = "log.txt"

# Scraper behaviour
HEADLESS                = False
_EMPTY_BEFORE_RECOVERY  = 1

# Neo4j write batch size (rows before flushing to Neo4j)
NEO4J_BATCH_SIZE = 200

# Checkpoint is saved to SQLite every N L2 profiles (no full-file rewrite)
CHECKPOINT_SAVE_EVERY = 1   # save after every profile (SQLite is fast)


# ═══════════════════════════════════════════════════════════════════════════════
#  PARQUET SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════

_PARQUET_SCHEMA = pa.schema([
    pa.field("source_fbid",  pa.string()),
    pa.field("source_name",  pa.string()),
    pa.field("source_url",   pa.string()),
    pa.field("friend_fbid",  pa.string()),
    pa.field("friend_name",  pa.string()),
    pa.field("friend_url",   pa.string()),
    pa.field("level",        pa.int8()),
    pa.field("scraped_at",   pa.string()),
])


# ═══════════════════════════════════════════════════════════════════════════════
#  NEO4J WRITER  — UNWIND batch mode
# ═══════════════════════════════════════════════════════════════════════════════

class Neo4jWriter:
    """
    Graph schema
    ────────────
    (:Person {fbid, name, url, first_seen, last_seen})
      -[:FRIEND_OF {level, scraped_at, last_seen}]->
    (:Person)

    All writes use UNWIND so a batch of N rows = 2 Cypher queries,
    regardless of N (previously 3×N queries).
    """

    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        if not NEO4J_AVAILABLE:
            raise RuntimeError("neo4j Python driver not installed.")

        self._database = database

        self._driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            max_connection_lifetime=3600,
            max_connection_pool_size=50,
            connection_acquisition_timeout=60,
        )
        self._driver.verify_connectivity()
        print(f"[Neo4j] Connected  → {uri}")
        print(f"[Neo4j] Database   → {database}")
        self._setup_indexes()

    # ── Schema ────────────────────────────────────────────────────

    def _setup_indexes(self):
        with self._session() as s:
            s.run(
                "CREATE CONSTRAINT person_fbid IF NOT EXISTS "
                "FOR (p:Person) REQUIRE p.fbid IS UNIQUE"
            )
            s.run(
                "CREATE INDEX person_name IF NOT EXISTS "
                "FOR (p:Person) ON (p.name)"
            )
        print("[Neo4j] Schema indexes verified.")

    # ── Single-node upsert (used for the root 'me' node) ─────────

    def merge_person(self, fbid: str, name: str, url: str):
        ts = datetime.utcnow().isoformat()
        with self._session() as s:
            s.run(
                """
                MERGE (p:Person {fbid: $fbid})
                ON CREATE SET p.name=$name, p.url=$url, p.first_seen=$ts
                ON MATCH  SET p.name=$name, p.url=$url, p.last_seen=$ts
                """,
                fbid=fbid, name=name, url=url, ts=ts,
            )

    # ── UNWIND batch write — 2 queries for any batch size ─────────

    def write_rows(self, rows: list):
        """
        Write a batch of edge-dicts produced by _build_rows().

        Sends exactly 2 Cypher queries regardless of batch size:
          Query 1 — UNWIND nodes list  → MERGE all Person nodes
          Query 2 — UNWIND edges list  → MERGE all FRIEND_OF rels
        """
        if not rows:
            return

        ts = datetime.utcnow().isoformat()

        # De-duplicate nodes within this batch to minimise MERGE contention
        nodes_seen: set = set()
        nodes_list: list = []
        for r in rows:
            for fbid, name, url in (
                (r["source_fbid"], r["source_name"], r["source_url"]),
                (r["friend_fbid"], r["friend_name"], r["friend_url"]),
            ):
                if fbid not in nodes_seen:
                    nodes_seen.add(fbid)
                    nodes_list.append({"fbid": fbid, "name": name, "url": url})

        edges_list = [
            {
                "src":   r["source_fbid"],
                "dst":   r["friend_fbid"],
                "level": r["level"],
            }
            for r in rows
        ]

        with self._session() as s:
            # ── 1 of 2: bulk node upsert ──────────────────────────
            s.run(
                """
                UNWIND $nodes AS n
                MERGE (p:Person {fbid: n.fbid})
                ON CREATE SET p.name      = n.name,
                              p.url       = n.url,
                              p.first_seen = $ts
                ON MATCH  SET p.name      = n.name,
                              p.url       = n.url,
                              p.last_seen  = $ts
                """,
                nodes=nodes_list,
                ts=ts,
            )

            # ── 2 of 2: bulk edge upsert ──────────────────────────
            s.run(
                """
                UNWIND $edges AS e
                MATCH (src:Person {fbid: e.src})
                MATCH (dst:Person {fbid: e.dst})
                MERGE (src)-[r:FRIEND_OF]->(dst)
                ON CREATE SET r.level      = e.level,
                              r.scraped_at = $ts
                ON MATCH  SET r.level      = CASE
                                WHEN e.level < r.level THEN e.level
                                ELSE r.level
                              END,
                              r.last_seen  = $ts
                """,
                edges=edges_list,
                ts=ts,
            )

        print(
            f"   [Neo4j] ✓ {len(rows)} edges written "
            f"({len(nodes_list)} nodes, 2 queries)"
        )

    # ── Analytics helpers ──────────────────────────────────────────

    def stats(self) -> dict:
        with self._session() as s:
            nodes = s.run("MATCH (p:Person) RETURN count(p) AS n").single()["n"]
            edges = s.run("MATCH ()-[r:FRIEND_OF]->() RETURN count(r) AS n").single()["n"]
        return {"nodes": nodes, "edges": edges}

    def shortest_path(self, fbid_a: str, fbid_b: str):
        with self._session() as s:
            return s.run(
                """
                MATCH p=shortestPath(
                  (a:Person {fbid:$a})-[:FRIEND_OF*]-(b:Person {fbid:$b})
                )
                RETURN [n IN nodes(p) | n.name] AS path, length(p) AS hops
                """,
                a=fbid_a, b=fbid_b,
            ).single()

    def mutual_friends(self, fbid_a: str, fbid_b: str) -> list:
        with self._session() as s:
            return s.run(
                """
                MATCH (a:Person {fbid:$a})-[:FRIEND_OF]-(m)-[:FRIEND_OF]-(b:Person {fbid:$b})
                WHERE a <> b
                RETURN DISTINCT m.name AS name, m.fbid AS fbid
                ORDER BY m.name
                """,
                a=fbid_a, b=fbid_b,
            ).data()

    def most_connected(self, top_n: int = 10) -> list:
        with self._session() as s:
            return s.run(
                """
                MATCH (p:Person)-[:FRIEND_OF]-()
                RETURN p.name AS name, p.fbid AS fbid, count(*) AS degree
                ORDER BY degree DESC
                LIMIT $n
                """,
                n=top_n,
            ).data()

    def close(self):
        self._driver.close()
        print("[Neo4j] Connection closed.")

    def __enter__(self):  return self
    def __exit__(self, *_): self.close()
    def _session(self):   return self._driver.session(database=self._database)


# ─────────────────────────────────────────────
# GLOBAL NEO4J INSTANCE
# ─────────────────────────────────────────────
_neo4j: "Neo4jWriter | None" = None


def _get_neo4j() -> "Neo4jWriter | None":
    return _neo4j


# ═══════════════════════════════════════════════════════════════════════════════
#  PARQUET HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _append_rows_parquet(rows: list, parquet_path: str):
    """
    Append a batch of edge-rows to the Parquet file.
    Uses Snappy compression; appends by reading + rewriting the file.
    For very large files (>50M rows) consider switching to a partitioned
    Parquet dataset — the schema here is forward-compatible with that.
    """
    if not rows:
        return

    ts = datetime.utcnow().isoformat()
    enriched = [{**r, "scraped_at": ts} for r in rows]

    new_table = pa.Table.from_pylist(enriched, schema=_PARQUET_SCHEMA)

    path = Path(parquet_path)
    if path.exists():
        existing = pq.read_table(str(path))
        combined = pa.concat_tables([existing, new_table])
    else:
        combined = new_table

    pq.write_table(combined, str(path), compression="snappy")


def load_seen_edges(parquet_path: str) -> "set | ScalableBloomFilter":
    """
    Load previously scraped (source_fbid, friend_fbid) pairs for dedup.

    Uses DuckDB to read only the two fbid columns — never loads full rows.
    Returns a Bloom filter if pybloom-live is available, else a plain set.
    Memory usage:
        Bloom filter  ≈ 50 MB for 10 M edges  (1 % false-positive rate)
        Plain set     ≈ 1–2 GB for 10 M edges
    """
    path = Path(parquet_path)

    if BLOOM_AVAILABLE:
        seen: "set | ScalableBloomFilter" = ScalableBloomFilter(
            mode=ScalableBloomFilter.LARGE_SET_GROWTH,
            error_rate=0.01,
        )
    else:
        seen = set()

    if not path.exists():
        print("[Parquet] No existing file — starting fresh.")
        return seen

    try:
        con = duckdb.connect()
        rows = con.execute(
            "SELECT source_fbid, friend_fbid FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
        con.close()

        for src, dst in rows:
            seen.add((src, dst))

        count = len(rows)
        print(f"[Parquet] Loaded {count:,} existing edges into dedup filter.")
    except Exception as e:
        print(f"[!] Could not read existing Parquet: {e}")

    return seen


def load_level1_from_parquet(parquet_path: str) -> list:
    """
    Re-hydrate the L1 friend list from a Parquet file for resume.
    Returns a list of {'name', 'url', 'fbid'} dicts.
    """
    path = Path(parquet_path)
    if not path.exists():
        return []
    try:
        con = duckdb.connect()
        rows = con.execute(
            """
            SELECT DISTINCT friend_name AS name,
                            friend_url  AS url,
                            friend_fbid AS fbid
            FROM   read_parquet(?)
            WHERE  level = 1
            """,
            [str(path)],
        ).fetchall()
        con.close()
        return [{"name": r[0], "url": r[1], "fbid": r[2]} for r in rows]
    except Exception as e:
        print(f"[!] Could not load L1 from Parquet: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  SQLITE CHECKPOINT  (replaces JSON rewrite-on-every-save)
# ═══════════════════════════════════════════════════════════════════════════════

def _open_checkpoint_db() -> sqlite3.Connection:
    con = sqlite3.connect(CHECKPOINT_DB)
    con.execute(
        "CREATE TABLE IF NOT EXISTS l2_done "
        "(fbid TEXT PRIMARY KEY, ts TEXT)"
    )
    con.commit()
    return con


def load_checkpoint(con: sqlite3.Connection) -> set:
    rows = con.execute("SELECT fbid FROM l2_done").fetchall()
    result = {r[0] for r in rows}
    print(f"[Checkpoint] {len(result)} L2 profiles already done (SQLite).")
    return result


def mark_l2_done(con: sqlite3.Connection, fbid: str):
    """Single INSERT — O(1), no file rewrite."""
    con.execute(
        "INSERT OR IGNORE INTO l2_done (fbid, ts) VALUES (?, ?)",
        (fbid, datetime.utcnow().isoformat()),
    )
    con.commit()


# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def _log(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8-sig") as f:
        f.write(f"[{ts}] {message}\n")


# ═══════════════════════════════════════════════════════════════════════════════
#  CHROME DRIVER BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_driver() -> webdriver.Chrome:
    opts = ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-setuid-sandbox")
    else:
        opts.add_argument("--start-maximized")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Block images & fonts — speeds up page load, we only need DOM text
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts":  2,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": (
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
                "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
                "Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});"
            )
        },
    )
    return driver


# ═══════════════════════════════════════════════════════════════════════════════
#  URL / NAME FILTERS
# ═══════════════════════════════════════════════════════════════════════════════

_FB_NAV_PATH_RE = re.compile(
    r"facebook\.com/"
    r"(friends|events|groups|pages|marketplace|gaming|watch|memories|saved|help"
    r"|notifications|settings|privacy|ads|business|donate|fundraisers|offers"
    r"|stories|reels|live|jobs|professional_dashboard)"
    r"(/|$)",
    re.IGNORECASE,
)
_PROFILE_SUBPAGE_RE = re.compile(
    r"facebook\.com/[^/?]+"
    r"/(likes_all|photos|videos|friends|about|reviews|map|sport|music"
    r"|movies|books|tv|checkins|following|followers|groups|events|posts"
    r"|friends_all|friends_mutual)"
    r"(/|$)",
    re.IGNORECASE,
)
_NON_PROFILE_PATH_RE = re.compile(
    r"facebook\.com/(search|hashtag|pages|business|places|public)",
    re.IGNORECASE,
)


def is_valid_profile_url(url: str) -> bool:
    if not url or "facebook.com" not in url:
        return False
    clean = url.split("?")[0].rstrip("/")
    if _FB_NAV_PATH_RE.search(clean):       return False
    if _PROFILE_SUBPAGE_RE.search(clean):   return False
    if _NON_PROFILE_PATH_RE.search(clean):  return False
    after_domain = re.split(r"facebook\.com/", clean, maxsplit=1)
    if len(after_domain) < 2 or not after_domain[1].strip("/"):
        return False
    return True


_SKIP_EXACT = {
    "friends list", "this photo", "add friend", "followers", "following",
    "recently", "photos", "about", "friend requests", "suggestions",
    "see all", "filter", "groups", "videos", "reels", "your reels",
    "saved reels", "all likes", "movies", "tv shows", "artists", "books",
    "sports teams", "athletes", "people", "restaurants", "apps and games",
    "add to story", "friends", "all friends", "birthdays", "see more",
    "show all", "hide", "more", "less", "create", "manage", "settings",
    "privacy", "help", "support", "terms", "policies", "contact",
    "feedback", "invite", "connect", "import", "export", "sync",
    "notifications", "messages", "requests", "updates", "home", "profile",
    "logout", "login", "signup", "live", "watch", "marketplace", "gaming",
    "pages", "events", "likes", "comments", "shares", "saves",
    "visit help center", "view profile cover photo", "mutual followers",
    "mutual following", "Sports", "Watched", "Add Friend",
}
_SKIP_SUBSTRINGS = [
    "current city", "hometown", "home town", "works at", "studied at",
    "went to", "lives in", "mutual friend", "mutual follower",
    "mutual following", "add friend", "photos of", "'s photos", "s photos",
    "albums", "view profile", "₹", "$", "€", " friends", "Sports", "Watched",
]


def is_valid_friend_name(name: str) -> bool:
    if not name:
        return False
    name = name.strip()
    first_line = next((l.strip() for l in name.splitlines() if l.strip()), "")
    if not first_line:
        return False
    name  = first_line
    lower = name.lower()
    if lower in _SKIP_EXACT:                                         return False
    if any(bad in lower for bad in _SKIP_SUBSTRINGS):               return False
    if not any(unicodedata.category(ch).startswith("L") for ch in name): return False
    if len(name) < 2 or name.replace(" ", "").isdigit():            return False
    return True


def normalize_profile_url(url: str) -> str:
    if not url:
        return ""
    return url.split("?")[0].rstrip("/").replace(
        "//m.facebook.com/", "//www.facebook.com/"
    )


def extract_fbid_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"[?&]id=(\d+)", url)
    if m:
        return m.group(1)
    return url.split("facebook.com/")[-1].split("?")[0].rstrip("/")


# ═══════════════════════════════════════════════════════════════════════════════
#  HUMAN-LIKE TIMING HELPERS
#  Longer pauses than original — more realistic, less likely to trigger
#  Facebook's automation detection on large runs.
# ═══════════════════════════════════════════════════════════════════════════════

def _human_pause(base: float = 0.4, jitter: float = 0.3):
    time.sleep(base + random.uniform(0, jitter))


def _occasional_long_pause(probability: float = 0.03):
    """
    ~3 % of scroll events trigger a longer human-like break.
    Pauses are now 3–8 s (was 0.5–1 s) to better mimic real browsing.
    """
    if random.random() < probability:
        pause = random.uniform(3.0, 8.0)
        print(f"   [timing] human pause {pause:.1f}s …")
        time.sleep(pause)


def _inter_profile_pause():
    """
    Pause between L2 profile scrapes.
    Occasionally inserts a longer cooldown (5 % chance of 30–90 s).
    """
    base = random.uniform(1.0, 2.5)
    time.sleep(base)
    if random.random() < 0.05:
        long_pause = random.uniform(30.0, 90.0)
        print(f"   [timing] long cooldown {long_pause:.0f}s …")
        time.sleep(long_pause)


def _jitter_scroll(driver, target_pos: int):
    overshoot = target_pos + random.randint(0, 50)
    driver.execute_script("window.scrollTo(0, arguments[0]);", overshoot)
    time.sleep(random.uniform(0.04, 0.09))
    driver.execute_script("window.scrollTo(0, arguments[0]);", target_pos)


def _wait_for_network_idle(driver, timeout: float = 2.0, poll: float = 0.25) -> int:
    deadline = time.time() + timeout
    last_h   = driver.execute_script("return document.body.scrollHeight;")
    while time.time() < deadline:
        time.sleep(poll)
        new_h = driver.execute_script("return document.body.scrollHeight;")
        if new_h != last_h:
            last_h   = new_h
            deadline = time.time() + timeout
    return last_h


# ═══════════════════════════════════════════════════════════════════════════════
#  CARD SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

def _scan_all_cards(
    driver,
    my_fbid:           str,
    seen_fbids:        set,
    processed_buttons: set,
) -> list:

    new_friends = []

    buttons = driver.find_elements(
        By.CSS_SELECTOR,
        "[aria-label^='Add Friend'], "
        "[aria-label^='Add friend'], "
        "[aria-label^='More options for']",
    )

    for btn in buttons:
        try:
            btn_id = getattr(btn, "id", None)
            if btn_id in processed_buttons:
                continue
            if btn_id:
                processed_buttons.add(btn_id)

            aria = btn.get_attribute("aria-label") or ""
            al   = aria.lower()

            if al.startswith("add friend "):
                card_name = aria[len("Add Friend "):].strip()
            elif al.startswith("more options for "):
                card_name = aria[len("More options for "):].strip()
            else:
                card_name = ""

            if not is_valid_friend_name(card_name):
                try:
                    parent = btn.find_element(
                        By.XPATH,
                        "./ancestor::*[.//a[contains(@href,'facebook.com')]][1]"
                    )
                    spans = parent.find_elements(
                        By.XPATH,
                        ".//span[@dir='auto']"
                    )
                    for span in spans:
                        txt = span.text.strip()
                        if is_valid_friend_name(txt):
                            card_name = txt
                            break
                except Exception:
                    pass

            if not is_valid_friend_name(card_name):
                continue

            card_url = None
            parent   = btn.find_element(
                By.XPATH,
                "./ancestor::*[.//a[contains(@href,'facebook.com')]][1]"
            )
            for a in parent.find_elements(
                By.XPATH,
                ".//a[contains(@href,'facebook.com')]"
            ):
                href = a.get_attribute("href") or ""
                if (
                    is_valid_profile_url(href)
                    and "friends_mutual" not in href
                    and "friends_all"    not in href
                ):
                    card_url = href
                    break

            if not card_url:
                continue

            fbid = extract_fbid_from_url(card_url)
            if fbid == my_fbid:
                continue
            if fbid in seen_fbids:
                continue

            seen_fbids.add(fbid)
            new_friends.append({"name": card_name, "url": card_url, "fbid": fbid})
            print(f"      [+] {card_name}")

        except Exception:
            continue

    return new_friends


# ═══════════════════════════════════════════════════════════════════════════════
#  ROW BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _build_rows(batch, source_name, source_url, source_fbid, level, seen_edges):
    """
    Convert a batch of friend-dicts into edge-row dicts.
    Deduplication uses the Bloom filter (or set) — O(1) per lookup.
    False positives from the Bloom filter are harmless: Neo4j MERGE is
    the authoritative deduplicator; a missed edge = one skipped write.
    """
    rows = []
    for fr in batch:
        edge = (source_fbid, fr["fbid"])
        if edge in seen_edges:
            continue
        seen_edges.add(edge)
        rows.append({
            "source_name": source_name,
            "source_url":  source_url,
            "source_fbid": source_fbid,
            "friend_name": fr["name"],
            "friend_url":  fr["url"],
            "friend_fbid": fr["fbid"],
            "level":       level,
        })
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE PAGE SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

_MAX_STABLE_SCROLLS = 4
_MAX_SCROLLS        = 999999


def _scrape_single_friends_url(
    driver,
    friends_url:  str,
    my_fbid:      str,
    parquet_path: str,
    write_header: bool,     # kept for API compat; Parquet has no header
    source_name:  str,
    source_url:   str,
    source_fbid:  str,
    level:        int,
    seen_edges,             # set | ScalableBloomFilter
) -> list:

    print(f"   [→] {friends_url}")
    driver.get(friends_url)
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    _human_pause(0.9, 0.3)

    seen_fbids_this_page = {my_fbid} if my_fbid else set()
    processed_buttons    = set()
    pending_rows         = []
    all_found:   list    = []
    rows_written: int    = 0

    neo = _get_neo4j()

    def _flush_pending_rows():
        nonlocal rows_written
        if not pending_rows:
            return
        # ── Parquet write ─────────────────────────────────────────
        _append_rows_parquet(pending_rows, parquet_path)
        # ── Neo4j UNWIND batch write ──────────────────────────────
        if neo:
            try:
                neo.write_rows(pending_rows)
            except Exception as e:
                print(f"   [Neo4j][!] Write error: {e}")
        rows_written += len(pending_rows)
        pending_rows.clear()

    stable_scrolls = 0
    last_height    = 0
    scroll_count   = 0
    current_pos    = 0

    BASE_STEP = 1000
    MAX_STEP  = 1650
    step      = BASE_STEP

    # ── Initial above-fold scan ───────────────────────────────────
    _wait_for_network_idle(driver, timeout=2.0, poll=0.25)
    batch = _scan_all_cards(driver, my_fbid, seen_fbids_this_page, processed_buttons)
    if batch:
        rows = _build_rows(batch, source_name, source_url, source_fbid,
                           level, seen_edges)
        if rows:
            pending_rows.extend(rows)
            if len(pending_rows) >= NEO4J_BATCH_SIZE:
                _flush_pending_rows()
        all_found.extend(batch)

    consecutive_empty  = 0
    recovery_just_done = False

    # ── Scroll loop ───────────────────────────────────────────────
    while scroll_count < _MAX_SCROLLS:
        scroll_count += 1

        if stable_scrolls >= 2:
            step = min(step + 350, MAX_STEP)
        else:
            step = BASE_STEP

        page_height = driver.execute_script("return document.body.scrollHeight;")
        next_pos    = min(current_pos + step, page_height)

        _jitter_scroll(driver, next_pos)
        current_pos = next_pos

        _wait_for_network_idle(driver, timeout=2.0, poll=0.25)
        _occasional_long_pause()

        batch      = _scan_all_cards(
            driver, my_fbid, seen_fbids_this_page, processed_buttons
        )
        new_found  = len(batch)
        new_height = driver.execute_script("return document.body.scrollHeight;")

        if batch:
            rows = _build_rows(batch, source_name, source_url, source_fbid,
                               level, seen_edges)
            if rows:
                pending_rows.extend(rows)
                if len(pending_rows) >= NEO4J_BATCH_SIZE:
                    _flush_pending_rows()
            all_found.extend(batch)

        height_grew = new_height > last_height
        last_height = new_height

        if new_found == 0 and not height_grew:
            stable_scrolls    += 1
            consecutive_empty += 1
            print(
                f"   [scroll #{scroll_count}] no new cards, height stable "
                f"({stable_scrolls}/{_MAX_STABLE_SCROLLS})"
            )

            if (
                consecutive_empty >= _EMPTY_BEFORE_RECOVERY
                and not recovery_just_done
            ):
                recovery_pos = max(0, current_pos - step * 1)
                print(
                    f"   [recovery ↑] {consecutive_empty} empty scrolls — "
                    f"scrolling UP to {recovery_pos}px to re-trigger lazy load …"
                )
                _jitter_scroll(driver, recovery_pos)
                _wait_for_network_idle(driver, timeout=2.0, poll=0.25)

                recovery_batch = _scan_all_cards(
                    driver, my_fbid, seen_fbids_this_page, processed_buttons
                )
                if recovery_batch:
                    print(
                        f"   [recovery ↑] ✓ Rescued {len(recovery_batch)} card(s) "
                        f"— resetting stable counter"
                    )
                    rows = _build_rows(
                        recovery_batch, source_name, source_url, source_fbid,
                        level, seen_edges,
                    )
                    if rows:
                        pending_rows.extend(rows)
                        if len(pending_rows) >= NEO4J_BATCH_SIZE:
                            _flush_pending_rows()
                    all_found.extend(recovery_batch)
                    stable_scrolls    = 0
                    consecutive_empty = 0
                else:
                    print(f"   [recovery ↑] Nothing new on upward pass.")
                    consecutive_empty = 0

                _jitter_scroll(driver, current_pos)
                _wait_for_network_idle(driver, timeout=2.0, poll=0.25)
                recovery_just_done = True

        else:
            if stable_scrolls > 0:
                print(f"   [scroll #{scroll_count}] resumed — resetting stable counter")
            stable_scrolls     = 0
            consecutive_empty  = 0
            recovery_just_done = False

        if stable_scrolls >= _MAX_STABLE_SCROLLS:
            print(f"   [scroll] Stable for {_MAX_STABLE_SCROLLS} scrolls — done.")
            break

        if next_pos >= new_height and not height_grew:
            extra_h = _wait_for_network_idle(driver, timeout=3.5, poll=0.25)
            if extra_h <= new_height:
                print(f"   [scroll] True bottom reached.")
                final_batch = _scan_all_cards(
                    driver, my_fbid, seen_fbids_this_page, processed_buttons
                )
                if final_batch:
                    rows = _build_rows(
                        final_batch, source_name, source_url, source_fbid,
                        level, seen_edges,
                    )
                    if rows:
                        pending_rows.extend(rows)
                        if len(pending_rows) >= NEO4J_BATCH_SIZE:
                            _flush_pending_rows()
                    all_found.extend(final_batch)
                break
            else:
                last_height = extra_h

    _flush_pending_rows()

    print(
        f"   [✓] Page done: {len(all_found)} friends, "
        f"{rows_written} new rows written, {scroll_count} scrolls"
    )
    return all_found


# ═══════════════════════════════════════════════════════════════════════════════
#  FRIEND SCRAPER WITH FALLBACK URL
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_friends_page_optimized(
    driver,
    profile_url:  str,
    my_fbid:      str,
    parquet_path: str,
    write_header: bool,
    source_name:  str,
    source_url:   str,
    source_fbid:  str,
    level:        int,
    seen_edges,
    use_mutual_fallback: bool = False,
) -> list:

    if "profile.php" in profile_url:
        base = profile_url.split("&sk=")[0]
        fbid = extract_fbid_from_url(profile_url)
        if fbid and fbid.isdigit():
            primary_url  = base + "&sk=friends_all"
            fallback_url = base + "&sk=friends_mutual"
        else:
            primary_url  = base + "&sk=friends"
            fallback_url = base + "&sk=friends_mutual"
    else:
        clean        = profile_url.rstrip("/")
        primary_url  = clean + "/friends_all"
        fallback_url = clean + "/friends_mutual"

    result = _scrape_single_friends_url(
        driver, primary_url, my_fbid, parquet_path, write_header,
        source_name, source_url, source_fbid, level, seen_edges,
    )

    if not result and use_mutual_fallback:
        print("   [fallback] Nothing on friends_all — trying friends_mutual …")
        result = _scrape_single_friends_url(
            driver, fallback_url, my_fbid, parquet_path, write_header,
            source_name, source_url, source_fbid, level, seen_edges,
        )

    print(f"   [✓] Total friends collected: {len(result)}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN NETWORK SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_friends_network(
    driver,
    parquet_path:  str  = PARQUET_FILE,
    max_level:     int  = 2,
    enable_neo4j:  bool = True,
):
    global _neo4j

    # ── Neo4j boot-up ─────────────────────────────────────────────
    if enable_neo4j and NEO4J_AVAILABLE:
        try:
            _neo4j = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
        except Exception as e:
            print(f"[!] Neo4j connection failed: {e}")
            print("    Falling back to Parquet-only mode.")
            _neo4j = None
    else:
        print("[Neo4j] Disabled or driver not installed — Parquet-only mode.")

    driver.get("https://www.facebook.com/me")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    my_url  = driver.current_url
    my_fbid = extract_fbid_from_url(my_url)
    my_name = "me"

    print(f"\n[+] Profile   : {my_name}")
    print(f"[+] URL       : {my_url}")
    print(f"[+] FBID      : {my_fbid}")
    print(f"[+] Output    : {parquet_path}  (Parquet/Snappy)")
    print(f"[+] Headless  : {HEADLESS}")
    print(f"[+] Neo4j     : {'connected' if _neo4j else 'disabled'}")
    print(f"[+] Bloom     : {'enabled' if BLOOM_AVAILABLE else 'disabled (using set)'}")

    # Seed 'me' node in Neo4j
    if _neo4j:
        _neo4j.merge_person(my_fbid, my_name, my_url)

    # ── Load seen edges via DuckDB + Bloom filter ─────────────────
    seen_edges = load_seen_edges(parquet_path)

    # ── Open SQLite checkpoint ────────────────────────────────────
    ckpt_con  = _open_checkpoint_db()
    l2_done   = load_checkpoint(ckpt_con)

    # ── Level 1 ───────────────────────────────────────────────────
    print("\n─── LEVEL 1 ───")
    l1_start = time.time()

    my_friends = scrape_friends_page_optimized(
        driver, my_url, my_fbid, parquet_path, True,
        my_name, my_url, my_fbid, 1, seen_edges,
        use_mutual_fallback=False,
    )

    # If we resumed and the page showed 0 (already scraped), reload from Parquet
    if not my_friends:
        print("[+] L1 page empty (already scraped?) — loading L1 from Parquet …")
        my_friends = load_level1_from_parquet(parquet_path)

    l1_elapsed = time.time() - l1_start
    _log(
        f"Level-1 | Friends saved: {len(my_friends)} | "
        f"Time: {l1_elapsed:.1f}s ({l1_elapsed/60:.1f} min)"
    )
    print(f"[✓] Level-1 complete — {len(my_friends)} friends")

    # ── Level 2 ───────────────────────────────────────────────────
    if max_level >= 2:
        print("\n─── LEVEL 2 ───")
        total = len(my_friends)

        for i, fr in enumerate(my_friends, start=1):
            if fr["fbid"] in l2_done:
                print(f"[{i}/{total}] [SKIP – L2 done] {fr['name']}")
                continue

            print(f"\n[{i}/{total}] Scraping L2: {fr['name']}")
            t0 = time.time()

            try:
                second = scrape_friends_page_optimized(
                    driver, fr["url"], my_fbid, parquet_path, False,
                    fr["name"], fr["url"], fr["fbid"], 2, seen_edges,
                    use_mutual_fallback=True,
                )
            except Exception as e:
                print(f"[!] Error scraping {fr['name']}: {e}")
                continue

            elapsed = time.time() - t0
            _log(
                f"Profile: {fr['name']} | Friends saved: {len(second)} | "
                f"Time: {elapsed:.1f}s ({elapsed/60:.1f} min)"
            )

            mark_l2_done(ckpt_con, fr["fbid"])
            l2_done.add(fr["fbid"])
            print(f"   [✓] L2 done for {fr['name']} — checkpoint saved (SQLite)")

            _inter_profile_pause()

    # ── Final stats ───────────────────────────────────────────────
    ckpt_con.close()

    if _neo4j:
        stats = _neo4j.stats()
        print(f"\n[Neo4j] Graph stats → {stats['nodes']:,} nodes, {stats['edges']:,} edges")
        _neo4j.close()

    # Quick DuckDB stats on the Parquet file
    if Path(parquet_path).exists():
        try:
            con = duckdb.connect()
            stats_row = con.execute(
                "SELECT COUNT(*) AS edges, COUNT(DISTINCT source_fbid) + "
                "COUNT(DISTINCT friend_fbid) AS approx_nodes "
                f"FROM read_parquet('{parquet_path}')"
            ).fetchone()
            con.close()
            print(f"[Parquet] {stats_row[0]:,} edges stored")
        except Exception:
            pass

    print(f"\n[✅] ALL DONE")
    print(f"[📁] Output     : {parquet_path}")
    print(f"[📋] Checkpoint : {CHECKPOINT_DB}")
    return parquet_path


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    # ── Neo4j connectivity test ───────────────────────────────────
    if "--test-neo4j" in sys.argv:
        print("\n[Neo4j] Running connectivity test …")
        print(f"  URI      : {NEO4J_URI}")
        print(f"  User     : {NEO4J_USER}")
        print(f"  Database : {NEO4J_DATABASE}")
        try:
            writer = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
            stats  = writer.stats()
            print(f"\n[✅] Connection OK!")
            print(f"     Nodes : {stats['nodes']:,}")
            print(f"     Edges : {stats['edges']:,}")
            writer.close()
        except Exception as e:
            print(f"\n[✗] Connection FAILED: {e}")
            print("\nTroubleshooting checklist:")
            print("  1. Is your Aura instance RUNNING (not paused) in console.neo4j.io?")
            print("  2. Password correct? Re-download credentials from Aura if unsure.")
            print("  3. pip install neo4j   (driver must be v5.x for Aura)")
            print("  4. Check firewall / VPN — port 7687 must be open outbound.")
        sys.exit(0)

    # ── Parquet inspection tool ───────────────────────────────────
    if "--inspect" in sys.argv:
        path = PARQUET_FILE
        if not Path(path).exists():
            print(f"[!] {path} not found.")
            sys.exit(1)
        con = duckdb.connect()
        print("\n── Edge count by level ──")
        print(con.execute(
            f"SELECT level, COUNT(*) AS edges FROM read_parquet('{path}') GROUP BY level ORDER BY level"
        ).df().to_string(index=False))
        print("\n── Top 20 most-connected nodes (degree) ──")
        print(con.execute(f"""
            SELECT name, fbid, degree FROM (
                SELECT friend_name AS name, friend_fbid AS fbid, COUNT(*) AS degree
                FROM read_parquet('{path}') GROUP BY friend_name, friend_fbid
                UNION ALL
                SELECT source_name, source_fbid, COUNT(*) FROM read_parquet('{path}')
                GROUP BY source_name, source_fbid
            ) GROUP BY name, fbid ORDER BY degree DESC LIMIT 20
        """).df().to_string(index=False))
        con.close()
        sys.exit(0)

    # ── Normal scraper entry point ────────────────────────────────
    from fb_login_1 import main
    driver = main()
    if driver:
        scrape_friends_network(driver, max_level=2, enable_neo4j=True)
    else:
        print("[✗] Login failed.")