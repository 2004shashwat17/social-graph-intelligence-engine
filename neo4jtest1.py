import csv
import json
import time
import random
import re
import unicodedata
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Neo4j driver ─────────────────────────────────────────────────────────────
try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("[!] neo4j-driver not installed. Run:  pip install neo4j")

# ═══════════════════════════════════════════════════════════════════
#  NEO4J CONFIG  ← edit these three lines
# ═════════════════════════════════════════════════════════════
NEO4J_URI      = " "   
NEO4J_USER     = ""
NEO4J_PASSWORD = " "   # test3 instance name
# ═══════════════════════════════════════════════════════════════════
#  HEADLESS TOGGLE
# ═══════════════════════════════════════════════════════════════════
HEADLESS = False

# ═══════════════════════════════════════════════════════════════════
#  UPWARD RECOVERY TRIGGER
# ═════════════════════════════════════════════════════════════
_EMPTY_BEFORE_RECOVERY = 1


# ─────────────────────────────────────────────
# NEO4J WRITER
# Wraps all graph operations behind a single
# clean interface.  All methods are idempotent
# via MERGE so re-runs never create duplicates.
# ─────────────────────────────────────────────

class Neo4jWriter:
    """
    Graph schema
    ────────────
    (:Person {fbid, name, url})
      -[:FRIEND_OF {level, scraped_at}]->
    (:Person {fbid, name, url})

    Indexes are created once on first use.
    """

    def __init__(self, uri: str, user: str, password: str):
        if not NEO4J_AVAILABLE:
            raise RuntimeError("neo4j Python driver not installed.")
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._setup_indexes()
        print(f"[Neo4j] Connected  →  {uri}")

    # ── Schema setup ─────────────────────────────────────────────

    def _setup_indexes(self):
        """Create uniqueness constraint + index (once, idempotent)."""
        with self._driver.session() as s:
            # Uniqueness constraint implicitly creates an index on fbid
            s.run(
                "CREATE CONSTRAINT person_fbid IF NOT EXISTS "
                "FOR (p:Person) REQUIRE p.fbid IS UNIQUE"
            )
            # Extra composite index for fast name lookups
            s.run(
                "CREATE INDEX person_name IF NOT EXISTS "
                "FOR (p:Person) ON (p.name)"
            )
        print("[Neo4j] Schema indexes verified.")

    # ── Node helpers ─────────────────────────────────────────────

    def merge_person(self, fbid: str, name: str, url: str):
        """
        MERGE a Person node by fbid.
        ON CREATE  - sets all three fields.
        ON MATCH   - updates name/url in case they changed.
        """
        with self._driver.session() as s:
            s.run(
                """
                MERGE (p:Person {fbid: $fbid})
                ON CREATE SET p.name = $name, p.url = $url,
                              p.first_seen = $ts
                ON MATCH  SET p.name = $name, p.url = $url,
                              p.last_seen  = $ts
                """,
                fbid=fbid, name=name, url=url,
                ts=datetime.utcnow().isoformat(),
            )

    # ── Edge helper ──────────────────────────────────────────────

    def merge_friend_of(
        self,
        src_fbid:  str,
        dst_fbid:  str,
        level:     int,
    ):
        """
        MERGE a FRIEND_OF relationship.
        Direction: src  ──FRIEND_OF──►  dst
        Multiple level-1 and level-2 edges are stored as a
        single relationship (MERGE); 'level' captures the
        shortest path at which this edge was discovered.
        """
        with self._driver.session() as s:
            s.run(
                """
                MATCH  (src:Person {fbid: $src_fbid})
                MATCH  (dst:Person {fbid: $dst_fbid})
                MERGE  (src)-[r:FRIEND_OF]->(dst)
                ON CREATE SET r.level       = $level,
                              r.scraped_at  = $ts
                ON MATCH  SET r.level       = CASE
                                WHEN $level < r.level THEN $level
                                ELSE r.level
                              END,
                              r.last_seen   = $ts
                """,
                src_fbid=src_fbid,
                dst_fbid=dst_fbid,
                level=level,
                ts=datetime.utcnow().isoformat(),
            )

    # ── Batch write (called after _build_rows) ───────────────────

    def write_rows(self, rows: list):
        """
        Accepts the same row dicts that _build_rows() produces and
        writes them to Neo4j.  One session per batch.
        """
        if not rows:
            return
        with self._driver.session() as s:
            for row in rows:
                # Upsert source node
                s.run(
                    """
                    MERGE (p:Person {fbid: $fbid})
                    ON CREATE SET p.name=$name, p.url=$url,
                                  p.first_seen=$ts
                    ON MATCH  SET p.name=$name, p.url=$url,
                                  p.last_seen=$ts
                    """,
                    fbid=row["source_fbid"],
                    name=row["source_name"],
                    url=row["source_url"],
                    ts=datetime.utcnow().isoformat(),
                )
                # Upsert friend node
                s.run(
                    """
                    MERGE (p:Person {fbid: $fbid})
                    ON CREATE SET p.name=$name, p.url=$url,
                                  p.first_seen=$ts
                    ON MATCH  SET p.name=$name, p.url=$url,
                                  p.last_seen=$ts
                    """,
                    fbid=row["friend_fbid"],
                    name=row["friend_name"],
                    url=row["friend_url"],
                    ts=datetime.utcnow().isoformat(),
                )
                # Upsert edge
                s.run(
                    """
                    MATCH (src:Person {fbid: $src})
                    MATCH (dst:Person {fbid: $dst})
                    MERGE (src)-[r:FRIEND_OF]->(dst)
                    ON CREATE SET r.level=$level,
                                  r.scraped_at=$ts
                    ON MATCH  SET r.level = CASE
                                    WHEN $level < r.level THEN $level
                                    ELSE r.level
                                  END,
                                  r.last_seen=$ts
                    """,
                    src=row["source_fbid"],
                    dst=row["friend_fbid"],
                    level=row["level"],
                    ts=datetime.utcnow().isoformat(),
                )
        print(f"   [Neo4j] ✓ Wrote {len(rows)} edges to graph")

    # ── Stats ────────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._driver.session() as s:
            nodes = s.run("MATCH (p:Person) RETURN count(p) AS n").single()["n"]
            edges = s.run("MATCH ()-[r:FRIEND_OF]->() RETURN count(r) AS n").single()["n"]
        return {"nodes": nodes, "edges": edges}

    # ── Useful queries you can call from the REPL ────────────────

    def shortest_path(self, fbid_a: str, fbid_b: str):
        """Return shortest friendship path between two FBIDs."""
        with self._driver.session() as s:
            result = s.run(
                """
                MATCH p=shortestPath(
                  (a:Person {fbid:$a})-[:FRIEND_OF*]-(b:Person {fbid:$b})
                )
                RETURN [n IN nodes(p) | n.name] AS path, length(p) AS hops
                """,
                a=fbid_a, b=fbid_b,
            ).single()
        return result

    def mutual_friends(self, fbid_a: str, fbid_b: str) -> list:
        """Return names of mutual friends between two FBIDs."""
        with self._driver.session() as s:
            rows = s.run(
                """
                MATCH (a:Person {fbid:$a})-[:FRIEND_OF]-(m)-[:FRIEND_OF]-(b:Person {fbid:$b})
                WHERE a <> b
                RETURN DISTINCT m.name AS name, m.fbid AS fbid
                ORDER BY m.name
                """,
                a=fbid_a, b=fbid_b,
            ).data()
        return rows

    def most_connected(self, top_n: int = 10) -> list:
        """Return top N most-connected people in the graph."""
        with self._driver.session() as s:
            rows = s.run(
                """
                MATCH (p:Person)-[:FRIEND_OF]-()
                RETURN p.name AS name, p.fbid AS fbid, count(*) AS degree
                ORDER BY degree DESC
                LIMIT $n
                """,
                n=top_n,
            ).data()
        return rows

    def close(self):
        self._driver.close()
        print("[Neo4j] Connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ─────────────────────────────────────────────
# GLOBAL NEO4J INSTANCE
# Initialised lazily in scrape_friends_network()
# ─────────────────────────────────────────────
_neo4j: Neo4jWriter | None = None


def _get_neo4j() -> Neo4jWriter | None:
    return _neo4j


# ═══════════════════════════════════════════════════════════════════
#  REST OF THE ORIGINAL CODE (unchanged except _flush_pending_rows
#  and _build_rows which now also call Neo4j)
# ═══════════════════════════════════════════════════════════════════

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

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2,
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


# ─────────────────────────────────────────────
# URL FILTERS
# ─────────────────────────────────────────────

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
    if _FB_NAV_PATH_RE.search(clean):      return False
    if _PROFILE_SUBPAGE_RE.search(clean):  return False
    if _NON_PROFILE_PATH_RE.search(clean): return False
    after_domain = re.split(r"facebook\.com/", clean, maxsplit=1)
    if len(after_domain) < 2 or not after_domain[1].strip("/"):
        return False
    return True


# ─────────────────────────────────────────────
# NAME VALIDATION
# ─────────────────────────────────────────────

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
    if lower in _SKIP_EXACT:
        return False
    if any(bad in lower for bad in _SKIP_SUBSTRINGS):
        return False
    if not any(unicodedata.category(ch).startswith("L") for ch in name):
        return False
    if len(name) < 2 or name.replace(" ", "").isdigit():
        return False
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


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

LOG_FILE = "log.txt"


def _log(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8-sig") as f:
        f.write(f"[{ts}] {message}\n")


# ─────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────

CHECKPOINT_FILE = "l2_done.json"


def load_checkpoint() -> set:
    if not Path(CHECKPOINT_FILE).exists():
        return set()
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8-sig") as f:
            return set(json.load(f).get("done_fbids", []))
    except Exception as e:
        print(f"[!] Could not read checkpoint: {e}")
        return set()


def save_checkpoint(done: set):
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8-sig") as f:
            json.dump({"done_fbids": sorted(done)}, f, indent=2)
    except Exception as e:
        print(f"[!] Could not save checkpoint: {e}")


def mark_l2_done(done: set, fbid: str):
    done.add(fbid)
    save_checkpoint(done)


# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

_CSV_FIELDS = [
    "source_name", "source_url", "source_fbid",
    "friend_name", "friend_url", "friend_fbid", "level",
]


def _append_rows_csv(rows: list, output_csv: str, write_header: bool):
    if not rows:
        return
    try:
        with open(output_csv, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
            if write_header:
                w.writeheader()
            w.writerows(rows)
    except Exception as e:
        print(f"[!] Failed to append CSV: {e}")


def load_existing_csv(output_csv: str):
    network, seen_edges = [], set()
    if not Path(output_csv).exists():
        return network, seen_edges
    try:
        with open(output_csv, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                network.append(row)
                seen_edges.add(
                    (row.get("source_fbid", "").strip(),
                     row.get("friend_fbid", "").strip())
                )
    except Exception as e:
        print(f"[!] Could not read existing CSV: {e}")
    return network, seen_edges


# ─────────────────────────────────────────────
# HUMAN-LIKE TIMING HELPERS
# ─────────────────────────────────────────────

def _human_pause(base: float = 0.4, jitter: float = 0.3):
    time.sleep(base + random.uniform(0, jitter))


def _occasional_long_pause(probability: float = 0.02):
    if random.random() < probability:
        time.sleep(random.uniform(0.5, 1.0))


def _jitter_scroll(driver, target_pos: int):
    overshoot = target_pos + random.randint(0, 50)
    driver.execute_script("window.scrollTo(0, arguments[0]);", overshoot)
    time.sleep(random.uniform(0.04, 0.09))
    driver.execute_script("window.scrollTo(0, arguments[0]);", target_pos)


# ─────────────────────────────────────────────
# WAIT FOR LAZY-LOAD NETWORK IDLE
# ─────────────────────────────────────────────

def _wait_for_network_idle(driver, timeout: float = 2.0, poll: float = 0.25):
    deadline = time.time() + timeout
    last_h   = driver.execute_script("return document.body.scrollHeight;")
    while time.time() < deadline:
        time.sleep(poll)
        new_h = driver.execute_script("return document.body.scrollHeight;")
        if new_h != last_h:
            last_h   = new_h
            deadline = time.time() + timeout
    return last_h


# ─────────────────────────────────────────────
# CARD SCANNER
# ─────────────────────────────────────────────

def _scan_all_cards(
    driver,
    my_fbid: str,
    seen_fbids: set,
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


# ─────────────────────────────────────────────
# ROW BUILDER  (unchanged logic, Neo4j write added)
# ─────────────────────────────────────────────

def _build_rows(batch, source_name, source_url, source_fbid, level, seen_edges):
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


# ─────────────────────────────────────────────
# CORE PAGE SCRAPER
# ─────────────────────────────────────────────

_MAX_STABLE_SCROLLS = 4
_MAX_SCROLLS        = 999999


def _scrape_single_friends_url(
    driver,
    friends_url:  str,
    my_fbid:      str,
    output_csv:   str,
    write_header: bool,
    source_name:  str,
    source_url:   str,
    source_fbid:  str,
    level:        int,
    seen_edges:   set,
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

    neo = _get_neo4j()  # may be None if Neo4j is disabled/unavailable

    def _flush_pending_rows():
        nonlocal rows_written
        if not pending_rows:
            return
        # ── CSV write (unchanged) ────────────────────────────────
        _append_rows_csv(
            pending_rows,
            output_csv,
            write_header=(write_header and rows_written == 0),
        )
        # ── Neo4j write (new) ────────────────────────────────────
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

    # ── Initial above-fold scan ──────────────────────────────────
    _wait_for_network_idle(driver, timeout=2.0, poll=0.25)
    batch = _scan_all_cards(driver, my_fbid, seen_fbids_this_page, processed_buttons)
    if batch:
        rows = _build_rows(batch, source_name, source_url, source_fbid,
                           level, seen_edges)
        if rows:
            pending_rows.extend(rows)
            if len(pending_rows) >= 100:
                _flush_pending_rows()
        all_found.extend(batch)

    consecutive_empty  = 0
    recovery_just_done = False

    # ── Scroll loop ──────────────────────────────────────────────
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
                if len(pending_rows) >= 100:
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
                        f"— resetting stable counter and continuing downward"
                    )
                    rows = _build_rows(
                        recovery_batch, source_name, source_url, source_fbid,
                        level, seen_edges,
                    )
                    if rows:
                        pending_rows.extend(rows)
                        if len(pending_rows) >= 100:
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
                        if len(pending_rows) >= 100:
                            _flush_pending_rows()
                    all_found.extend(final_batch)
                break
            else:
                last_height = extra_h

    _flush_pending_rows()

    print(
        f"   [✓] Page done: {len(all_found)} friends, "
        f"{rows_written} new CSV rows, {scroll_count} scrolls"
    )
    return all_found


# ─────────────────────────────────────────────
# FRIEND SCRAPER WITH FALLBACK URL
# ─────────────────────────────────────────────

def scrape_friends_page_optimized(
    driver,
    profile_url:  str,
    my_fbid:      str,
    output_csv:   str,
    write_header: bool,
    source_name:  str,
    source_url:   str,
    source_fbid:  str,
    level:        int,
    seen_edges:   set,
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
        driver, primary_url, my_fbid, output_csv, write_header,
        source_name, source_url, source_fbid, level, seen_edges,
    )

    if not result and use_mutual_fallback:
        print("   [fallback] Nothing on friends_all — trying friends_mutual …")
        result = _scrape_single_friends_url(
            driver, fallback_url, my_fbid, output_csv, write_header,
            source_name, source_url, source_fbid, level, seen_edges,
        )

    print(f"   [✓] Total friends collected: {len(result)}")
    return result


# ─────────────────────────────────────────────
# MAIN NETWORK SCRAPER
# ─────────────────────────────────────────────

def scrape_friends_network(
    driver,
    output_csv:          str  = "friends_network.csv",
    max_level:           int  = 2,
    enable_neo4j:        bool = True,
):
    global _neo4j

    # ── Neo4j boot-up ────────────────────────────────────────────
    if enable_neo4j and NEO4J_AVAILABLE:
        try:
            _neo4j = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        except Exception as e:
            print(f"[!] Neo4j connection failed: {e}")
            print("    Falling back to CSV-only mode.")
            _neo4j = None
    else:
        print("[Neo4j] Disabled or driver not installed — CSV-only mode.")

    driver.get("https://www.facebook.com/me")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    my_url  = driver.current_url
    my_fbid = extract_fbid_from_url(my_url)
    my_name = "me"

    print(f"\n[+] Profile  : {my_name}")
    print(f"[+] URL      : {my_url}")
    print(f"[+] FBID     : {my_fbid}")
    print(f"[+] Output   : {output_csv}")
    print(f"[+] Headless : {HEADLESS}")
    print(f"[+] Neo4j    : {'connected' if _neo4j else 'disabled'}")

    # Seed the "me" node in Neo4j
    if _neo4j:
        _neo4j.merge_person(my_fbid, my_name, my_url)

    network, seen_edges = load_existing_csv(output_csv)
    is_new_file         = len(network) == 0
    l2_done             = load_checkpoint()
    print(f"[+] L2 checkpoint: {len(l2_done)} profiles already done")

    # ── Level 1 ──────────────────────────────────────────────────
    print("\n─── LEVEL 1 ───")
    l1_start   = time.time()
    my_friends = scrape_friends_page_optimized(
        driver, my_url, my_fbid, output_csv, is_new_file,
        my_name, my_url, my_fbid, 1, seen_edges,
        use_mutual_fallback=False,
    )
    l1_elapsed = time.time() - l1_start
    _log(
        f"Level-1 | Friends saved: {len(my_friends)} | "
        f"Time: {l1_elapsed:.1f}s ({l1_elapsed/60:.1f} min)"
    )
    print(f"[✓] Level-1 complete — {len(my_friends)} friends")

    # ── Level 2 ──────────────────────────────────────────────────
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
                    driver, fr["url"], my_fbid, output_csv, False,
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

            mark_l2_done(l2_done, fr["fbid"])
            print(f"   [✓] L2 done for {fr['name']} — checkpoint updated")

            time.sleep(random.uniform(0.5, 1.0))

    # ── Final Neo4j stats ─────────────────────────────────────────
    if _neo4j:
        stats = _neo4j.stats()
        print(f"\n[Neo4j] Graph stats → {stats['nodes']} nodes, {stats['edges']} edges")
        _neo4j.close()

    print(f"\n[✅] ALL DONE")
    print(f"[📁] Output     : {output_csv}")
    print(f"[📋] Checkpoint : {CHECKPOINT_FILE}")
    return output_csv


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from fb_login_1 import main
    driver = main()
    if driver:
        scrape_friends_network(driver, max_level=2, enable_neo4j=True)
    else:
        print("[✗] Login failed.")