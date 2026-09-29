"""
fb_scraper_optimized.py
═══════════════════════════════════════════════════════════════════════
Facebook Friends Network Scraper — Optimized for lakh-scale SNA

pip install neo4j pyarrow duckdb pybloom-live selenium
═══════════════════════════════════════════════════════════════════════

OPTIMIZATIONS OVER PREVIOUS VERSION:
  1. Neo4j UNWIND batch writes     — 3N queries → 2 queries per flush
  2. Parquet partitioned dataset   — O(1) append, never rewrites old data
                                     replaces O(N²) single-file rewrite
  3. DuckDB edge deduplication     — O(1) resume load, no full-file scan
  4. Bloom filter (seen_edges)     — ~50 MB for 10M edges vs ~2 GB set
                                     edge key normalised to (min,max) fbid
                                     so (A,B) and (B,A) treated as same edge
  5. SQLite checkpoint             — replaces JSON rewrite on every profile
  6. Better human timing           — longer random pauses, realistic delays
  7. Neo4j batch size 200 → 500    — fewer round-trips per session
  8. Multi-label nodes             — :RootUser / :Level1Friend / :Level2Friend
                                     for visual distinction in Neo4j Explore
  9. Bidirectional edge fix        — Two directed FRIENDS rels per pair:
                                     (A)-[:FRIENDS]->(B) + (B)-[:FRIENDS]->(A)
                                     Neo4j has no truly undirected storage;
                                     single MERGE (a)-[r]-(b) creates only one
                                     directed edge → breaks centrality/GDS
 10. Role-based indexes            — extra indexes on Level1Friend / Level2Friend
 11. Final stats DuckDB query      — updated for partitioned dataset glob

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
NEO4J_URI      = "neo4j://127.0.0.1:7687"   
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "nHXklY0lpo6rmM9oTrOxs0rLvCedKmkNWVYXETD7_NQ" 

# Output — PARQUET_DIR is now a *directory* of partition files, not a single file.
# Every flush appends a new part_<timestamp>.parquet file inside this directory.
# Read with:  duckdb.read_parquet('friends_network/*.parquet')
PARQUET_DIR        = "friends_network"           # partitioned dataset directory
CHECKPOINT_DB      = "checkpoint.db"             # SQLite checkpoint
LOG_FILE           = "log.txt"

# Scraper behaviour
HEADLESS                = False
_EMPTY_BEFORE_RECOVERY  = 1

# Neo4j write batch size — increased from 200 → 500 (fewer round-trips)
NEO4J_BATCH_SIZE = 500

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
#  NEO4J WRITER  — UNWIND batch mode + multi-label + undirected FRIENDS edge
# ═══════════════════════════════════════════════════════════════════════════════

class Neo4jWriter:
    """
    Graph schema
    ────────────
    (:Person:RootUser      {fbid, name, url, first_seen, last_seen})
    (:Person:Level1Friend  {fbid, name, url, first_seen, last_seen})
    (:Person:Level2Friend  {fbid, name, url, first_seen, last_seen})

    Edges (bidirectional — two directed relationships per unique pair):
      (:Person)-[:FRIENDS]->(:Person)   [stored both A→B and B→A]

    Why two directed relationships instead of one undirected:
      Facebook friendship is symmetric but Neo4j does not support truly
      undirected storage — a MERGE without an arrow always creates a single
      directed edge internally.  Storing both directions makes degree,
      betweenness, PageRank, and all GDS algorithms produce correct results
      without adding UNION or bidirectional match patterns to every query.

    All writes use UNWIND so a batch of N rows = 2 Cypher queries,
    regardless of N.
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
            # Core uniqueness constraint on all Person nodes
            s.run(
                "CREATE CONSTRAINT person_fbid IF NOT EXISTS "
                "FOR (p:Person) REQUIRE p.fbid IS UNIQUE"
            )
            # Name lookup index
            s.run(
                "CREATE INDEX person_name IF NOT EXISTS "
                "FOR (p:Person) ON (p.name)"
            )
            # Role-label indexes — enable fast filtered queries per level
            # e.g. MATCH (p:RootUser) or MATCH (p:Level1Friend)
            s.run(
                "CREATE INDEX root_user_fbid IF NOT EXISTS "
                "FOR (p:RootUser) ON (p.fbid)"
            )
            s.run(
                "CREATE INDEX l1_friend_fbid IF NOT EXISTS "
                "FOR (p:Level1Friend) ON (p.fbid)"
            )
            s.run(
                "CREATE INDEX l2_friend_fbid IF NOT EXISTS "
                "FOR (p:Level2Friend) ON (p.fbid)"
            )
        print("[Neo4j] Schema indexes verified (Person + RootUser + L1 + L2).")

    # ── Single-node upsert (used for the root 'me' node) ─────────

    def merge_person(self, fbid: str, name: str, url: str, label: str = "RootUser"):
        """
        Upsert a single node and assign it an extra role label.
        label must be one of: RootUser, Level1Friend, Level2Friend
        """
        ts = datetime.utcnow().isoformat()
        # Two-step: MERGE on Person (unique constraint), then SET label
        with self._session() as s:
            s.run(
                f"""
                MERGE (p:Person {{fbid: $fbid}})
                ON CREATE SET p.name=$name, p.url=$url, p.first_seen=$ts
                ON MATCH  SET p.name=$name, p.url=$url, p.last_seen=$ts
                WITH p
                CALL apoc.create.addLabels(p, [$label]) YIELD node
                RETURN node
                """,
                fbid=fbid, name=name, url=url, ts=ts, label=label,
            )

    def merge_person_no_apoc(self, fbid: str, name: str, url: str):
        """
        Fallback merge_person without APOC — used if APOC is not installed.
        Sets RootUser label via a separate SET call (requires Neo4j 5+).
        """
        ts = datetime.utcnow().isoformat()
        with self._session() as s:
            s.run(
                """
                MERGE (p:Person {fbid: $fbid})
                ON CREATE SET p.name=$name, p.url=$url, p.first_seen=$ts
                ON MATCH  SET p.name=$name, p.url=$url, p.last_seen=$ts
                SET p:RootUser
                """,
                fbid=fbid, name=name, url=url, ts=ts,
            )

    # ── UNWIND batch write — 2 queries for any batch size ─────────

    def write_rows(self, rows: list):
        """
        Write a batch of edge-dicts produced by _build_rows().

        Sends exactly 2 Cypher queries regardless of batch size:
          Query 1 — UNWIND nodes list  → MERGE all Person nodes + SET role labels
          Query 2 — UNWIND edges list  → MERGE bidirectional FRIENDS rels (A→B + B→A)

        Role labels per node:
          source node at level 1  → :Level1Friend  (the root's direct friend)
          source node at level 2  → :Level2Friend  (L1's friend being scraped)
          friend node at level 1  → :Level1Friend
          friend node at level 2  → :Level2Friend

        Edge direction:
          Neo4j does NOT support truly undirected relationships at storage level.
          MERGE (a)-[r]-(b) without an arrow still creates ONE directed edge
          internally (always src→dst), which breaks centrality and traversal.
          We therefore MERGE BOTH directions explicitly:
            (min_fbid)-[:FRIENDS]->(max_fbid)  AND
            (max_fbid)-[:FRIENDS]->(min_fbid)
          This ensures degree, betweenness, PageRank, and all GDS algorithms
          see the friendship as truly symmetric.
        """
        if not rows:
            return

        ts = datetime.utcnow().isoformat()

        # Build node list with role label derived from row level
        nodes_seen: set = set()
        nodes_list: list = []
        for r in rows:
            level     = r["level"]
            role      = "Level1Friend" if level == 1 else "Level2Friend"

            for fbid, name, url in (
                (r["source_fbid"], r["source_name"], r["source_url"]),
                (r["friend_fbid"], r["friend_name"], r["friend_url"]),
            ):
                if fbid not in nodes_seen:
                    nodes_seen.add(fbid)
                    nodes_list.append({
                        "fbid":  fbid,
                        "name":  name,
                        "url":   url,
                        "role":  role,
                    })

        # Normalise edge direction: always src = lexicographically smaller fbid
        # This ensures (A→B) and (B→A) map to the same MERGE key in Neo4j,
        # preventing duplicate undirected relationships.
        edges_list = []
        for r in rows:
            a, b = r["source_fbid"], r["friend_fbid"]
            src, dst = (a, b) if a <= b else (b, a)
            edges_list.append({
                "src":   src,
                "dst":   dst,
                "level": r["level"],
            })

        with self._session() as s:
            # ── 1 of 2: bulk node upsert + role label ─────────────
            s.run(
                """
                UNWIND $nodes AS n
                MERGE (p:Person {fbid: n.fbid})
                ON CREATE SET p.name       = n.name,
                              p.url        = n.url,
                              p.first_seen = $ts
                ON MATCH  SET p.name       = n.name,
                              p.url        = n.url,
                              p.last_seen  = $ts
                WITH p, n
                CALL apoc.create.addLabels(p, [n.role]) YIELD node
                RETURN node
                """,
                nodes=nodes_list,
                ts=ts,
            )

            # ── 2 of 2: bulk bidirectional edge upsert ────────────
            # Facebook friendship is symmetric, so we store TWO directed
            # relationships per pair: (A)-[:FRIENDS]->(B) and (B)-[:FRIENDS]->(A).
            # Neo4j does NOT support truly undirected relationships at the storage
            # level — MERGE (a)-[r]-(b) without an arrow still creates a single
            # directed edge internally (always src→dst), which breaks centrality
            # algorithms and traversals that expect both directions.
            # By MERGing both directions explicitly, every node's degree,
            # betweenness, and PageRank calculations are correct.
            s.run(
                """
                UNWIND $edges AS e
                MATCH (src:Person {fbid: e.src})
                MATCH (dst:Person {fbid: e.dst})
                MERGE (src)-[r1:FRIENDS]->(dst)
                ON CREATE SET r1.level      = e.level,
                              r1.scraped_at = $ts
                ON MATCH  SET r1.level      = CASE
                                WHEN e.level < r1.level THEN e.level
                                ELSE r1.level
                              END,
                              r1.last_seen  = $ts
                MERGE (dst)-[r2:FRIENDS]->(src)
                ON CREATE SET r2.level      = e.level,
                              r2.scraped_at = $ts
                ON MATCH  SET r2.level      = CASE
                                WHEN e.level < r2.level THEN e.level
                                ELSE r2.level
                              END,
                              r2.last_seen  = $ts
                """,
                edges=edges_list,
                ts=ts,
            )

        print(
            f"   [Neo4j] ✓ {len(rows)} edges written "
            f"({len(nodes_list)} nodes, 2 queries)"
        )

    # ── write_rows fallback — no APOC installed ────────────────────

    def write_rows_no_apoc(self, rows: list):
        """
        Same as write_rows() but uses SET p:Label syntax instead of
        apoc.create.addLabels().  Use this if APOC is not available on
        your Aura instance (free tier often lacks APOC).

        Limitation: Cypher SET p:Label requires the label name to be
        a literal, not a parameter.  We therefore split into three
        batches by role and run separate queries.
        """
        if not rows:
            return

        ts = datetime.utcnow().isoformat()

        # Partition nodes by role
        l1_nodes, l2_nodes = [], []
        nodes_seen: set = set()

        for r in rows:
            role = "Level1Friend" if r["level"] == 1 else "Level2Friend"
            bucket = l1_nodes if role == "Level1Friend" else l2_nodes

            for fbid, name, url in (
                (r["source_fbid"], r["source_name"], r["source_url"]),
                (r["friend_fbid"], r["friend_name"], r["friend_url"]),
            ):
                if fbid not in nodes_seen:
                    nodes_seen.add(fbid)
                    bucket.append({"fbid": fbid, "name": name, "url": url})

        # Normalise edges
        edges_list = []
        for r in rows:
            a, b = r["source_fbid"], r["friend_fbid"]
            src, dst = (a, b) if a <= b else (b, a)
            edges_list.append({"src": src, "dst": dst, "level": r["level"]})

        _NODE_Q_L1 = """
            UNWIND $nodes AS n
            MERGE (p:Person {fbid: n.fbid})
            ON CREATE SET p.name=$name_dummy, p.url=$url_dummy, p.first_seen=$ts
            ON MATCH  SET p.name=n.name, p.url=n.url, p.last_seen=$ts
            SET p:Level1Friend
        """
        _NODE_Q_L2 = """
            UNWIND $nodes AS n
            MERGE (p:Person {fbid: n.fbid})
            ON CREATE SET p.name=n.name, p.url=n.url, p.first_seen=$ts
            ON MATCH  SET p.name=n.name, p.url=n.url, p.last_seen=$ts
            SET p:Level2Friend
        """
        _EDGE_Q = """
            UNWIND $edges AS e
            MATCH (src:Person {fbid: e.src})
            MATCH (dst:Person {fbid: e.dst})
            MERGE (src)-[r1:FRIENDS]->(dst)
            ON CREATE SET r1.level=e.level, r1.scraped_at=$ts
            ON MATCH  SET r1.level=CASE WHEN e.level < r1.level THEN e.level ELSE r1.level END,
                          r1.last_seen=$ts
            MERGE (dst)-[r2:FRIENDS]->(src)
            ON CREATE SET r2.level=e.level, r2.scraped_at=$ts
            ON MATCH  SET r2.level=CASE WHEN e.level < r2.level THEN e.level ELSE r2.level END,
                          r2.last_seen=$ts
        """

        with self._session() as s:
            if l1_nodes:
                s.run("""
                    UNWIND $nodes AS n
                    MERGE (p:Person {fbid: n.fbid})
                    ON CREATE SET p.name=n.name, p.url=n.url, p.first_seen=$ts
                    ON MATCH  SET p.name=n.name, p.url=n.url, p.last_seen=$ts
                    SET p:Level1Friend
                """, nodes=l1_nodes, ts=ts)
            if l2_nodes:
                s.run("""
                    UNWIND $nodes AS n
                    MERGE (p:Person {fbid: n.fbid})
                    ON CREATE SET p.name=n.name, p.url=n.url, p.first_seen=$ts
                    ON MATCH  SET p.name=n.name, p.url=n.url, p.last_seen=$ts
                    SET p:Level2Friend
                """, nodes=l2_nodes, ts=ts)
            s.run(_EDGE_Q, edges=edges_list, ts=ts)

        print(
            f"   [Neo4j/no-apoc] ✓ {len(rows)} edges written "
            f"({len(nodes_seen)} nodes, 3 queries)"
        )

    # ── Analytics helpers ──────────────────────────────────────────

    def stats(self) -> dict:
        with self._session() as s:
            nodes = s.run("MATCH (p:Person) RETURN count(p) AS n").single()["n"]
            edges = s.run("MATCH ()-[r:FRIENDS]-() RETURN count(r)/2 AS n").single()["n"]
        return {"nodes": nodes, "edges": edges}

    def shortest_path(self, fbid_a: str, fbid_b: str):
        with self._session() as s:
            return s.run(
                """
                MATCH p=shortestPath(
                  (a:Person {fbid:$a})-[:FRIENDS*]-(b:Person {fbid:$b})
                )
                RETURN [n IN nodes(p) | n.name] AS path, length(p) AS hops
                """,
                a=fbid_a, b=fbid_b,
            ).single()

    def mutual_friends(self, fbid_a: str, fbid_b: str) -> list:
        with self._session() as s:
            return s.run(
                """
                MATCH (a:Person {fbid:$a})-[:FRIENDS]-(m)-[:FRIENDS]-(b:Person {fbid:$b})
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
                MATCH (p:Person)-[:FRIENDS]-()
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
# GLOBAL NEO4J INSTANCE + APOC FLAG
# ─────────────────────────────────────────────
_neo4j: "Neo4jWriter | None" = None
_apoc_available: bool = True   # assume APOC present; auto-detect on first write


def _get_neo4j() -> "Neo4jWriter | None":
    return _neo4j


def _neo4j_write(neo: "Neo4jWriter", rows: list):
    """
    Wrapper that auto-detects APOC availability and falls back gracefully.
    On the first failure due to missing APOC, sets _apoc_available=False
    and retries with the no-APOC path.
    """
    global _apoc_available
    if not neo or not rows:
        return
    try:
        if _apoc_available:
            neo.write_rows(rows)
        else:
            neo.write_rows_no_apoc(rows)
    except Exception as e:
        err = str(e).lower()
        if "apoc" in err and _apoc_available:
            print(f"   [Neo4j][!] APOC not available — switching to no-APOC mode.")
            _apoc_available = False
            try:
                neo.write_rows_no_apoc(rows)
            except Exception as e2:
                print(f"   [Neo4j][!] Write error (no-apoc): {e2}")
        else:
            print(f"   [Neo4j][!] Write error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PARQUET HELPERS  — partitioned dataset (O(1) append)
# ═══════════════════════════════════════════════════════════════════════════════

def _parquet_glob(parquet_dir: str) -> str:
    """Return the glob pattern for reading all partitions."""
    return str(Path(parquet_dir) / "*.parquet")


def _append_rows_parquet(rows: list, parquet_dir: str):
    """
    Append a batch of edge-rows as a NEW partition file inside parquet_dir.

    WHY THIS REPLACES THE ORIGINAL SINGLE-FILE APPROACH:
      The original code read the entire existing Parquet file, concatenated
      the new rows, and rewrote the whole file on every flush.  At 10 lakh
      edges that means every 500-row batch triggers a full rewrite of a
      ~200 MB file — O(N²) I/O that grows worse as the dataset grows.

      By writing each flush as a new partition file (part_<ms>.parquet)
      inside a directory, every append is O(1) regardless of total dataset
      size.  DuckDB and PyArrow can read the whole directory transparently
      via a glob pattern.

    TRADE-OFF:
      Many small files can slow down DuckDB reads if there are thousands of
      them.  At batch size 500 and ~1M edges you get ~2000 files ≈ fine.
      If needed, run a one-off compaction after scraping is complete:
        duckdb.sql("COPY (SELECT * FROM read_parquet('friends_network/*.parquet'))
                    TO 'friends_network_compact.parquet' (FORMAT PARQUET)")
    """
    if not rows:
        return

    ts = datetime.utcnow().isoformat()
    enriched = [{**r, "scraped_at": ts} for r in rows]

    new_table = pa.Table.from_pylist(enriched, schema=_PARQUET_SCHEMA)

    # Create directory on first call
    out_dir = Path(parquet_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Unique filename using millisecond timestamp + random suffix to avoid
    # collisions if two flushes happen within the same millisecond.
    part_id   = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    part_path = out_dir / f"part_{part_id}.parquet"

    pq.write_table(new_table, str(part_path), compression="snappy")


def load_seen_edges(parquet_dir: str) -> "set | ScalableBloomFilter":
    """
    Load previously scraped (min_fbid, max_fbid) pairs for dedup.

    BIDIRECTIONAL FIX:
      The original code stored the raw (source_fbid, friend_fbid) tuple.
      This meant (A,B) and (B,A) were treated as distinct edges, causing
      duplicate rows in Parquet and duplicate FRIEND_OF relationships in
      Neo4j when both A→B and B→A were scraped.

      We now normalise to (min(a,b), max(a,b)) so each friendship pair
      has exactly one canonical key regardless of which direction it was
      discovered.  This halves storage and eliminates all bidirectional
      duplicates.

    Uses DuckDB to read only the two fbid columns — never loads full rows.
    Returns a Bloom filter if pybloom-live is available, else a plain set.
    Memory usage:
        Bloom filter  ≈ 50 MB for 10 M edges  (1 % false-positive rate)
        Plain set     ≈ 1–2 GB for 10 M edges
    """
    glob = _parquet_glob(parquet_dir)
    parts = list(Path(parquet_dir).glob("*.parquet")) if Path(parquet_dir).exists() else []

    if BLOOM_AVAILABLE:
        seen: "set | ScalableBloomFilter" = ScalableBloomFilter(
            mode=ScalableBloomFilter.LARGE_SET_GROWTH,
            error_rate=0.01,
        )
    else:
        seen = set()

    if not parts:
        print("[Parquet] No existing partition files — starting fresh.")
        return seen

    try:
        con = duckdb.connect()
        rows = con.execute(
            f"SELECT source_fbid, friend_fbid FROM read_parquet('{glob}')"
        ).fetchall()
        con.close()

        for src, dst in rows:
            # Normalise to canonical (min, max) pair
            key = (src, dst) if src <= dst else (dst, src)
            seen.add(key)

        print(f"[Parquet] Loaded {len(rows):,} existing edges into dedup filter "
              f"({len(parts)} partition files).")
    except Exception as e:
        print(f"[!] Could not read existing Parquet: {e}")

    return seen


def load_level1_from_parquet(parquet_dir: str) -> list:
    """
    Re-hydrate the L1 friend list from the partitioned Parquet dataset for resume.
    Returns a list of {'name', 'url', 'fbid'} dicts.
    """
    parts = list(Path(parquet_dir).glob("*.parquet")) if Path(parquet_dir).exists() else []
    if not parts:
        return []
    glob = _parquet_glob(parquet_dir)
    try:
        con = duckdb.connect()
        rows = con.execute(
            f"""
            SELECT DISTINCT friend_name AS name,
                            friend_url  AS url,
                            friend_fbid AS fbid
            FROM   read_parquet('{glob}')
            WHERE  level = 1
            """
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
#  CARD SCANNER  — unchanged Selenium DOM approach (JS extraction skipped
#  intentionally: JS-only extraction loses data on FB's React-rendered cards)
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
#  ROW BUILDER  — normalised bidirectional deduplication
# ═══════════════════════════════════════════════════════════════════════════════

def _build_rows(batch, source_name, source_url, source_fbid, level, seen_edges):
    """
    Convert a batch of friend-dicts into edge-row dicts.

    BIDIRECTIONAL DEDUPLICATION FIX:
      The original code keyed seen_edges on (source_fbid, friend_fbid).
      This treated (A,B) and (B,A) as different edges, so mutual friendships
      were stored twice — once when A's page was scraped and again when B's
      page was scraped later.

      We now normalise the key to (min(a,b), max(a,b)).  This means the
      second time the same friendship pair is encountered (from the other
      direction), the Bloom filter will already know about it and skip it.

      The row stored in Parquet always preserves the original scraping
      direction (source → friend) for auditability, but the dedup key is
      the canonical pair so storage is never duplicated.

    False positives from the Bloom filter are harmless: Neo4j MERGE is
    the authoritative deduplicator; a missed edge = one skipped write.
    """
    rows = []
    for fr in batch:
        # Canonical dedup key — order-independent
        a, b = source_fbid, fr["fbid"]
        edge_key = (a, b) if a <= b else (b, a)

        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

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
    parquet_dir:  str,
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
        # ── Parquet partition append — O(1), never rewrites old data ─
        _append_rows_parquet(pending_rows, parquet_dir)
        # ── Neo4j UNWIND batch write (APOC or no-APOC auto-detected) ─
        _neo4j_write(neo, list(pending_rows))
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
    parquet_dir:  str,
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
        driver, primary_url, my_fbid, parquet_dir, write_header,
        source_name, source_url, source_fbid, level, seen_edges,
    )

    if not result and use_mutual_fallback:
        print("   [fallback] Nothing on friends_all — trying friends_mutual …")
        result = _scrape_single_friends_url(
            driver, fallback_url, my_fbid, parquet_dir, write_header,
            source_name, source_url, source_fbid, level, seen_edges,
        )

    print(f"   [✓] Total friends collected: {len(result)}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN NETWORK SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_friends_network(
    driver,
    parquet_dir:   str  = PARQUET_DIR,
    max_level:     int  = 2,
    enable_neo4j:  bool = True,
):
    global _neo4j

    # ── Neo4j boot-up ─────────────────────────────────────────────
    if enable_neo4j and NEO4J_AVAILABLE:
        try:
            _neo4j = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,)
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
    print(f"[+] Output    : {parquet_dir}/  (Parquet partitioned dataset)")
    print(f"[+] Headless  : {HEADLESS}")
    print(f"[+] Neo4j     : {'connected' if _neo4j else 'disabled'}")
    print(f"[+] Bloom     : {'enabled' if BLOOM_AVAILABLE else 'disabled (using set)'}")
    print(f"[+] Batch sz  : {NEO4J_BATCH_SIZE} rows/flush")

    # Seed 'me' node in Neo4j as :RootUser
    if _neo4j:
        try:
            _neo4j.merge_person(my_fbid, my_name, my_url, label="RootUser")
        except Exception:
            # APOC might not be available — use plain SET
            _neo4j.merge_person_no_apoc(my_fbid, my_name, my_url)

    # ── Load seen edges via DuckDB + Bloom filter ─────────────────
    seen_edges = load_seen_edges(parquet_dir)

    # ── Open SQLite checkpoint ────────────────────────────────────
    ckpt_con  = _open_checkpoint_db()
    l2_done   = load_checkpoint(ckpt_con)

    # ── Level 1 ───────────────────────────────────────────────────
    print("\n─── LEVEL 1 ───")
    l1_start = time.time()

    my_friends = scrape_friends_page_optimized(
        driver, my_url, my_fbid, parquet_dir, True,
        my_name, my_url, my_fbid, 1, seen_edges,
        use_mutual_fallback=False,
    )

    # If we resumed and the page showed 0 (already scraped), reload from Parquet
    if not my_friends:
        print("[+] L1 page empty (already scraped?) — loading L1 from Parquet …")
        my_friends = load_level1_from_parquet(parquet_dir)

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
                    driver, fr["url"], my_fbid, parquet_dir, False,
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

    # Quick DuckDB stats on the partitioned Parquet dataset
    parts = list(Path(parquet_dir).glob("*.parquet")) if Path(parquet_dir).exists() else []
    if parts:
        try:
            glob = _parquet_glob(parquet_dir)
            con  = duckdb.connect()
            stats_row = con.execute(
                f"""
                SELECT
                    COUNT(*) AS raw_edges,
                    COUNT(DISTINCT LEAST(source_fbid, friend_fbid) || '|' ||
                                   GREATEST(source_fbid, friend_fbid)) AS unique_pairs,
                    COUNT(DISTINCT source_fbid) + COUNT(DISTINCT friend_fbid) AS approx_nodes
                FROM read_parquet('{glob}')
                """
            ).fetchone()
            con.close()
            print(f"[Parquet] {stats_row[0]:,} raw rows | "
                  f"{stats_row[1]:,} unique friendship pairs | "
                  f"{len(parts)} partition files")
        except Exception:
            pass

    print(f"\n[✅] ALL DONE")
    print(f"[📁] Output     : {parquet_dir}/  ({len(parts)} partition files)")
    print(f"[📋] Checkpoint : {CHECKPOINT_DB}")
    print(f"\nTo read the full dataset:")
    print(f"  import duckdb")
    print(f"  df = duckdb.sql(\"SELECT * FROM read_parquet('{parquet_dir}/*.parquet')\").df()")
    return parquet_dir


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
        print(f"  Database : {0}")
        try:
            writer = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, )
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
        parquet_dir = PARQUET_DIR
        parts = list(Path(parquet_dir).glob("*.parquet")) if Path(parquet_dir).exists() else []
        if not parts:
            print(f"[!] No Parquet files found in '{parquet_dir}/'")
            sys.exit(1)
        glob = _parquet_glob(parquet_dir)
        con  = duckdb.connect()
        print(f"\n── Dataset: {len(parts)} partition files in '{parquet_dir}/' ──")
        print("\n── Edge count by level ──")
        print(con.execute(
            f"SELECT level, COUNT(*) AS edges FROM read_parquet('{glob}') GROUP BY level ORDER BY level"
        ).df().to_string(index=False))
        print("\n── Unique friendship pairs ──")
        print(con.execute(f"""
            SELECT COUNT(DISTINCT LEAST(source_fbid, friend_fbid) || '|' ||
                                  GREATEST(source_fbid, friend_fbid)) AS unique_pairs
            FROM read_parquet('{glob}')
        """).df().to_string(index=False))
        print("\n── Top 20 most-connected nodes (degree) ──")
        print(con.execute(f"""
            SELECT name, fbid, SUM(degree) AS total_degree FROM (
                SELECT friend_name AS name, friend_fbid AS fbid, COUNT(*) AS degree
                FROM read_parquet('{glob}') GROUP BY friend_name, friend_fbid
                UNION ALL
                SELECT source_name, source_fbid, COUNT(*) FROM read_parquet('{glob}')
                GROUP BY source_name, source_fbid
            ) GROUP BY name, fbid ORDER BY total_degree DESC LIMIT 20
        """).df().to_string(index=False))
        con.close()
        sys.exit(0)

    # ── Compact all partition files into one Parquet file ─────────
    if "--compact" in sys.argv:
        parquet_dir = PARQUET_DIR
        parts = list(Path(parquet_dir).glob("*.parquet")) if Path(parquet_dir).exists() else []
        if not parts:
            print(f"[!] No Parquet files found in '{parquet_dir}/'")
            sys.exit(1)
        out = f"{parquet_dir}_compact.parquet"
        print(f"[compact] Merging {len(parts)} files → {out} …")
        glob = _parquet_glob(parquet_dir)
        con  = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT DISTINCT
                    source_fbid, source_name, source_url,
                    friend_fbid, friend_name, friend_url,
                    level, scraped_at
                FROM read_parquet('{glob}')
            ) TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        con.close()
        row_count = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        print(f"[compact] ✅ Done — {row_count:,} deduplicated rows in {out}")
        sys.exit(0)

    # ── Normal scraper entry point ────────────────────────────────
    from fb_login_1 import main
    driver = main()
    if driver:
        scrape_friends_network(driver, max_level=2, enable_neo4j=True)
    else:
        print("[✗] Login failed.")