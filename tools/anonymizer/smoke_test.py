#!/usr/bin/env python3
"""
tools/anonymizer/smoke_test.py

End-to-end smoke test:
- Loads .env if present
- Connects to source & masked DBs
- Resets masked DB, runs mask_db.py with DRY_LIMIT (subset) or full copy
- Performs sanity checks
- Optional deep profiling (schema-level stats; NO raw values)
- Optional AI summary via OpenAI

Usage:
  python smoke_test.py [--limit 200] [--deep] [--ai]
"""

from __future__ import annotations

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras as ex


# -----------------------------
# Env helpers
# -----------------------------
def load_dotenv_from_here():
    """Load .env if present in the current directory (tools/anonymizer)."""
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def env(key, default=None, required=False):
    val = os.environ.get(key, default)
    if required and (val is None or val == ""):
        print(f"❌ Missing required env var: {key}", file=sys.stderr)
        sys.exit(1)
    return val


# -----------------------------
# DB helpers
# -----------------------------
def connect(dbname):
    return psycopg2.connect(
        host=env("PGHOST", "127.0.0.1"),
        port=env("PGPORT", "5432"),
        user=env("PGUSER", "postgres"),
        password=env("PGPASSWORD", ""),
        dbname=dbname,
    )


def one_value(conn, sql, params=None):
    with conn.cursor() as cur:
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def rows(conn, sql, params=None):
    with conn.cursor(cursor_factory=ex.RealDictCursor) as cur:
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchall()


def reset_masked_db(dbname):
    conn = connect(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE reviews, orders, users, products RESTART IDENTITY CASCADE;")
        conn.commit()
    finally:
        conn.close()


def has_table(conn, table: str) -> bool:
    return bool(
        one_value(
            conn,
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s;
            """,
            (table,),
        )
    )


def list_tables(conn) -> List[str]:
    rs = rows(
        conn,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        ORDER BY table_name;
        """,
    )
    return [r["table_name"] for r in rs]


def list_columns(conn, table) -> List[Dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position;
        """,
        (table,),
    )


def is_textual(pg_type: str) -> bool:
    return pg_type in {"text", "character varying", "character", "varchar", "char", "citext"}


def is_numeric(pg_type: str) -> bool:
    return pg_type in {
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "double precision",
    }


def is_temporal(pg_type: str) -> bool:
    return pg_type in {
        "date",
        "timestamp without time zone",
        "timestamp with time zone",
        "time without time zone",
        "time with time zone",
    }


# -----------------------------
# Pipeline steps
# -----------------------------
def run_masker(dry_limit: int | None):
    env_copy = os.environ.copy()
    if dry_limit:
        env_copy["DRY_LIMIT"] = str(dry_limit)
        print(f"▶ Running mask_db.py with DRY_LIMIT={dry_limit} …")
    else:
        env_copy.pop("DRY_LIMIT", None)
        print("▶ Running mask_db.py (full) …")
    proc = subprocess.run([sys.executable, "mask_db.py"], cwd=os.path.dirname(__file__), env=env_copy)
    if proc.returncode != 0:
        print("❌ mask_db.py failed", file=sys.stderr)
        sys.exit(proc.returncode)
    print("✅ mask_db.py completed")


def sanity_checks(dst_db):
    print("▶ Running sanity checks on masked DB …")
    conn = connect(dst_db)

    checks = {}
    # Row counts
    checks["row_counts"] = {
        "users": one_value(conn, "SELECT COUNT(*) FROM users;"),
        "products": one_value(conn, "SELECT COUNT(*) FROM products;"),
        "orders": one_value(conn, "SELECT COUNT(*) FROM orders;"),
        "reviews": one_value(conn, "SELECT COUNT(*) FROM reviews;"),
    }

    # Email uniqueness & loose validity
    checks["email_dupes"] = one_value(
        conn,
        "SELECT COUNT(*) FROM (SELECT email, COUNT(*) c FROM users GROUP BY 1 HAVING COUNT(*)>1) s;",
    )
    checks["email_invalid"] = one_value(
        conn,
        "SELECT COUNT(*) FROM users WHERE email NOT LIKE '%@%.__%';",
    )

    # FK orphans
    checks["orphans"] = {
        "orders_user": one_value(
            conn,
            "SELECT COUNT(*) FROM orders o LEFT JOIN users u ON u.id=o.user_id WHERE u.id IS NULL;",
        ),
        "orders_product": one_value(
            conn,
            "SELECT COUNT(*) FROM orders o LEFT JOIN products p ON p.id=o.product_id WHERE p.id IS NULL;",
        ),
        "reviews_user": one_value(
            conn,
            "SELECT COUNT(*) FROM reviews r LEFT JOIN users u ON u.id=r.user_id WHERE u.id IS NULL;",
        ),
        "reviews_product": one_value(
            conn,
            "SELECT COUNT(*) FROM reviews r LEFT JOIN products p ON p.id=r.product_id WHERE p.id IS NULL;",
        ),
    }

    conn.close()
    return checks


# -----------------------------
# Deep profiling (no raw values)
# -----------------------------
def safe_ratio(n: int, d: int) -> float:
    return (float(n) / d) if d else 0.0


def profile_column(conn, table: str, col: str, pg_type: str) -> Dict[str, Any]:
    prof: Dict[str, Any] = {}
    tot = one_value(conn, f'SELECT COUNT(*) FROM "{table}";') or 0
    nulls = one_value(conn, f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL;') or 0
    distinct = one_value(conn, f'SELECT COUNT(DISTINCT "{col}") FROM "{table}";') or 0
    prof["counts"] = {"total": tot, "nulls": nulls, "distinct": distinct, "null_rate": safe_ratio(nulls, tot)}

    # Type-specific summaries (no values)
    if is_textual(pg_type):
        # length summaries (min/avg/max) without revealing content
        rs = rows(
            conn,
            f"""
            SELECT
              MIN(LENGTH("{col}")) AS min_len,
              AVG(LENGTH("{col}")) AS avg_len,
              MAX(LENGTH("{col}")) AS max_len
            FROM "{table}"
            WHERE "{col}" IS NOT NULL;
            """,
        )
        prof["text_lengths"] = rs[0] if rs else {"min_len": None, "avg_len": None, "max_len": None}

    if is_numeric(pg_type):
        rs = rows(
            conn,
            f"""
            SELECT
              MIN("{col}") AS min_val,
              AVG("{col}") AS avg_val,
              MAX("{col}") AS max_val
            FROM "{table}"
            WHERE "{col}" IS NOT NULL;
            """,
        )
        prof["numeric_summary"] = rs[0] if rs else {"min_val": None, "avg_val": None, "max_val": None}

    if is_temporal(pg_type):
        rs = rows(
            conn,
            f"""
            SELECT
              MIN("{col}") AS min_ts,
              MAX("{col}") AS max_ts
            FROM "{table}"
            WHERE "{col}" IS NOT NULL;
            """,
        )
        prof["temporal_range"] = rs[0] if rs else {"min_ts": None, "max_ts": None}

    return prof


PII_PATTERNS = {
    "email": r'[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}',
    "phone": r'(?:(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,4}\)?[\s-]?)?\d{3}[\s-]?\d{4})',
    "ssn_like": r'\b\d{3}-\d{2}-\d{4}\b',
    "cc_last4": r'\b\d{4}\b',
}


def pii_scan(conn, table: str, col: str, pg_type: str) -> Dict[str, Any]:
    """Only scan textual columns. For others, return skipped."""
    out = {"table": table, "column": col, "type": pg_type, "hits": {}, "scanned": False}
    if not is_textual(pg_type):
        out["skipped_reason"] = "non-text column"
        return out
    out["scanned"] = True
    for name, pattern in PII_PATTERNS.items():
        # Use REGEXP_MATCHES safe on text; cast to text for safety
        out["hits"][name] = one_value(
            conn,
            f'SELECT COUNT(*) FROM "{table}" WHERE "{col}"::text ~ %s;',
            (pattern,),
        ) or 0
    return out


def deep_profile(src_db: str, dst_db: str, sample_tables: List[str] | None = None, limit_cols: int = 10) -> Dict[str, Any]:
    src = connect(src_db)
    dst = connect(dst_db)
    try:
        # Drive table list from masked DB; optionally filter
        tables = sample_tables or list_tables(dst)
        src_tables = set(list_tables(src))

        payload: Dict[str, Any] = {"src_db": src_db, "dst_db": dst_db, "tables": []}

        for t in tables:
            # Example optional filter: exclude helper tables if desired
            # if t.startswith("masking_"):
            #     continue

            dst_cols = list_columns(dst, t)[:limit_cols]
            table_block: Dict[str, Any] = {"table": t, "in_src": (t in src_tables), "columns": [], "pii_scans": []}

            for c in dst_cols:
                col_name = c["column_name"]
                data_type = c["data_type"]

                # Profile in dst always
                dst_prof = profile_column(dst, t, col_name, data_type)

                # Profile in src only if table exists there
                if t in src_tables:
                    src_prof = profile_column(src, t, col_name, data_type)
                else:
                    src_prof = {"missing": True}

                table_block["columns"].append(
                    {"column": col_name, "type": data_type, "src": src_prof, "dst": dst_prof}
                )

                table_block["pii_scans"].append(pii_scan(dst, t, col_name, data_type))

            payload["tables"].append(table_block)

        return payload
    finally:
        src.close()
        dst.close()


# -----------------------------
# AI summary (optional)
# -----------------------------
def maybe_ai_summary(checks: Dict[str, Any], deep_payload: Dict[str, Any] | None) -> str | None:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ℹ️ AI summary disabled (set OPENAI_API_KEY to enable).")
        return None

    try:
        # Works for openai 1.47.0 and >=1.106
        from openai import OpenAI

        client = OpenAI(api_key=api_key)  # correct signature; no proxies arg
    except Exception as e:
        print(f"⚠️ OpenAI summary skipped: client init failed: {e}")
        return None

    prompt = {
        "checks": checks,
        "deep_profile": deep_payload or {},
        "instruction": (
            "You are a data quality assistant. Summarize the JSON using brief bullets. "
            "Call out potential privacy risks (PII patterns), high null rates, very low distinctness, "
            "and any table present only in masked DB. Do NOT reveal any actual values."
        ),
    }

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data quality assistant."},
                {"role": "user", "content": json.dumps(prompt, indent=2, default=str)},
            ],
            temperature=0.2,
            max_tokens=350,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ OpenAI summary failed: {e}")
        return None


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=25, help="Rows per table for DRY run (0 = full).")
    parser.add_argument("--ai", action="store_true", help="Generate AI summary.")
    parser.add_argument("--deep", action="store_true", help="Run deep profiling (schema-level; no values).")
    args = parser.parse_args()

    # Ensure we run from tools/anonymizer
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Load .env locally
    load_dotenv_from_here()

    src_db = env("SRC_DB", required=True)
    dst_db = env("DST_DB", required=True)

    print("=== Smoke Test: start ===")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"SRC_DB={src_db}  DST_DB={dst_db}")

    # DB connectivity
    try:
        with connect(src_db) as c:
            one_value(c, "SELECT 1;")
        with connect(dst_db) as c:
            one_value(c, "SELECT 1;")
        print("✅ DB connectivity OK")
    except Exception as e:
        print(f"❌ DB connectivity failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Reset masked DB and run the anonymizer on a small slice (or full)
    reset_masked_db(dst_db)
    dry_limit = args.limit if args.limit > 0 else None
    run_masker(dry_limit=dry_limit)

    # Sanity checks
    checks = sanity_checks(dst_db)
    print("=== Sanity checks (masked DB) ===")
    print(json.dumps(checks, indent=2))

    deep_payload = None
    if args.deep:
        print("\n▶ Running deep profiling (no raw values) …")
        try:
            deep_payload = deep_profile(src_db, dst_db, sample_tables=None, limit_cols=12)
            # Keep the stdout compact; print a small summary (counts only)
            summary_view = {
                "tables_profiled": len(deep_payload.get("tables", [])),
                "example": deep_payload.get("tables", [])[:1],  # include just 1 table block as a preview
            }
            print(json.dumps(summary_view, indent=2, default=str))
        except Exception as e:
            print(f"⚠️ Deep profiling failed: {e}")

    # Optional AI summary
    if args.ai:
        summary = maybe_ai_summary(checks, deep_payload)
        if summary:
            print("\n=== AI summary ===")
            print(summary)

    # Tip if the slice is small
    if dry_limit and checks["row_counts"]["orders"] == 0:
        print(
            "\nℹ️ Tip: orders are 0 in this slice. Try a larger sample:\n"
            "   python smoke_test.py --limit 200"
        )

    print("\n=== Smoke Test: done ===")


if __name__ == "__main__":
    main()
