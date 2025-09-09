#!/usr/bin/env python3
"""
smoke_test.py — Prod DB Insight (schema-first, metadata-only, zero raw data)

What this does:
  • Connects to the SOURCE (prod) database defined by env vars
  • Reads Postgres metadata (pg_catalog, information_schema, pg_stats)
  • Produces an executive summary:
      - Tables by size & estimated rows
      - Columns (type, nullability, default) + PII likelihood heuristic
      - PKs, FKs, indexes
      - Data-quality signals from pg_stats (null_frac, n_distinct, MCVs)
      - Top risks (high nulls, low distinctness, potential PII)
  • Outputs JSON (default) or Markdown (--format md)
  • No table scans; no raw values; very fast & safe

Env vars (same as your project):
  PGHOST, PGPORT, PGUSER, PGPASSWORD, SRC_DB  (required)
Optional:
  SCHEMAS   comma-separated, default "public"
  TZ        timezone label for generated_at, default "UTC"

Usage examples:
  python smoke_test.py --prod-insight
  python smoke_test.py --prod-insight --schema-only
  python smoke_test.py --prod-insight --format md > prod_insight.md
  SCHEMAS=public,analytics python smoke_test.py --prod-insight

Windows:
  py smoke_test.py --prod-insight

Requires: psycopg2 (already in your requirements)
Optionally uses: python-dotenv (auto-load .env if available)
"""

from __future__ import annotations
import os
import re
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

# Optional .env loading (non-fatal if missing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import psycopg2
import psycopg2.extras as ex


# ---------- Utilities ----------

def get_schemas_from_env() -> List[str]:
    raw = os.environ.get("SCHEMAS", "public")
    return [s.strip() for s in raw.split(",") if s.strip()]

def now_iso() -> str:
    tzlabel = os.environ.get("TZ", "UTC")
    return f"{datetime.now(timezone.utc).isoformat()} ({tzlabel})"

def prod_conn():
    dsn = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", 5432)),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
        "dbname": os.environ["SRC_DB"],   # raises KeyError if missing -> clearer failure
    }

  # Print all values
    print(">>> Effective DB Connection Settings:")
    for k, v in dsn.items():
        print(f"    {k:8s} = {v}")
    return psycopg2.connect(**dsn)

def qall(conn, sql: str, params=None) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=ex.RealDictCursor) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


# ---------- Metadata Queries (fast, no scans) ----------

def q_tables(conn, schemas: List[str]):
    sql = """
    SELECT n.nspname AS schema,
           c.relname AS table,
           COALESCE(c.reltuples, 0)::bigint AS row_est,
           pg_relation_size(c.oid) AS rel_bytes,
           pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = ANY(%s)
    ORDER BY total_bytes DESC, n.nspname, c.relname;
    """
    return qall(conn, sql, (schemas,))



def _normalize_aliases(rows):
    """Ensure rows have 'schema', 'table', 'column' keys even if queries used schema_name/table_name/column_name."""
    normed = []
    for r in rows:
        if isinstance(r, dict):
            rr = dict(r)
            if 'schema' not in rr and 'schema_name' in rr:
                rr['schema'] = rr['schema_name']
            if 'table' not in rr and 'table_name' in rr:
                rr['table'] = rr['table_name']
            if 'column' not in rr and 'column_name' in rr:
                rr['column'] = rr['column_name']
            if 'ref_schema' not in rr and 'ref_schema_name' in rr:
                rr['ref_schema'] = rr['ref_schema_name']
            if 'ref_table' not in rr and 'ref_table_name' in rr:
                rr['ref_table'] = rr['ref_table_name']
            if 'ref_column' not in rr and 'ref_column_name' in rr:
                rr['ref_column'] = rr['ref_column_name']
            normed.append(rr)
        else:
            normed.append(r)
    return normed

def q_columns(conn, schemas: List[str]):
    sql = """
    SELECT table_schema AS schema,
           table_name   AS table,
           column_name,
           data_type,
           is_nullable,
           column_default
    FROM information_schema.columns
    WHERE table_schema = ANY(%s)
    ORDER BY table_schema, table_name, ordinal_position;
    """
    return qall(conn, sql, (schemas,))

def q_foreign_keys(conn, schemas: List[str]):
    sql = """
    SELECT
      tc.constraint_schema AS schema_name,
      tc.table_name        AS table_name,
      kcu.column_name      AS column_name,
      ccu.table_schema     AS ref_schema_name,
      ccu.table_name       AS ref_table_name,
      ccu.column_name      AS ref_column_name,
      rc.update_rule,
      rc.delete_rule
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
     AND tc.table_name   = kcu.table_name
    JOIN information_schema.referential_constraints rc
      ON rc.constraint_name = tc.constraint_name
     AND rc.constraint_schema = tc.constraint_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = rc.unique_constraint_name
     AND ccu.constraint_schema = rc.unique_constraint_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = ANY(%s)
    ORDER BY schema_name, table_name, column_name;
    """
    return qall(conn, sql, (schemas,))

def q_indexes(conn, schemas: List[str]):
    sql = """
    SELECT n.nspname AS schema, c.relname AS table, i.relname AS index,
           pg_relation_size(i.oid) AS idx_bytes,
           ix.indisunique, ix.indisvalid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_index ix ON ix.indrelid = c.oid
    JOIN pg_class i ON i.oid = ix.indexrelid
    WHERE c.relkind = 'r'
      AND n.nspname = ANY(%s)
    ORDER BY idx_bytes DESC, n.nspname, c.relname;
    """
    return qall(conn, sql, (schemas,))

def q_pg_stats(conn, schemas: List[str]):
    sql = """
    SELECT schemaname AS schema,
           tablename  AS table,
           attname    AS column,
           null_frac,
           n_distinct,
           most_common_vals,
           most_common_freqs
    FROM pg_stats
    WHERE schemaname = ANY(%s)
    ORDER BY schemaname, tablename, attname;
    """
    return qall(conn, sql, (schemas,))


# ---------- Heuristics & Summaries ----------

PII_PATTERN = re.compile(
    r"(email|e-mail|mail|name|firstname|lastname|surname|phone|mobile|msisdn|"
    r"address|street|postcode|zipcode|zip|iban|bic|card|cc|pan|ssn|dob|birth|passport)",
    re.IGNORECASE
)

def pii_likelihood(col_name: str, data_type: str | None) -> str:
    score = 0
    if PII_PATTERN.search(col_name or ""):
        score += 2
    if data_type and data_type.lower() in ("text", "character varying", "citext"):
        score += 1
    # 0=low, 1=medium, 2+=high
    return ("low", "medium", "high")[min(score, 2)]

def summarize_pg_stat(stat_row: Dict[str, Any]) -> Dict[str, Any]:
    nd = stat_row.get("n_distinct")
    distinct_desc = None
    if nd is not None:
        if nd > 0:
            distinct_desc = f"~{int(nd)} distinct"
        elif nd < 0:
            # negative means fraction of rows: -0.5 => 50% distinct
            distinct_desc = f"{abs(nd):.2f}× rows (distinctness fraction)"
    return {
        "null_frac": stat_row.get("null_frac"),
        "distinctness": distinct_desc,
        "mcv_present": bool(stat_row.get("most_common_vals")),
    }

def risk_rank_tables(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Simple ranking: largest tables first
    return sorted(tables, key=lambda t: (t.get("size_bytes", 0), t.get("row_est", 0)), reverse=True)

def detect_top_risks(tables: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    high_nulls = []
    low_distinct = []
    pii_hotspots = []

    for t in tables:
        tname = f'{t["schema"]}.{t["table"]}'
        for c in t.get("columns", []):
            q = c.get("quality", {})
            nf = q.get("null_frac")
            if isinstance(nf, (int, float)) and nf >= 0.30:
                high_nulls.append(f"{tname}.{c['name']} (null_frac={nf:.2f})")

            dist = q.get("distinctness") or ""
            if "distinctness fraction" in dist:
                # parse like "0.05× rows (distinctness fraction)"
                try:
                    frac = float(dist.split("×")[0])
                    if frac <= 0.10:
                        low_distinct.append(f"{tname}.{c['name']} (~{frac:.2f} distinct)")
                except Exception:
                    pass

            if c.get("pii_likelihood") == "high":
                pii_hotspots.append(f"{tname}.{c['name']} ({c.get('type','')})")

    return {
        "high_null_rate_columns": sorted(high_nulls)[:25],
        "low_distinctness_columns": sorted(low_distinct)[:25],
        "potential_pii_columns": sorted(pii_hotspots)[:25],
    }


# ---------- Builder ----------



def q_primary_keys(conn, schemas):
    sql = """
    SELECT
        tc.table_schema      AS schema_name,
        tc.table_name        AS table_name,
        kcu.column_name      AS column_name,
        kcu.ordinal_position AS ordinal_position,
        tc.constraint_name   AS constraint_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
     AND tc.table_name   = kcu.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = ANY(%s)
    ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;
    """
    return qall(conn, sql, (schemas,))

def build_prod_insight(conn, schemas: List[str], schema_only: bool = False) -> Dict[str, Any]:
    tables = q_tables(conn, schemas)
    tables = _normalize_aliases(tables)
    columns = q_columns(conn, schemas)
    columns = _normalize_aliases(columns)
    pks = q_primary_keys(conn, schemas)
    pks = _normalize_aliases(pks)
    fks = q_foreign_keys(conn, schemas)
    fks = _normalize_aliases(fks)
    idxs = q_indexes(conn, schemas)
    idxs = _normalize_aliases(idxs)

    # Organize by table key
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in tables:
        key = (t["schema"], t["table"])
        by_key[key] = {
            "schema": t["schema"],
            "table": t["table"],
            "row_est": int(t.get("row_est", 0)),
            "size_bytes": int(t.get("total_bytes", 0)),
            "columns": [],
            "primary_key": [],
            "foreign_keys": [],
            "indexes": [],
        }

    for c in columns:
        key = (c["schema"], c["table"])
        if key in by_key:
            by_key[key]["columns"].append({
                "name": c["column_name"],
                "type": c["data_type"],
                "nullable": (c["is_nullable"] == "YES"),
                "default": c["column_default"],
                "pii_likelihood": pii_likelihood(c["column_name"], c["data_type"]),
            })

    for p in pks:
        key = (p["schema"], p["table"])
        if key in by_key:
            by_key[key]["primary_key"].append(p["column"])

    for f in fks:
        key = (f["schema"], f["table"])
        if key in by_key:
            by_key[key]["foreign_keys"].append({
                "column": f["column"],
                "ref": f'{f["ref_schema"]}.{f["ref_table"]}.{f["ref_column"]}',
                "on_update": f["update_rule"],
                "on_delete": f["delete_rule"],
            })

    for i in idxs:
        key = (i["schema"], i["table"])
        if key in by_key:
            by_key[key]["indexes"].append({
                "name": i["index"],
                "bytes": int(i["idx_bytes"]),
                "unique": bool(i["indisunique"]),
                "valid": bool(i["indisvalid"]),
            })

    tables_out = list(by_key.values())
    # Short-circuit if schema only
    if schema_only:
        return {
            "generated_at": now_iso(),
            "db": os.environ.get("SRC_DB"),
            "schemas": schemas,
            "tables": risk_rank_tables(tables_out),
            "top_risks": {"note": "Skipped (schema-only)"},
        }

    # Attach pg_stats
    stats = q_pg_stats(conn, schemas)
    stats_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in stats:
        stats_map.setdefault((s["schema"], s["table"]), {})[s["column"]] = s

    for t in tables_out:
        per_col = stats_map.get((t["schema"], t["table"]), {})
        cols_new = []
        for c in t["columns"]:
            sc = per_col.get(c["name"])
            c2 = dict(c)
            if sc:
                c2["quality"] = summarize_pg_stat(sc)
            cols_new.append(c2)
        t["columns"] = cols_new

    insight = {
        "generated_at": now_iso(),
        "db": os.environ.get("SRC_DB"),
        "schemas": schemas,
        "tables": risk_rank_tables(tables_out),
    }
    insight["top_risks"] = detect_top_risks(insight["tables"])
    return insight


# ---------- Renderers ----------

def as_markdown(report: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"# Prod DB Insight — {report.get('db')}")
    lines.append(f"_Generated: {report.get('generated_at')}_")
    lines.append("")
    lines.append("## Top Risks")
    tr = report.get("top_risks", {})
    for key, items in tr.items():
        lines.append(f"- **{key.replace('_',' ').title()}** ({len(items)}):")
        for v in items[:10]:
            lines.append(f"  - {v}")
        if len(items) > 10:
            lines.append(f"  - … (+{len(items)-10} more)")
    lines.append("")
    lines.append("## Largest Tables")
    for t in report.get("tables", [])[:10]:
        lines.append(f"- `{t['schema']}.{t['table']}` — rows≈{t['row_est']:,}, size={t['size_bytes']:,} bytes")
    lines.append("")
    lines.append("## Notes")
    lines.append("- This report uses Postgres metadata only (no scans, no raw values).")
    lines.append("- `null_frac` and `n_distinct` come from `pg_stats` (estimates).")
    lines.append("- PII likelihood is heuristic (column name/type).")
    return "\n".join(lines)


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Prod DB metadata-based smoke test / insight.")
    ap.add_argument("--prod-insight", action="store_true",
                    help="Generate schema/data-quality insight from metadata.")
    ap.add_argument("--schema-only", action="store_true",
                    help="With --prod-insight, skip pg_stats (fastest).")
    ap.add_argument("--format", choices=["json", "md"], default="json",
                    help="Output format (default json).")
    ap.add_argument("--out", default=None,
                    help="Optional output file path; default prints to stdout.")
    args = ap.parse_args()

    if not args.prod_insight:
        # Default to insight mode if user forgot the flag (nice UX).
        args.prod_insight = True

    schemas = get_schemas_from_env()

    with prod_conn() as conn:
        report = build_prod_insight(conn, schemas=schemas, schema_only=args.schema_only)

    if args.format == "md":
        output = as_markdown(report)
    else:
        output = json.dumps(report, indent=2, default=str)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(output)
    else:
        print(output)


if __name__ == "__main__":
    main()

