# infer_faker_mapping.py
# Generate a faker_mapping.yaml by inspecting a Postgres schema and samples.
# - Infers Faker providers per column (by name, type, and sample values)
# - Records constraints (PK/UNIQUE/FKs)
# - YAML-safe (converts psycopg2 RealDictRow and sets to lists/dicts)

import os, re, json, statistics
from collections import defaultdict
from typing import Dict, Any, List, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import yaml
from dotenv import load_dotenv

load_dotenv()

PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGDATABASE = os.getenv("PGDATABASE", "postgres")

SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", "200"))
MAX_DISTINCT_ENUM = int(os.getenv("MAX_DISTINCT_ENUM", "30"))  # treat text columns with <=N distinct as enum-like

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", re.I)

# Column name heuristics → candidate faker
NAME_HINTS = [
    (re.compile(r"email", re.I), "internet.email"),
    (re.compile(r"(full_)?name|first_name|last_name", re.I), "person.name"),
    (re.compile(r"user(name)?", re.I), "internet.user_name"),
    (re.compile(r"password|passwd|pwd", re.I), "password"),
    (re.compile(r"phone|mobile|tel", re.I), "phone_number"),
    (re.compile(r"\bcity\b", re.I), "address.city"),
    (re.compile(r"\bcountry\b", re.I), "address.country"),
    (re.compile(r"postcode|postal|zip", re.I), "address.postcode"),
    (re.compile(r"address", re.I), "address.street_address"),
    (re.compile(r"sku|code", re.I), "bothify:????-######"),
    (re.compile(r"url|link", re.I), "internet.url"),
    (re.compile(r"\bip\b", re.I), "internet.ipv4"),
    (re.compile(r"category|status|type", re.I), "enum"),
    (re.compile(r"description|summary|text|comment|note|body", re.I), "text"),
    (re.compile(r"title|subject", re.I), "sentence"),
    (re.compile(r"price|amount|total|cost|balance", re.I), "pyfloat"),
    (re.compile(r"quantity|qty|count", re.I), "pyint"),
    (re.compile(r"rating|score", re.I), "pyint"),
    (re.compile(r"created|updated|date|time|_at$", re.I), "date_time"),
]

def connect():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )

def fetch_tables(conn) -> List[str]:
    q = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
    ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return [r[0] for r in cur.fetchall()]

def fetch_columns(conn, table: str) -> List[Dict[str, Any]]:
    q = """
    SELECT c.column_name, c.data_type, (c.is_nullable='YES') AS is_nullable, c.column_default
    FROM information_schema.columns c
    WHERE c.table_schema='public' AND c.table_name=%s
    ORDER BY c.ordinal_position;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (table,))
        return cur.fetchall()

def fetch_constraints(conn, table: str) -> Dict[str, Any]:
    d = {"pk": set(), "unique": set(), "fks": []}
    # PK / UNIQUE
    q = """
    SELECT kcu.column_name, tc.constraint_type
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
     AND tc.table_name = kcu.table_name
    WHERE tc.table_schema='public' AND tc.table_name=%s
      AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE');
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        for col, ctype in cur.fetchall():
            if ctype == 'PRIMARY KEY':
                d["pk"].add(col)
            elif ctype == 'UNIQUE':
                d["unique"].add(col)

    # FKs
    qf = """
    SELECT kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type='FOREIGN KEY'
      AND tc.table_schema='public'
      AND tc.table_name=%s;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(qf, (table,))
        d["fks"] = cur.fetchall()
    return d

def sample_table(conn, table: str, limit=SAMPLE_ROWS) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f'SELECT * FROM "{table}" ORDER BY 1 LIMIT %s;', (limit,))
        return cur.fetchall()

def looks_like_email(values: List[str]) -> bool:
    n = min(30, len(values))
    if n == 0: return False
    hits = sum(1 for v in values[:n] if isinstance(v, str) and EMAIL_RE.match(v or ""))
    return hits >= max(3, int(0.5 * n))

def enum_candidates(values: List[Any], max_distinct=MAX_DISTINCT_ENUM) -> List[Any]:
    # Treat short text columns with limited distinct values as enum-like
    vals = [v for v in values if isinstance(v, str) and v.strip() != ""]
    distinct = list({v.strip() for v in vals})
    if 1 <= len(distinct) <= max_distinct:
        return sorted(distinct)
    return []

def numeric_stats(values: List[Any]) -> Tuple[float, float]:
    nums = []
    for v in values:
        try:
            if v is None: continue
            nums.append(float(v))
        except Exception:
            pass
    if not nums:
        return (0.0, 1.0)
    return (min(nums), max(nums))

def guess_provider(colname: str, dtype: str, sample_vals: List[Any], is_unique: bool):
    # Column-name hint first
    for rx, provider in NAME_HINTS:
        if rx.search(colname):
            if provider == "enum":
                enums = enum_candidates(sample_vals)
                return ("enum", {"choices": enums} if enums else None)
            if provider == "pyint" and dtype not in ("integer", "bigint", "smallint"):
                continue
            if provider == "pyfloat" and dtype not in ("numeric","real","double precision","integer","bigint","smallint"):
                continue
            if provider == "bothify:????-######":
                return ("bothify", {"mask":"????-######"})
            return (provider, None)

    # Type-based fallback
    if dtype in ("integer", "bigint", "smallint"):
        lo, hi = numeric_stats(sample_vals)
        return ("pyint", {"min_value": int(min(lo, 0)), "max_value": int(max(hi, 100))})
    if dtype in ("numeric","real","double precision"):
        lo, hi = numeric_stats(sample_vals)
        return ("pyfloat", {"min_value": float(min(lo, 0.0)), "max_value": float(max(hi, 100.0)), "right_digits": 2})
    if "timestamp" in dtype or "date" in dtype or "time" in dtype:
        return ("date_time_between", {"start_date":"-2y", "end_date":"now"})
    if dtype in ("boolean",):
        return ("boolean", None)

    # string-ish
    svals = [v for v in sample_vals if isinstance(v, str)]
    if looks_like_email(svals):
        return ("internet.email", None)
    enums = enum_candidates(svals)
    if enums:
        return ("enum", {"choices": enums})
    # Big text vs short text
    avg_len = statistics.mean([len(v) for v in svals]) if svals else 30
    if avg_len > 60:
        return ("paragraph", {"nb_sentences": 3})
    if avg_len > 20:
        return ("sentence", None)
    return ("word", None)

def main():
    mapping = {
        "meta": {
            "generated_by": "infer_faker_mapping.py",
            "version": 1,
            "db": {"host": PGHOST, "port": PGPORT, "database": PGDATABASE}
        },
        "tables": {}
    }
    with connect() as conn:
        tables = fetch_tables(conn)
        for t in tables:
            cols = fetch_columns(conn, t)
            cons = fetch_constraints(conn, t)
            rows = sample_table(conn, t, SAMPLE_ROWS)

            col_samples = defaultdict(list)
            for r in rows:
                for k, v in r.items():
                    col_samples[k].append(v)

            # --- Ensure YAML-serializable structures
            fks_serializable = [dict(x) for x in (cons.get("fks") or [])]  # RealDictRow -> dict
            pk_list = sorted(list(cons.get("pk", set())))
            unique_list = sorted(list(cons.get("unique", set())))

            table_cfg = {
                "columns": {},
                "constraints": {
                    "pk": pk_list,
                    "unique": unique_list,
                    "fks": fks_serializable,
                },
            }

            for c in cols:
                name = c["column_name"]
                dtype = c["data_type"]
                nullable = bool(c["is_nullable"])
                unique = (name in cons.get("unique", set())) or (name in cons.get("pk", set()))
                provider, params = guess_provider(name, dtype, col_samples.get(name, []), unique)
                table_cfg["columns"][name] = {
                    "type": dtype,
                    "nullable": nullable,
                    "unique": unique,
                    "provider": provider,
                    "params": params or {}
                }

            mapping["tables"][t] = table_cfg

    with open("faker_mapping.yaml", "w") as f:
        yaml.safe_dump(mapping, f, sort_keys=False, default_flow_style=False)
    print("Wrote faker_mapping.yaml")

if __name__ == "__main__":
    main()
