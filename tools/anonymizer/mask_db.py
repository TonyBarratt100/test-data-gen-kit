#!/usr/bin/env python3
"""
Fast anonymizer for the Hackathon DB.

- Reads optional faker_mapping.yaml to decide which columns to mask.
- Deterministic, unique emails using id + md5 -> example.test
- Bcrypt cost adjustable via env BCRYPT_ROUNDS (default 4 for speed)
- DRY_LIMIT (env) copies a filtered, referentially consistent slice.
- Truncates destination tables before writing to avoid PK collisions.
- Writes a masking_audit record.

Env vars used:
  PGHOST, PGPORT, PGUSER, PGPASSWORD
  SRC_DB, DST_DB
  DRY_LIMIT (optional int)
  BCRYPT_ROUNDS (optional int, default 4)
"""

import os
import random
import hashlib
from datetime import datetime, timezone

import bcrypt
import psycopg2
import psycopg2.extras as ex
from faker import Faker
import yaml

# ------------------------------
# Config & PRNG/Faker seeding
# ------------------------------
faker = Faker()
Faker.seed(1234)
random.seed(1234)

BATCH = 1000
BCRYPT_ROUNDS = int(os.environ.get("BCRYPT_ROUNDS", "4"))  # lower = faster; 10-12 is prod-like

# Load mapping file if present; otherwise default mapping.
DEFAULT_MAPPING = {
    "users": {
        "columns": {
            "email": {"provider": "internet.email"},
            "full_name": {"provider": "person.name"},
            "password": {"provider": "password.hash"},
        }
    },
    "reviews": {
        "columns": {
            "comment": {"provider": "text.sentence"},
        }
    },
}

def load_mapping(path="faker_mapping.yaml"):
    if os.path.exists(path):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            return data or DEFAULT_MAPPING
    return DEFAULT_MAPPING

MAPPING = load_mapping()


# ------------------------------
# DB helpers
# ------------------------------
def connect(dbname: str):
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
        dbname=dbname,
    )


def truncate_all(dst):
    """Start clean to avoid duplicate PK issues on reruns."""
    with dst.cursor() as cur:
        cur.execute("TRUNCATE TABLE reviews, orders, users, products RESTART IDENTITY CASCADE;")
    dst.commit()


def colnames(cur, table: str):
    cur.execute(f'SELECT * FROM "{table}" LIMIT 0;')
    return [d.name for d in cur.description]


def select_ids(conn, table: str, limit: int):
    with conn.cursor() as cur:
        cur.execute(f'SELECT id FROM "{table}" ORDER BY id LIMIT %s;', (limit,))
        return [r[0] for r in cur.fetchall()]


# ------------------------------
# Masking primitives
# ------------------------------
def mask_value(table: str, col: str, value, *, row_ctx=None):
    """Return masked value based on faker_mapping; deterministic where needed."""
    cfg = (MAPPING.get(table, {}) or {}).get("columns", {}).get(col, {})
    provider = cfg.get("provider")
    if not provider:
        return value

    # Row context is used for deterministic unique emails based on id.
    row_ctx = row_ctx or {}

    if provider == "internet.email":
        # Deterministic, unique, and fast (no faker.unique) — prefer id if available
        uid = row_ctx.get("id")
        base = str(value) if value is not None else ""
        digest = hashlib.md5(base.encode()).hexdigest()[:6]
        if uid is not None:
            return f"user{uid}+{digest}@example.test"
        return f"anon+{digest}@example.test"

    if provider == "person.name":
        return faker.name()

    if provider == "password.hash":
        # Random password then bcrypt with configurable rounds
        pw = faker.password(length=10)
        return bcrypt.hashpw(pw.encode(), bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode()

    if provider == "text.sentence":
        return faker.sentence(nb_words=8)

    if provider == "date_time_between":
        p = cfg.get("params", {})
        return faker.date_time_between(
            start_date=p.get("start_date", "-2y"),
            end_date=p.get("end_date", "now")
        )

    if provider == "pyint":
        p = cfg.get("params", {})
        return faker.pyint(
            min_value=p.get("min", 0),
            max_value=p.get("max", 1000)
        )

    if provider == "enum":
        choices = (cfg.get("params") or {}).get("choices", [])
        return random.choice(choices) if choices else value

    # Fallback: replace with a simple word to avoid leaking originals
    return faker.word()


def bulk_copy_table(src, dst, table: str, *, mask_cols=None, where_sql="", where_params=()):
    """Copy table data from src->dst, with optional column masking and filtering."""
    scur = src.cursor()
    dcur = dst.cursor()

    cols = colnames(scur, table)
    id_idx = cols.index("id") if "id" in cols else None
    col_list = ", ".join([f'"{c}"' for c in cols])

    scur.execute(f'SELECT {col_list} FROM "{table}" {where_sql};', where_params)
    insert_sql = f'INSERT INTO "{table}" ({col_list}) VALUES %s'

    buf = []
    for row in scur:
        row = list(row)
        if mask_cols:
            row_ctx = {"id": row[id_idx]} if id_idx is not None else {}
            for i, c in enumerate(cols):
                if c in mask_cols:
                    row[i] = mask_value(table, c, row[i], row_ctx=row_ctx)
        buf.append(tuple(row))
        if len(buf) >= BATCH:
            ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)
            buf.clear()

    if buf:
        ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)


# ------------------------------
# Main pipeline
# ------------------------------
def main():
    src_db = os.environ["SRC_DB"]
    dst_db = os.environ["DST_DB"]
    dry_limit = os.environ.get("DRY_LIMIT")
    dry_limit = int(dry_limit) if (dry_limit and dry_limit.strip().isdigit()) else 0

    src = connect(src_db)
    dst = connect(dst_db)

    # Always start clean to ensure reproducible reruns
    truncate_all(dst)

    if dry_limit <= 0:
        print("Copying products...")
        bulk_copy_table(src, dst, "products")
        dst.commit()

        print("Masking users...")
        bulk_copy_table(src, dst, "users", mask_cols=["email", "full_name", "password"])
        dst.commit()

        print("Copying orders...")
        bulk_copy_table(src, dst, "orders")
        dst.commit()

        print("Masking reviews...")
        bulk_copy_table(src, dst, "reviews", mask_cols=["comment"])
        dst.commit()
    else:
        print(f"DRY mode with filtering: limit={dry_limit}")

        user_ids = select_ids(src, "users", dry_limit)
        prod_ids = select_ids(src, "products", dry_limit)

        print("Copying products (subset)...")
        bulk_copy_table(
            src, dst, "products",
            where_sql="WHERE id = ANY(%s)",
            where_params=(prod_ids,),
        )
        dst.commit()

        print("Masking users (subset)...")
        bulk_copy_table(
            src, dst, "users",
            mask_cols=["email", "full_name", "password"],
            where_sql="WHERE id = ANY(%s)",
            where_params=(user_ids,),
        )
        dst.commit()

        print("Copying orders (filtered)...")
        bulk_copy_table(
            src, dst, "orders",
            where_sql="WHERE user_id = ANY(%s) AND product_id = ANY(%s)",
            where_params=(user_ids, prod_ids),
        )
        dst.commit()

        print("Masking reviews (filtered)...")
        bulk_copy_table(
            src, dst, "reviews",
            mask_cols=["comment"],
            where_sql="WHERE user_id = ANY(%s) AND product_id = ANY(%s)",
            where_params=(user_ids, prod_ids),
        )
        dst.commit()

    # Write audit row
    with dst.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS masking_audit (
                ran_at TIMESTAMPTZ,
                source_db TEXT,
                masked_tables TEXT[]
            );
        """)
        cur.execute(
            "INSERT INTO masking_audit (ran_at, source_db, masked_tables) VALUES (%s, %s, %s);",
            (datetime.now(timezone.utc), src_db, ["users", "products", "orders", "reviews"])
        )
    dst.commit()

    print("Done.")


if __name__ == "__main__":
    main()
