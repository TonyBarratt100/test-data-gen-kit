import os, hashlib, json
import psycopg2
import psycopg2.extras as ex
from faker import Faker

# --- Config ---
SEED = 123
SALT = "TDG_SALT_2025"
BATCH = 5000
DRY_LIMIT = int(os.getenv("DRY_LIMIT", "0"))  # 0 = full data

PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = int(os.getenv("PGPORT", "55433"))
PGUSER = os.getenv("PGUSER", "hackathon_user")
PGPASSWORD = os.getenv("PGPASSWORD", "hackathon_pass")
SRC_DB = os.getenv("SRC_DB", "hackathon_db")
DST_DB = os.getenv("DST_DB", "hackathon_db_masked")

fk = Faker(); fk.seed_instance(SEED)

# --- Helpers ---
def connect(db: str):
    return psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=db)

def get_columns(conn, table: str, schema: str = "public"):
    """Fetch stable column order from information_schema."""
    with conn.cursor() as c:
        c.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in c.fetchall()]

def mask_email(email: str, user_id: int) -> str:
    if not email:
        return f"{hashlib.md5((SALT+str(user_id)).encode()).hexdigest()[:10]}@example.test"
    if "@" in email:
        local, domain = email.split("@", 1)
    else:
        local, domain = email, "example.test"
    masked_local = hashlib.md5((SALT+local).encode()).hexdigest()[:10] + "." + str(user_id)[-3:]
    return f"{masked_local}@{domain}"

def get_id_subset(conn, table: str, id_col: str = "id", limit: int = 0):
    if limit <= 0:
        return None  # means 'all'
    with conn.cursor() as c:
        c.execute(f"SELECT {id_col} FROM {table} ORDER BY {id_col} LIMIT %s", (limit,))
        return {r[0] for r in c.fetchall()}

# --- Copy/Mask routines ---
def copy_table_filtered(src, dst, table: str, cols: list[str], where_sql: str = "", params: tuple = ()):
    """Generic SELECT (with optional WHERE) -> INSERT, preserves column order."""
    limit_clause = f" LIMIT {DRY_LIMIT}" if DRY_LIMIT and not params else ""  # when filtering by ids, limit by WHERE set size
    select_sql = f"SELECT {','.join(cols)} FROM {table} " + (f"WHERE {where_sql} " if where_sql else "") + f"ORDER BY id{limit_clause}"
    insert_sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES %s"
    with src.cursor(name=f"{table}_cur") as cur:
        cur.itersize = BATCH
        cur.execute(select_sql, params)
        buf = []
        with dst.cursor() as dcur:
            for row in cur:
                buf.append(tuple(row))
                if len(buf) >= BATCH:
                    ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)
                    buf.clear()
            if buf:
                ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)

def bulk_copy_table(src, dst, table: str):
    """Full table copy (no WHERE)."""
    cols = get_columns(src, table)
    copy_table_filtered(src, dst, table, cols)

def mask_users_subset(src, dst, user_ids: list[int] | None):
    # users: id, email, full_name, password, is_active, created_at, updated_at
    where = "id = ANY(%s)" if user_ids is not None else ""
    params = (list(user_ids),) if user_ids is not None else ()
    limit_clause = "" if user_ids is not None else (f" LIMIT {DRY_LIMIT}" if DRY_LIMIT else "")
    select_sql = f"""
        SELECT id, email, full_name, password, is_active, created_at, updated_at
        FROM users
        {(f"WHERE {where}" if where else "")}
        ORDER BY id{limit_clause}
    """
    insert_sql = """INSERT INTO users
                    (id, email, full_name, password, is_active, created_at, updated_at)
                    VALUES %s"""
    template = "(%s,%s,%s, crypt('Test1234!', gen_salt('bf')) ,%s,%s,%s)"
    with src.cursor(name="users_cur") as cur:
        cur.itersize = BATCH
        cur.execute(select_sql, params)
        buf = []
        with dst.cursor() as dcur:
            for uid, email, full_name, password, is_active, created_at, updated_at in cur:
                buf.append((
                    uid,
                    mask_email(email or "", uid),
                    fk.name(),
                    # password replaced via SQL crypt() in template
                    is_active, created_at, updated_at
                ))
                if len(buf) >= BATCH:
                    ex.execute_values(dcur, insert_sql, buf, template=template, page_size=BATCH)
                    buf.clear()
            if buf:
                ex.execute_values(dcur, insert_sql, buf, template=template, page_size=BATCH)

def mask_reviews_subset(src, dst, rev_cols: list[str], user_ids: list[int] | None, product_ids: list[int] | None):
    has_comment = "comment" in rev_cols
    if user_ids is not None and product_ids is not None:
        where = "user_id = ANY(%s) AND product_id = ANY(%s)"
        params = (list(user_ids), list(product_ids))
    else:
        where = ""
        params = ()
    limit_clause = "" if where else (f" LIMIT {DRY_LIMIT}" if DRY_LIMIT else "")
    select_sql = "SELECT " + ",".join(rev_cols) + f" FROM reviews " + (f"WHERE {where} " if where else "") + f"ORDER BY id{limit_clause}"
    insert_sql = f"INSERT INTO reviews ({','.join(rev_cols)}) VALUES %s"
    with src.cursor(name="reviews_cur") as cur:
        cur.itersize = BATCH
        cur.execute(select_sql, params)
        buf = []
        with dst.cursor() as dcur:
            for row in cur:
                row = list(row)
                if has_comment:
                    i = rev_cols.index("comment")
                    row[i] = fk.paragraph(nb_sentences=3)
                buf.append(tuple(row))
                if len(buf) >= BATCH:
                    ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)
                    buf.clear()
            if buf:
                ex.execute_values(dcur, insert_sql, buf, page_size=BATCH)

def write_audit(dst):
    with dst.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS masking_audit (
                id serial PRIMARY KEY,
                ran_at timestamptz DEFAULT now(),
                source_db text,
                masked_tables jsonb
            )
        """)
        cur.execute("""
            INSERT INTO masking_audit (source_db, masked_tables)
            VALUES (%s, %s)
        """, (SRC_DB, json.dumps({
            "users":  ["email","full_name","password -> crypt(Test1234!)"],
            "reviews":["comment"]
        })))
    dst.commit()

# --- Main orchestration ---
def main():
    src = connect(SRC_DB); dst = connect(DST_DB)
    try:
        # Grab column lists once
        prod_cols = get_columns(src, "products")
        ord_cols  = get_columns(src, "orders")
        rev_cols  = get_columns(src, "reviews")

        if DRY_LIMIT:
            # Build consistent subsets to avoid FK violations
            user_ids = get_id_subset(src, "users", "id", DRY_LIMIT)
            prod_ids = get_id_subset(src, "products", "id", DRY_LIMIT)
            print(f"DRY mode: limiting to {DRY_LIMIT} users/products and filtering dependent rows.")

            # 1) products subset
            print("Copying products (subset)...")
            copy_table_filtered(src, dst, "products", prod_cols, "id = ANY(%s)", (list(prod_ids),))
            dst.commit()

            # 2) users subset (masked)
            print("Masking users (subset)...")
            mask_users_subset(src, dst, list(user_ids))
            dst.commit()

            # 3) orders filtered to chosen users/products
            print("Copying orders (filtered)...")
            copy_table_filtered(
                src, dst, "orders", ord_cols,
                "user_id = ANY(%s) AND product_id = ANY(%s)",
                (list(user_ids), list(prod_ids))
            )
            dst.commit()

            # 4) reviews filtered (masked)
            print("Masking reviews (filtered)...")
            mask_reviews_subset(src, dst, rev_cols, list(user_ids), list(prod_ids))
            dst.commit()

        else:
            # Full dataset in FK-safe order
            print("Copying products..."); bulk_copy_table(src, dst, "products"); dst.commit()
            print("Masking users...");    mask_users_subset(src, dst, None);    dst.commit()
            print("Copying orders...");   bulk_copy_table(src, dst, "orders");  dst.commit()
            print("Masking reviews...");  mask_reviews_subset(src, dst, rev_cols, None, None); dst.commit()

        print("Writing audit..."); write_audit(dst)
        print("Done.")
    finally:
        src.close(); dst.close()

if __name__ == "__main__":
    main()
