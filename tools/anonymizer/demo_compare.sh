#!/usr/bin/env bash
set -euo pipefail

# Load env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "Missing .env file. Copy from .env.example first."
  exit 1
fi

PSQL="psql -v ON_ERROR_STOP=1 -h ${PGHOST} -p ${PGPORT} -U ${PGUSER}"

sep() { printf "\n============================================================\n%s\n============================================================\n" "$1"; }

# --- Row counts
sep "ROW COUNTS (ORIGINAL vs MASKED)"
$PSQL -d "${SRC_DB}" -c "SELECT 'users' AS tbl, count(*) FROM users
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'reviews', count(*) FROM reviews
ORDER BY tbl;"

$PSQL -d "${DST_DB}" -c "SELECT 'users' AS tbl, count(*) FROM users
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'reviews', count(*) FROM reviews
ORDER BY tbl;"

# --- Users: first 5
sep "USERS (FIRST 5) — ORIGINAL"
$PSQL -d "${SRC_DB}" -c "SELECT id, email, full_name, LEFT(password,10) AS pw_prefix FROM users ORDER BY id LIMIT 5;"

sep "USERS (FIRST 5) — MASKED"
$PSQL -d "${DST_DB}" -c "SELECT id, email, full_name, LEFT(password,10) AS pw_prefix FROM users ORDER BY id LIMIT 5;"

# --- Reviews: first 5
sep "REVIEWS (FIRST 5) — ORIGINAL"
$PSQL -d "${SRC_DB}" -c "SELECT id, LEFT(comment,60) AS comment_snippet FROM reviews ORDER BY id LIMIT 5;"

sep "REVIEWS (FIRST 5) — MASKED"
$PSQL -d "${DST_DB}" -c "SELECT id, LEFT(comment,60) AS comment_snippet FROM reviews ORDER BY id LIMIT 5;"

# --- Email checks (MASKED)
sep "EMAIL CHECKS (MASKED)"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS duplicate_emails FROM (SELECT email, COUNT(*) c FROM users GROUP BY email HAVING COUNT(*)>1) t;"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS invalid_emails FROM users WHERE position('@' in email)=0;"

# --- FK sanity (MASKED)
sep "FOREIGN KEY SANITY (MASKED)"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS orphan_orders_users   FROM orders  o LEFT JOIN users    u ON o.user_id=u.id     WHERE u.id IS NULL;"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS orphan_orders_products FROM orders  o LEFT JOIN products p ON o.product_id=p.id WHERE p.id IS NULL;"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS orphan_reviews_users   FROM reviews r LEFT JOIN users    u ON r.user_id=u.id     WHERE u.id IS NULL;"
$PSQL -d "${DST_DB}" -c "SELECT COUNT(*) AS orphan_reviews_products FROM reviews r LEFT JOIN products p ON r.product_id=p.id WHERE p.id IS NULL;"

# --- Audit
sep "MASKING AUDIT (MASKED DB)"
$PSQL -d "${DST_DB}" -c "SELECT ran_at, source_db, masked_tables FROM masking_audit ORDER BY ran_at DESC LIMIT 1;"
