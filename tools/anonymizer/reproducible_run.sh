#!/usr/bin/env bash
set -euo pipefail

echo "=== Test Data Hackathon Reproducible Run ==="

# --- Location: run from tools/anonymizer ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# --- Env: load .env if present, otherwise sane defaults
if [[ -f .env ]]; then
  echo "Loading .env ..."
  set -a; source .env; set +a
fi
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export SRC_DB="${SRC_DB:-hackathon_db}"
export DST_DB="${DST_DB:-hackathon_db_masked}"
export DRY_LIMIT="${DRY_LIMIT:-0}"

# --- Tool wrappers as arrays (robust against quoting)
PSQL=(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER")
CREATEDB=(createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER")
PG_DUMP=(pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER")

# --- Python: venv + install from requirements.txt
echo "Creating virtual environment..."
python3 -m venv .venv >/dev/null 2>&1 || true
source .venv/bin/activate

echo "Installing Python dependencies from requirements.txt..."
python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt

# --- Ensure masked DB schema exists (fresh, schema-only)
echo "Ensuring masked DB schema exists..."
# Terminate existing connections & drop DB if it exists
"${PSQL[@]}" -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DST_DB}' AND pid <> pg_backend_pid();" >/dev/null || true
"${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS ${DST_DB};" >/dev/null || true

# Create fresh masked DB
"${CREATEDB[@]}" "$DST_DB"

# Recreate schema (no data)
"${PG_DUMP[@]}" -d "$SRC_DB" -s | "${PSQL[@]}" -d "$DST_DB" >/dev/null
# Needed for bcrypt/crypt functions if you later use pg hashing
"${PSQL[@]}" -d "$DST_DB" -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;" >/dev/null || true

# --- Run anonymizer (mapping-driven)
echo "Running anonymization (mask_db.py)..."
export PGHOST PGPORT PGUSER PGPASSWORD SRC_DB DST_DB DRY_LIMIT
python mask_db.py

# --- Compare original vs masked (optional)
if [[ -x ./demo_compare.sh ]]; then
  echo "Running comparison..."
  ./demo_compare.sh
else
  echo "Note: demo_compare.sh not found or not executable; skipping comparison."
fi

echo "=== Done ==="
