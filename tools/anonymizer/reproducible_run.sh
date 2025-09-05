#!/usr/bin/env bash
set -euo pipefail

echo "=== Test Data Hackathon Reproducible Run ==="

# Load environment variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "Missing .env file. Copy from .env.example first."
  exit 1
fi

# Ensure virtual environment
if [ ! -d .venv ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi
source .venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Prepare masked DB schema (if needed)
echo "Ensuring masked DB schema exists..."
createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$DST_DB" || true
pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$SRC_DB" -s | psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DST_DB"

# Enable pgcrypto (for bcrypt)
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DST_DB" -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"

# Run anonymization
echo "Running anonymization (mask_db.py)..."
python mask_db.py

# Compare before/after
echo "Running demo comparison..."
./demo_compare.sh

echo "=== Done ==="
