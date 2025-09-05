# Test Data Generator (TDG)

Synthetic test data generator built with **Python + Faker**.  
Designed to create realistic, consistent, and scalable datasets for development and testing.
---
---

## 🔐 Test Data Anonymization Toolkit

This repo now includes a **data masking & anonymization toolkit** under  
[`tools/anonymizer`](tools/anonymizer).

- **`mask_db.py`** → anonymizes sensitive fields (`users.email`, `users.full_name`, `users.password`, `reviews.comment`)  
- **`reproducible_run.sh`** → sets up venv, installs deps, prepares DBs, runs masking, runs comparison  
- **`demo_compare.sh`** → shows before/after row counts, samples, email validity, and FK checks  
- **`.env.example`** → connection settings template  
- **`requirements.txt`** → pinned Python dependencies  

👉 To try it out:

```bash
cd tools/anonymizer
cp .env.example .env   # adjust DB connection
./reproducible_run.sh

## ✨ Features

- **Entities**: Users, Products, Orders, Reviews  
- **Realism via Faker**: names, emails, phone numbers, product descriptions, review text  
- **Beyond Faker**:
  - Distributions (Zipf popularity, log-normal prices)
  - Referential integrity (orders ↔ users/products, reviews ↔ products)
  - Status fields (`shipped`, `cancelled`, etc.)
- **CLI commands**:
  - `generate` → CSV/JSON export
  - `seed-postgres` → populate Postgres DB
  - `call-api` → push data into API endpoints
  - `anonymize` → mask PII
- **Mock API** for quick integration testing
- **Demo SQL** queries for analytics use cases

---

## 🚀 Quick Start

Clone the repo and set up your environment:

```bash
git clone https://github.com/TonyBarratt100/test-data-gen-kit.git
cd test-data-gen-kit
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

---

### 1. Run Postgres in Docker

```bash
docker run -d --name tdgpg   -e POSTGRES_USER=postgres   -e POSTGRES_PASSWORD=postgres   -e POSTGRES_DB=tdg   -e PGDATA=/var/lib/postgresql/data/pgdata   -v tdgpgdata:/var/lib/postgresql/data/pgdata   -p 55432:5432 postgres:16
```

---

### 2. Seed the Database

```bash
export DB_URL="postgresql+psycopg2://tdg:tdg@127.0.0.1:55432/tdg"
python -m tdg.cli seed-postgres --create --truncate
```

---

### 3. Run Demo Queries

```bash
PGPASSWORD=tdg psql -h 127.0.0.1 -p 55432 -U tdg -d tdg -f demo.sql
```

Example queries include:
- Row counts per table  
- Top products by revenue  
- Average rating per product  
- Order status funnel  

---

### 4. Use the Mock API

```bash
pip install fastapi uvicorn
python mock_app.py
```

Push fake data into the mock API:

```bash
python -m tdg.cli call-api --users 20 --orders 50 --reviews 30
curl http://localhost:8000/stats
# → {"users":20, "orders":50, "reviews":30}
```

---

## 📂 Project Structure

```
src/tdg/         # Generators & CLI
mock_app.py      # FastAPI mock server
demo.sql         # Example queries
docker-compose.yml
README.md
```

---

## 📖 Demo Materials
- `hackathon_cheatsheet.pdf` → overview of flow  
- `post_reboot_checklist.pdf` → restart guide (reseed optional)  
- `demo.sql` → ready-to-run queries  
- `hackathon_demo_flow.pdf` → 2-minute demo timeline  

---

## 🏆 Hackathon Value

This project extends Faker into a **full test data system**:  
- Believable values ✔  
- Consistent relationships ✔  
- Automated workflows ✔  
- Supports files, databases, and APIs ✔  
- Optional anonymization for compliance ✔
---

## 🔐 Test Data Anonymization Toolkit

This repo now includes a **data masking & anonymization toolkit** under  
[`tools/anonymizer`](tools/anonymizer).

- **`mask_db.py`** → anonymizes sensitive fields (`users.email`, `users.full_name`, `users.password`, `reviews.comment`)  
- **`reproducible_run.sh`** → sets up venv, installs deps, prepares DBs, runs masking, runs comparison  
- **`demo_compare.sh`** → shows before/after row counts, samples, email validity, and FK checks  
- **`.env.example`** → connection settings template  
- **`requirements.txt`** → pinned Python dependencies  

👉 To try it out:

```bash
cd tools/anonymizer
cp .env.example .env   # adjust DB connection
./reproducible_run.sh
