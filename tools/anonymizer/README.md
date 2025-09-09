# Anonymizer

The **Anonymizer** connects to a Postgres database, inspects schema and metadata, and produces a **GDPR compliance + data quality report**.

---

## ✨ Features

- **Primary keys, foreign keys, indexes** detection  
- **Column profiling**:
  - Type, nullability, default values
  - Null fraction, distinctness, most common values
- **PII likelihood** heuristic:
  - High → emails, names, free text
  - Medium → descriptive/categorical fields
  - Low → numeric, dates, booleans
- **Top risks summary**:
  - High-null columns
  - Potential PII
  - Low-distinctness fields

---

## 🚀 Quickstart

### Linux / macOS

```bash
# copy example env and edit with DB creds
cp ../../.env.example .env

# run tests
./test.sh

# run smoke test
python smoke_test.py
```

### Windows (PowerShell)

```powershell
# copy env file
copy ..\..\.env.example .env

# set environment variables (example)
$env:PGHOST="127.0.0.1"
$env:PGPORT="55433"
$env:PGUSER="hackathon_user"
$env:PGPASSWORD="hackathon_pass"
$env:SRC_DB="hackathon_db"
$env:SCHEMAS="public"

# run tests (requires Git Bash)
bash test.sh

# or run smoke test directly
python smoke_test.py
```

👉 On Windows you need **Git Bash** (bundled with Git for Windows) or **WSL2** to run `test.sh`.  
If neither is available, manually reproduce the script’s steps in PowerShell.

---

## 📝 Example Output

JSON snippet:

```json
{
  "db": "hackathon_db",
  "schemas": ["public"],
  "tables": [
    {
      "schema": "public",
      "table": "users",
      "columns": [
        {"name": "email", "type": "character varying", "pii_likelihood": "high"},
        {"name": "full_name", "type": "character varying", "pii_likelihood": "high"},
        {"name": "password", "type": "character varying", "pii_likelihood": "medium"}
      ],
      "primary_key": ["id"],
      "foreign_keys": [],
      "indexes": [...]
    }
  ],
  "top_risks": {
    "high_null_rate_columns": ["public.users.updated_at (null_frac=1.00)"],
    "potential_pii_columns": ["public.users.email", "public.users.full_name"]
  }
}
```

---

## 🔐 GDPR Use Cases

- Identify and mask **PII columns** before exporting test data.  
- Share **masked schemas** with developers/testers safely.  
- Support **DSAR / Right to be Forgotten** workflows by knowing where PII resides.  
- Maintain **privacy by design** in test data pipelines.

---

## ⚡ Tips

- Re-run `smoke_test.py` after masking to verify no PII is left.  
- Use `\dt` in `psql` to confirm table names if queries fail.  
- For Windows, prefer **Git Bash** or **WSL2** for best compatibility.
