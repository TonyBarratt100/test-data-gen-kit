
# 🛡️ Database Anonymizer & Test Data Generator  

![Python](https://img.shields.io/badge/Python-3.12-blue.svg) ![License: MIT](https://img.shields.io/badge/License-MIT-green.svg) ![Hackathon](https://img.shields.io/badge/Test%20Data-Hackathon%202025-orange.svg)  

**Easily anonymize databases for safe testing while keeping schema & integrity intact.**  

A lightweight framework to anonymize sensitive database records while preserving structure, integrity, and usefulness for testing.  
Built during the **Test Data Hackathon 2025**, this project masks real data with Faker-generated substitutes, validates referential integrity, and provides reproducible runs.  

---

## 🚀 Quickstart  

Clone the repo and run the reproducible script:  

    git clone https://github.com/your-org/anonymizer.git
    cd anonymizer
    ./reproducible_run.sh

This will:  
1. Create a `.venv` and install dependencies from `requirements.txt`.  
2. Ensure the masked DB schema exists.  
3. Run the anonymization pipeline (`mask_db.py`).  
4. Validate results (row counts, foreign keys, email checks).  
5. Log an audit record of the masking operation.  

---

## 🔍 Examples  

### Row Counts (Original vs Masked)  

    tbl    | count 
    ----------+-------
    orders   | 29240
    products | 20000
    reviews  | 60043
    users    | 10000

### Users Table – Before & After Masking  

Original  

    id |            email             |   full_name    | pw_prefix  
    ----+------------------------------+----------------+------------
     1 | melindasmith@example.org     | Kathy Davis    | S^8UQNh!)v
     2 | cunninghamrachel@example.org | David Finley   | n1&4LvCpir

Masked  

    id |           email           |     full_name      | pw_prefix  
    ----+---------------------------+--------------------+------------
     1 | user1+07df79@example.test | Jessica Smith      | $2b$04$g/9
     2 | user2+48f174@example.test | Danielle Contreras | $2b$04$ysM

---

## 🧪 Smoke Test  

Run a quick subset anonymization in “dry mode” to verify masking:  

    python smoke_test.py --limit 100000

Sample output:  

    === Sanity checks (masked DB) ===
    {
      "row_counts": {
        "users": 10000,
        "products": 20000,
        "orders": 29240,
        "reviews": 60043
      },
      "email_dupes": 0,
      "email_invalid": 0,
      "orphans": {
        "orders_user": 0,
        "orders_product": 0,
        "reviews_user": 0,
        "reviews_product": 0
      }
    }

✅ Confirms row counts match, no duplicates, no invalid emails, and no broken foreign keys.  

---

## 🏗️ Architecture  

    ┌──────────────┐       ┌──────────────┐
    │              │       │              │
    │  Source DB   │──────▶│  mask_db.py  │
    │ (hackathon)  │       │ (anonymizer) │
    │              │       │              │
    └───────┬──────┘       └──────┬──────┘
            │                     │
            │  masked tables      │
            ▼                     ▼
    ┌──────────────┐       ┌──────────────┐
    │              │       │              │
    │  Masked DB   │──────▶│  Validation  │
    │ (safe to use)│       │ (row counts, │
    │              │       │  FKs, audit) │
    └──────────────┘       └──────────────┘

Flow:  
1. Copy schema & data from Source DB.  
2. Replace sensitive fields with Faker / hashed values.  
3. Write anonymized records into Masked DB.  
4. Run validation checks and store an audit record.  

---

## ⚙️ Configuration  

Masking rules are defined in `faker_mapping.yaml`.  
This file maps database columns to Faker providers or hashing functions. Example:  

    users:
      email: faker.email
      full_name: faker.name
      pw_prefix: bcrypt
    reviews:
      comment_snippet: faker.text

You can extend this mapping with any supported Faker providers or custom functions.  

---

## 🤝 Contributing  

This project was developed for the **Test Data Hackathon 2025**. Contributions are welcome!  

- Issues: Use GitHub Issues to report bugs or request features.  
- Pull Requests: Fork the repo, create a feature branch, and open a PR.  
- Hackathon context: Focus was on anonymization, reproducibility, and validation. Future work may include:  
  - FastAPI API endpoints for on-demand anonymization.  
  - Support for more DB backends.  
  - Configurable seeding for deterministic masking.  

---

## 📜 License  

MIT License — simple and permissive.  
See the LICENSE file for full text.  
