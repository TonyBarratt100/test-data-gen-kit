# 🧪 Test Data Hackathon

Welcome to the **Test Data Hackathon**! This internal event is designed to tackle two of the most pressing challenges in Test Data Management (TDM): **synthetic data generation** and **data masking/anonymization**. You’ll be working with a sample e-commerce-like codebase involving users, products, orders, and reviews. The main goal is to create test data generation and anonymization solutions that enhance integration testing reliability, privacy compliance, and developer productivity.

## 📘 Codebase Overview

The codebase contains four core models:

- **User**
  - Fields: `email`, `full_name`, `password`, `is_active`, timestamps
  - Relationships: Has many `orders` and `reviews`
  - PII fields: `email`, `full_name`, `password`

- **Product**
  - Fields: `name`, `description`, `price`, `category`, `stock`
  - Relationships: Has many `orders` and `reviews`

- **Order**
  - Fields: `user_id`, `product_id`, `quantity`, `total_price`, `status`, timestamps
  - Relationships: Belongs to `user` and `product`

- **Review**
  - Fields: `user_id`, `product_id`, `rating`, `comment`, timestamps
  - Relationships: Belongs to `user` and `product`
  - Notes: A user may only leave one review per product

## 🧩 Hackathon Exercises

### 🔧 Exercise 1: Synthetic Data Generation

Create a Python tool to populate the database with **realistic, varied, and reproducible synthetic data** for all four models.

#### 🔍 Requirements:

- Generate consistent and linked data:
  - Users who have placed orders
  - Products that are reviewed
  - Orders referencing valid users/products
  - Respect unique and foreign key constraints
- Allow test scenarios such as:
  - Users with 0, 1, or many orders/reviews
  - Products with high/low/no stock
  - Orders with edge-case status or large quantities
- Seed-based reproducibility.

#### Advanced Ideas:

- Add CLI support (e.g., `python generate.py --users 100 --orders 500`).
- Support config files (YAML/JSON) for data shape.

### 🔐 Exercise 2: Data Masking and Anonymization

Build a tool to **anonymize production data** while maintaining structure and utility for integration testing.

#### Requirements:

- Identify and transform sensitive fields:
  - `User.email`, `User.full_name`, `User.password`
  - Any comments in `Review.comment`
- Techniques to support:
  - Faker-based substitutions
  - Shuffling or hashing identifiers
  - Nulling or format-preserving obfuscation
- Maintain referential integrity across models.
- Log or report what was masked.

#### Edge Cases to Handle:

- Ensure masked `email` fields remain unique.
- Preserve correct foreign key relationships.
- Avoid breaking the `user_id`/`product_id` links in `Order` and `Review`.

### 🧰 Suggested Tools & Libraries
| Task                  | Tools                                                  |
| --------------------- | ------------------------------------------------------ |
| Synthetic Generation  | Faker, Hypothesis, pydbgen, NumPy, Pandas              |
| Anonymization         | Pandas, Faker, Presidio, anonymizedf, re, cryptography |

### 🧪 Testing and Integration

Run pytest after data generation to ensure relational and integrity constraints.
Validate anonymization:
- No original emails or names in the output
- Referential links intact
- Consider generating a data_audit.json for logs.

## 👥 Team Collaboration

Each team owns their implementation of the exercises. Collaboration and cross-pollination of ideas are encouraged!

## 📄 License

Internal Use Only – Confidential.

Let’s build something impactful. Happy hacking! 🚀
