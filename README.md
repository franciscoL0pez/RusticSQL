# 🦀 SQL Rustic - Programming Workshop

A lightweight SQL implementation built with **Rust**, using CSV files as database tables.

## 🚀 What it does

This project brings SQL functionality to CSV files, treating each CSV as a table where the first row defines column names and subsequent rows contain your data records.

## 🔧 Getting Started

### Run queries
```bash
cargo run -- CSV_Directory "your query here;"
```

### Run tests
```bash
cargo test
```

## 📝 Supported Operations

**Core Commands:**
- **INSERT** - Add new records
- **UPDATE** - Modify existing data  
- **DELETE** - Remove records
- **SELECT** - Query your data

**Advanced SELECT features:**
- ✅ WHERE clauses (with some limitations)
- ✅ Comparison operators (LENGTH not supported)
- ✅ Boolean logic (no abbreviations)
- ✅ ORDER BY sorting

## 💡 Example Queries

```sql
-- Add a new user
INSERT INTO users (name, age) VALUES ("Alice", 28);

-- Find adult users, sorted by name
SELECT * FROM users WHERE age >= 18 ORDER BY name;

-- Update someone's age
UPDATE users SET age = 29 WHERE name = "Alice";

-- Clean up old records
DELETE FROM users WHERE age < 13;
```

## 🗂️ How it works

- Place your CSV files in the specified directory
- First row = column headers
- Following rows = your data
- End queries with semicolon (`;`)

## 🧪 Testing

Comprehensive test suite included to verify all SQL operations work correctly.

---

*Academic project for Programming Workshop - Professor Deymonaz*
